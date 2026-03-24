package modules

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha512"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	tokenTypeOneTime  = "onetime"
	tokenTypeLifetime = "lifetime"
)

// TokenManager manages JWT tokens with configurable usage mode.
// TOKEN_TYPE=onetime  => token is consumed after successful validation
// TOKEN_TYPE=lifetime => token can be reused until expiration/flush
type TokenManager struct {
	mu               sync.RWMutex
	db               *Database
	replManager      *ReplicationManager
	aesKey           [32]byte // AES-256 key
	tokens           map[string]*TokenRecord
	encryptionSalt   string
	tokenType        string
	tokenTTL         time.Duration
	maxTokensPerUser int
}

// TokenRecord stores information about issued tokens
type TokenRecord struct {
	Token    string    `json:"token"`
	Username string    `json:"username"`
	IsAdmin  bool      `json:"is_admin"`
	IssuedAt time.Time `json:"issued_at"`
	Used     bool      `json:"used"`
	UsedAt   time.Time `json:"used_at,omitempty"`
}

// JWTPayload represents the decrypted JWT payload
type JWTPayload struct {
	Username string `json:"username"`
	IsAdmin  bool   `json:"is_admin"`
	IssuedAt int64  `json:"issued_at"`
	Nonce    string `json:"nonce"`
}

// NewTokenManager creates a new token manager
func NewTokenManager(db *Database, aesKeyString string, replManager *ReplicationManager) (*TokenManager, error) {
	tm := &TokenManager{
		db:               db,
		replManager:      replManager,
		tokens:           make(map[string]*TokenRecord),
		tokenType:        tokenTypeOneTime,
		tokenTTL:         0,
		maxTokensPerUser: 0,
	}

	// Derive AES key from provided string (hash with SHA512, take first 32 bytes)
	if aesKeyString == "" {
		aesKeyString = "default-secret-key-change-this-in-production"
	}
	hash := sha512.Sum512([]byte(aesKeyString))
	copy(tm.aesKey[:], hash[:32])

	// Generate random salt for obfuscation
	saltBytes := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, saltBytes); err != nil {
		return nil, fmt.Errorf("failed to generate salt: %w", err)
	}
	tm.encryptionSalt = hex.EncodeToString(saltBytes)

	// Initialize tokens table in system database
	db.mu.Lock()
	if db.data["system"].PrivateTables["tokens"] == nil {
		db.data["system"].PrivateTables["tokens"] = make([]interface{}, 0)
	}
	db.mu.Unlock()

	if tokenTypeRaw := strings.TrimSpace(strings.ToLower(os.Getenv("TOKEN_TYPE"))); tokenTypeRaw != "" {
		switch tokenTypeRaw {
		case tokenTypeOneTime, tokenTypeLifetime:
			tm.tokenType = tokenTypeRaw
		default:
			// Keep safe default (onetime) for unknown values
		}
	}

	// TOKEN_EXPIRATION is the preferred setting name.
	// TOKEN_TTL_SECONDS is kept as backwards-compatible fallback.
	ttlStr := strings.TrimSpace(os.Getenv("TOKEN_EXPIRATION"))
	if ttlStr == "" {
		ttlStr = strings.TrimSpace(os.Getenv("TOKEN_TTL_SECONDS"))
	}
	if ttlStr != "" {
		if ttlSeconds, err := strconv.Atoi(ttlStr); err == nil && ttlSeconds > 0 {
			tm.tokenTTL = time.Duration(ttlSeconds) * time.Second
		}
	}

	if maxTokensStr := strings.TrimSpace(os.Getenv("MAX_TOKENS_PER_USER")); maxTokensStr != "" {
		if maxTokens, err := strconv.Atoi(maxTokensStr); err == nil && maxTokens > 0 {
			tm.maxTokensPerUser = maxTokens
		}
	}

	if tm.tokenTTL > 0 {
		go tm.tokenCleanupWorker()
	}

	return tm, nil
}

func tokenTimestampToTime(value interface{}) time.Time {
	switch v := value.(type) {
	case int64:
		if v <= 0 {
			return time.Time{}
		}
		return time.Unix(v, 0)
	case int:
		if v <= 0 {
			return time.Time{}
		}
		return time.Unix(int64(v), 0)
	case float64:
		if v <= 0 {
			return time.Time{}
		}
		return time.Unix(int64(v), 0)
	case json.Number:
		i, err := v.Int64()
		if err == nil && i > 0 {
			return time.Unix(i, 0)
		}
		f, err := v.Float64()
		if err == nil && f > 0 {
			return time.Unix(int64(f), 0)
		}
	case string:
		if v == "" {
			return time.Time{}
		}
		i, err := strconv.ParseInt(v, 10, 64)
		if err == nil && i > 0 {
			return time.Unix(i, 0)
		}
	}

	return time.Time{}
}

func (tm *TokenManager) tokenCleanupWorker() {
	interval := tm.tokenTTL / 2
	if interval < 15*time.Second {
		interval = 15 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		tm.cleanupExpiredTokens()
	}
}

func (tm *TokenManager) cleanupExpiredTokens() {
	if tm.tokenTTL <= 0 {
		return
	}

	now := time.Now()
	expiredTokens := make(map[string]struct{})

	// Collect expired tokens from in-memory cache
	tm.mu.RLock()
	for token, rec := range tm.tokens {
		if rec == nil || rec.IssuedAt.IsZero() {
			continue
		}
		if now.Sub(rec.IssuedAt) >= tm.tokenTTL {
			expiredTokens[token] = struct{}{}
		}
	}
	tm.mu.RUnlock()

	// Collect expired tokens from database table (including replicated ones not in memory)
	if tm.db != nil {
		tm.db.mu.RLock()
		if systemDB, ok := tm.db.data["system"]; ok && systemDB.PrivateTables != nil {
			dbTokens := systemDB.PrivateTables["tokens"]
			for _, row := range dbTokens {
				tokenMap, ok := row.(map[string]interface{})
				if !ok {
					continue
				}
				token, _ := tokenMap["token"].(string)
				if token == "" {
					continue
				}

				issuedAt := tokenTimestampToTime(tokenMap["issued_at"])
				if issuedAt.IsZero() {
					issuedAt = tokenTimestampToTime(tokenMap["created_at"])
				}
				if issuedAt.IsZero() {
					continue
				}

				if now.Sub(issuedAt) >= tm.tokenTTL {
					expiredTokens[token] = struct{}{}
				}
			}
		}
		tm.db.mu.RUnlock()
	}

	if len(expiredTokens) == 0 {
		return
	}

	// Delete expired tokens through DB API so cleanup is replicated in cluster
	for token := range expiredTokens {
		_, _ = tm.db.DeleteRows("system", "tokens", map[string]string{"token": token}, true)
	}

	// Remove from in-memory cache
	tm.mu.Lock()
	for token := range expiredTokens {
		delete(tm.tokens, token)
	}
	tm.mu.Unlock()
}

func tokenRowIssuedAt(row map[string]interface{}) time.Time {
	if row == nil {
		return time.Time{}
	}
	issuedAt := tokenTimestampToTime(row["issued_at"])
	if !issuedAt.IsZero() {
		return issuedAt
	}
	return tokenTimestampToTime(row["created_at"])
}

func (tm *TokenManager) ensureUserTokenCapacityLocked(username string, incomingTokens int) error {
	if tm.maxTokensPerUser <= 0 || tm.db == nil {
		return nil
	}

	username = strings.TrimSpace(username)
	if username == "" {
		return nil
	}

	type tokenRowRef struct {
		Token    string
		IssuedAt time.Time
	}

	tm.db.mu.RLock()
	systemDB, ok := tm.db.data["system"]
	if !ok || systemDB == nil || systemDB.PrivateTables == nil {
		tm.db.mu.RUnlock()
		return nil
	}
	rows := systemDB.PrivateTables["tokens"]
	userTokens := make([]tokenRowRef, 0)
	for _, rawRow := range rows {
		row, ok := rawRow.(map[string]interface{})
		if !ok || row == nil {
			continue
		}
		if strings.TrimSpace(fmt.Sprintf("%v", row["username"])) != username {
			continue
		}
		token := strings.TrimSpace(fmt.Sprintf("%v", row["token"]))
		if token == "" {
			continue
		}
		userTokens = append(userTokens, tokenRowRef{
			Token:    token,
			IssuedAt: tokenRowIssuedAt(row),
		})
	}
	tm.db.mu.RUnlock()

	if incomingTokens < 0 {
		incomingTokens = 0
	}

	overflow := len(userTokens) + incomingTokens - tm.maxTokensPerUser
	if overflow <= 0 {
		return nil
	}

	sort.Slice(userTokens, func(i, j int) bool {
		left := userTokens[i]
		right := userTokens[j]
		if left.IssuedAt.Equal(right.IssuedAt) {
			return left.Token < right.Token
		}
		if left.IssuedAt.IsZero() {
			return true
		}
		if right.IssuedAt.IsZero() {
			return false
		}
		return left.IssuedAt.Before(right.IssuedAt)
	})

	for _, tokenRef := range userTokens[:overflow] {
		delete(tm.tokens, tokenRef.Token)
		if _, err := tm.db.DeleteRows("system", "tokens", map[string]string{"token": tokenRef.Token}, true); err != nil {
			return err
		}
	}

	return nil
}

// IssueToken issues a new JWT token for a user
func (tm *TokenManager) IssueToken(username string, isAdmin bool) (string, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Generate random nonce
	nonceBytes := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, nonceBytes); err != nil {
		return "", fmt.Errorf("failed to generate nonce: %w", err)
	}
	nonce := hex.EncodeToString(nonceBytes)

	// Create payload
	payload := JWTPayload{
		Username: username,
		IsAdmin:  isAdmin,
		IssuedAt: time.Now().Unix(),
		Nonce:    nonce,
	}

	if err := tm.ensureUserTokenCapacityLocked(username, 1); err != nil {
		return "", fmt.Errorf("failed to enforce max tokens per user: %w", err)
	}

	// Encrypt payload
	encryptedToken, err := tm.encryptPayload(&payload)
	if err != nil {
		return "", fmt.Errorf("failed to encrypt token: %w", err)
	}

	// Store unencrypted record in database for validation
	record := &TokenRecord{
		Token:    encryptedToken,
		Username: username,
		IsAdmin:  isAdmin,
		IssuedAt: time.Now(),
		Used:     false,
	}

	tm.tokens[encryptedToken] = record

	// Also store in system.tokens table
	tm.db.mu.Lock()
	if tm.db.data["system"].PrivateTables["tokens"] == nil {
		tm.db.data["system"].PrivateTables["tokens"] = make([]interface{}, 0)
	}
	tokenRow := map[string]interface{}{
		"token":      encryptedToken,
		"username":   username,
		"is_admin":   isAdmin,
		"issued_at":  record.IssuedAt.Unix(),
		"used":       false,
		"created_at": time.Now().Unix(),
	}
	tm.db.data["system"].PrivateTables["tokens"] = append(
		tm.db.data["system"].PrivateTables["tokens"],
		tokenRow,
	)
	tm.db.mu.Unlock()

	// Replicate token issuance to all peer servers
	if tm.replManager != nil {
		tm.replManager.EnqueueTokenIssueEvent(tokenRow)
	}

	return encryptedToken, nil
}

// ValidateToken validates a token and returns the username if valid
// Returns the username if valid, empty string if invalid or already used
func (tm *TokenManager) ValidateToken(encryptedToken string) (string, bool, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Check if token exists and hasn't been used
	record, exists := tm.tokens[encryptedToken]
	if !exists {
		return "", false, fmt.Errorf("token not found")
	}
	if record == nil {
		return "", false, fmt.Errorf("token not found")
	}

	if tm.tokenType == tokenTypeOneTime && record.Used {
		return "", false, fmt.Errorf("token already used")
	}

	// Decrypt and validate payload
	payload, err := tm.decryptPayload(encryptedToken)
	if err != nil {
		return "", false, fmt.Errorf("failed to decrypt token: %w", err)
	}

	return payload.Username, payload.IsAdmin, nil
}

// ConsumeToken marks a token as used and removes it
func (tm *TokenManager) ConsumeToken(encryptedToken string) error {
	if tm.tokenType == tokenTypeLifetime {
		// Lifetime tokens remain valid until expiration/flush.
		return nil
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	record, exists := tm.tokens[encryptedToken]
	if !exists {
		return fmt.Errorf("token not found")
	}

	if record.Used {
		return fmt.Errorf("token already used")
	}

	// Mark as used
	record.Used = true
	record.UsedAt = time.Now()

	// Update in system database - remove the used token
	if tm.db != nil {
		tm.db.mu.Lock()
		if systemDB, ok := tm.db.data["system"]; ok && systemDB.PrivateTables != nil {
			tokens := systemDB.PrivateTables["tokens"]
			newTokens := make([]interface{}, 0)
			for _, t := range tokens {
				if tokenMap, ok := t.(map[string]interface{}); ok {
					if tokenMap["token"] == encryptedToken {
						// Skip this token - delete it
						continue
					}
				}
				newTokens = append(newTokens, t)
			}
			systemDB.PrivateTables["tokens"] = newTokens
			tm.db.data["system"] = systemDB
		}
		tm.db.mu.Unlock()
	}

	// Replicate token consumption to all peer servers
	if tm.replManager != nil {
		tm.replManager.EnqueueTokenConsumeEvent(encryptedToken, record.UsedAt)
	}

	// Remove from in-memory cache
	delete(tm.tokens, encryptedToken)

	return nil
}

// TokenType returns current token behavior mode: onetime or lifetime.
func (tm *TokenManager) TokenType() string {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.tokenType
}

// TokenExpiration returns configured token expiration (0 means no expiration).
func (tm *TokenManager) TokenExpiration() time.Duration {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.tokenTTL
}

// MaxTokensPerUser returns the configured per-user token cap. Zero means unlimited.
func (tm *TokenManager) MaxTokensPerUser() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.maxTokensPerUser
}

// FlushTokens removes all tokens from the system
func (tm *TokenManager) FlushTokens() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.tokens = make(map[string]*TokenRecord)

	// Clear from database
	tm.db.mu.Lock()
	tm.db.data["system"].PrivateTables["tokens"] = make([]interface{}, 0)
	tm.db.mu.Unlock()
}

// encryptPayload encrypts a JWT payload using AES-256-GCM
func (tm *TokenManager) encryptPayload(payload *JWTPayload) (string, error) {
	// Marshal payload to JSON
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	// Generate random IV (96 bits for GCM)
	iv := make([]byte, 12)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return "", fmt.Errorf("failed to generate IV: %w", err)
	}

	// Create cipher
	block, err := aes.NewCipher(tm.aesKey[:])
	if err != nil {
		return "", fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("failed to create GCM: %w", err)
	}

	// Encrypt
	ciphertext := aead.Seal(iv, iv, payloadJSON, nil)

	// Return base64 encoded: IV + ciphertext
	return base64.RawURLEncoding.EncodeToString(ciphertext), nil
}

// decryptPayload decrypts an encrypted JWT payload
func (tm *TokenManager) decryptPayload(encryptedToken string) (*JWTPayload, error) {
	// Decode from base64
	ciphertext, err := base64.RawURLEncoding.DecodeString(encryptedToken)
	if err != nil {
		return nil, fmt.Errorf("failed to decode token: %w", err)
	}

	// Extract IV (first 12 bytes)
	if len(ciphertext) < 12 {
		return nil, fmt.Errorf("invalid token format")
	}
	iv := ciphertext[:12]
	ct := ciphertext[12:]

	// Create cipher
	block, err := aes.NewCipher(tm.aesKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Decrypt
	payloadJSON, err := aead.Open(nil, iv, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt token: %w", err)
	}

	// Unmarshal payload
	var payload JWTPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	return &payload, nil
}

// GetTokenCount returns the number of active tokens
func (tm *TokenManager) GetTokenCount() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.tokens)
}
