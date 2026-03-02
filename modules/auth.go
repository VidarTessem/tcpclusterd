package modules

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// User represents a user in the system
type User struct {
	Username     string    `json:"username"`
	PasswordHash string    `json:"password_hash"`
	CreatedAt    time.Time `json:"created_at"`
	IsAdmin      bool      `json:"is_admin"`
}

// JWT represents a JSON Web Token (simplified implementation)
type JWT struct {
	Username  string    `json:"username"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
	Signature string    `json:"signature"`
}

// AuthManager handles user authentication and JWT tokens
type AuthManager struct {
	users          map[string]*User
	mu             sync.RWMutex
	jwtSecret      string
	runtimePath    string
	persistEnabled bool
}

var authManager *AuthManager

// InitAuthManager initializes the authentication manager
func InitAuthManager(jwtSecret string, runtimePath string, persistEnabled bool) {
	authManager = &AuthManager{
		users:          make(map[string]*User),
		jwtSecret:      jwtSecret,
		runtimePath:    runtimePath,
		persistEnabled: persistEnabled,
	}

	// Load users from disk if persistence is enabled
	if persistEnabled && runtimePath != "" {
		if err := authManager.loadUsers(); err != nil {
			log.Printf("[AUTH] Failed to load users: %v", err)
		}
	}
}

// GetAuthManager returns the auth manager instance
func GetAuthManager() *AuthManager {
	return authManager
}

// hashPassword creates a SHA-256 hash of the password
func hashPassword(password string) string {
	hash := sha256.Sum256([]byte(password))
	return hex.EncodeToString(hash[:])
}

// CreateUser creates a new user
func (am *AuthManager) CreateUser(username, password string, isAdmin bool) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	if _, exists := am.users[username]; exists {
		return errors.New("user already exists")
	}

	user := &User{
		Username:     username,
		PasswordHash: hashPassword(password),
		CreatedAt:    time.Now(),
		IsAdmin:      isAdmin,
	}

	am.users[username] = user

	// Persist if enabled
	if am.persistEnabled {
		go am.saveUsers()
	}

	log.Printf("[AUTH] User created: %s (admin: %v)", username, isAdmin)
	return nil
}

// Authenticate validates username and password
func (am *AuthManager) Authenticate(username, password string) (*User, error) {
	am.mu.RLock()
	defer am.mu.RUnlock()

	user, exists := am.users[username]
	if !exists {
		return nil, errors.New("invalid credentials")
	}

	if user.PasswordHash != hashPassword(password) {
		return nil, errors.New("invalid credentials")
	}

	return user, nil
}

// GenerateToken creates a JWT token for a user
func (am *AuthManager) GenerateToken(username string) (string, error) {
	am.mu.RLock()
	user, exists := am.users[username]
	am.mu.RUnlock()

	if !exists {
		return "", errors.New("user not found")
	}

	jwt := JWT{
		Username:  username,
		IssuedAt:  time.Now(),
		ExpiresAt: time.Now().Add(24 * time.Hour), // Token valid for 24 hours
	}

	// Create signature
	tokenData := fmt.Sprintf("%s:%s:%s", jwt.Username, jwt.IssuedAt.Format(time.RFC3339), jwt.ExpiresAt.Format(time.RFC3339))
	hash := sha256.Sum256([]byte(tokenData + am.jwtSecret))
	jwt.Signature = hex.EncodeToString(hash[:])

	// Encode as base64 JSON
	jsonData, err := json.Marshal(jwt)
	if err != nil {
		return "", err
	}

	token := base64.StdEncoding.EncodeToString(jsonData)
	log.Printf("[AUTH] Token generated for user: %s (admin: %v)", username, user.IsAdmin)
	return token, nil
}

// ValidateToken validates a JWT token and returns the username if valid
func (am *AuthManager) ValidateToken(tokenString string) (string, bool, error) {
	// Decode base64
	jsonData, err := base64.StdEncoding.DecodeString(tokenString)
	if err != nil {
		return "", false, errors.New("invalid token format")
	}

	var jwt JWT
	if err := json.Unmarshal(jsonData, &jwt); err != nil {
		return "", false, errors.New("invalid token format")
	}

	// Check expiration
	if time.Now().After(jwt.ExpiresAt) {
		return "", false, errors.New("token expired")
	}

	// Verify signature
	tokenData := fmt.Sprintf("%s:%s:%s", jwt.Username, jwt.IssuedAt.Format(time.RFC3339), jwt.ExpiresAt.Format(time.RFC3339))
	hash := sha256.Sum256([]byte(tokenData + am.jwtSecret))
	expectedSignature := hex.EncodeToString(hash[:])

	if jwt.Signature != expectedSignature {
		return "", false, errors.New("invalid token signature")
	}

	// Check if user still exists and get admin status
	am.mu.RLock()
	user, exists := am.users[jwt.Username]
	am.mu.RUnlock()

	if !exists {
		return "", false, errors.New("user not found")
	}

	return jwt.Username, user.IsAdmin, nil
}

// saveUsers persists users to disk
func (am *AuthManager) saveUsers() error {
	if am.runtimePath == "" {
		return errors.New("runtime path not set")
	}

	am.mu.RLock()
	defer am.mu.RUnlock()

	usersFile := filepath.Join(am.runtimePath, "users.json")

	data, err := json.MarshalIndent(am.users, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal users: %v", err)
	}

	if err := os.WriteFile(usersFile, data, 0600); err != nil {
		return fmt.Errorf("failed to write users file: %v", err)
	}

	return nil
}

// loadUsers loads users from disk
func (am *AuthManager) loadUsers() error {
	if am.runtimePath == "" {
		return errors.New("runtime path not set")
	}

	usersFile := filepath.Join(am.runtimePath, "users.json")

	// Check if file exists
	if _, err := os.Stat(usersFile); os.IsNotExist(err) {
		log.Printf("[AUTH] No users file found, starting fresh")
		return nil
	}

	data, err := os.ReadFile(usersFile)
	if err != nil {
		return fmt.Errorf("failed to read users file: %v", err)
	}

	am.mu.Lock()
	defer am.mu.Unlock()

	if err := json.Unmarshal(data, &am.users); err != nil {
		return fmt.Errorf("failed to unmarshal users: %v", err)
	}

	log.Printf("[AUTH] Loaded %d users from disk", len(am.users))
	return nil
}

// GenerateJWTSecret generates a random JWT secret
func GenerateJWTSecret() string {
	bytes := make([]byte, 32)
	if _, err := rand.Read(bytes); err != nil {
		log.Printf("Warning: failed to generate random JWT secret: %v, using placeholder", err)
		return "placeholder-jwt-secret-change-this"
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

// GetUserCount returns the number of registered users
func (am *AuthManager) GetUserCount() int {
	am.mu.RLock()
	defer am.mu.RUnlock()
	return len(am.users)
}

// ChangePassword changes a user's password
func (am *AuthManager) ChangePassword(username, oldPassword, newPassword string) error {
	am.mu.Lock()
	defer am.mu.Unlock()

	user, exists := am.users[username]
	if !exists {
		return errors.New("user not found")
	}

	if user.PasswordHash != hashPassword(oldPassword) {
		return errors.New("invalid old password")
	}

	user.PasswordHash = hashPassword(newPassword)

	// Persist if enabled
	if am.persistEnabled {
		go am.saveUsers()
	}

	log.Printf("[AUTH] Password changed for user: %s", username)
	return nil
}
