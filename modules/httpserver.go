package modules

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

const defaultMaxRequestBodyBytes int64 = 1 << 20 // 1 MiB
const defaultAuthWindow = 1 * time.Minute
const defaultAuthBlockDuration = 5 * time.Minute
const defaultMaxAuthAttemptsPerIP = 20
const defaultMaxAuthAttemptsPerUserIP = 8
const genericAuthErrorMessage = "invalid credentials"
const authRateLimitTable = "auth_rate_limits"

var errRequestBodyTooLarge = errors.New("request body too large")

type authRateState struct {
	windowStart  time.Time
	attempts     int
	blockedUntil time.Time
}

// HTTPServer handles HTTP requests for the SQL-based API
type HTTPServer struct {
	db           *Database
	parser       *SQLParser
	replManager  *ReplicationManager
	tokenManager *TokenManager
	wsServer     *WebSocketServer
	replToken    string
	maxBodyBytes int64
	authByIP     map[string]*authRateState
	authByUserIP map[string]*authRateState
	authWindow   time.Duration
	authBlockFor time.Duration
	mu           sync.RWMutex
}

// QueryRequest represents a query request
type QueryRequest struct {
	Query    string `json:"query"`
	Username string `json:"username"`
	Password string `json:"password"`
	Token    string `json:"token,omitempty"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	OK      bool        `json:"ok"`
	Data    interface{} `json:"data,omitempty"`
	Message string      `json:"message,omitempty"`
	Error   string      `json:"error,omitempty"`
	Token   string      `json:"token,omitempty"`
}

// NewHTTPServer creates a new HTTP server
func NewHTTPServer(db *Database, replManager *ReplicationManager, tokenManager *TokenManager, wsServer *WebSocketServer, peerServer *PeerServer, clusterMgr *ClusterManager, replToken string) *HTTPServer {
	hs := &HTTPServer{
		db:           db,
		parser:       NewSQLParser(db),
		replManager:  replManager,
		tokenManager: tokenManager,
		wsServer:     wsServer,
		replToken:    replToken,
		maxBodyBytes: defaultMaxRequestBodyBytes,
		authByIP:     make(map[string]*authRateState),
		authByUserIP: make(map[string]*authRateState),
		authWindow:   defaultAuthWindow,
		authBlockFor: defaultAuthBlockDuration,
	}
	hs.loadAuthRateLimitStateFromDB()
	return hs
}

func (hs *HTTPServer) readRequestBody(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, hs.maxBodyBytes+1))
	if err != nil {
		return nil, err
	}

	if int64(len(body)) > hs.maxBodyBytes {
		return nil, errRequestBodyTooLarge
	}

	return body, nil
}

func (hs *HTTPServer) validateReplicationToken(token string) bool {
	if hs.replToken == "" {
		return true
	}

	return subtle.ConstantTimeCompare([]byte(token), []byte(hs.replToken)) == 1
}

func normalizeUsernameForRateLimit(username string) string {
	u := strings.TrimSpace(strings.ToLower(username))
	if u == "" {
		return "_"
	}
	return u
}

func authUserIPKey(username, ip string) string {
	return normalizeUsernameForRateLimit(username) + "@" + ip
}

func toInt64(v interface{}) int64 {
	switch t := v.(type) {
	case int64:
		return t
	case int:
		return int64(t)
	case float64:
		return int64(t)
	case json.Number:
		i, err := t.Int64()
		if err == nil {
			return i
		}
		f, err := t.Float64()
		if err == nil {
			return int64(f)
		}
	case string:
		n := strings.TrimSpace(t)
		if n == "" {
			return 0
		}
		if i, err := json.Number(n).Int64(); err == nil {
			return i
		}
	}

	return 0
}

func (hs *HTTPServer) loadAuthRateLimitStateFromDB() {
	rows, err := hs.db.SelectRows("system", authRateLimitTable, nil, true)
	if err != nil {
		return
	}

	entries, ok := rows.([]interface{})
	if !ok {
		return
	}

	now := time.Now()

	hs.mu.Lock()
	defer hs.mu.Unlock()

	for _, e := range entries {
		row, ok := e.(map[string]interface{})
		if !ok {
			continue
		}

		scope, _ := row["scope"].(string)
		key, _ := row["key"].(string)
		if strings.TrimSpace(scope) == "" || strings.TrimSpace(key) == "" {
			continue
		}

		state := &authRateState{
			attempts: int(toInt64(row["attempts"])),
		}

		if ws := toInt64(row["window_start"]); ws > 0 {
			state.windowStart = time.Unix(ws, 0)
		}
		if bu := toInt64(row["blocked_until"]); bu > 0 {
			state.blockedUntil = time.Unix(bu, 0)
		}

		if state.attempts <= 0 && !now.Before(state.blockedUntil) {
			continue
		}

		switch scope {
		case "ip":
			hs.authByIP[key] = state
		case "userip":
			hs.authByUserIP[key] = state
		}
	}
}

func (hs *HTTPServer) persistAuthRateState(scope, key string, state *authRateState) {
	where := map[string]string{
		"scope": scope,
		"key":   key,
	}
	_, _ = hs.db.DeleteRows("system", authRateLimitTable, where, true)

	if state == nil {
		return
	}

	windowStart := int64(0)
	if !state.windowStart.IsZero() {
		windowStart = state.windowStart.Unix()
	}

	blockedUntil := int64(0)
	if !state.blockedUntil.IsZero() {
		blockedUntil = state.blockedUntil.Unix()
	}

	row := map[string]interface{}{
		"scope":         scope,
		"key":           key,
		"attempts":      state.attempts,
		"window_start":  windowStart,
		"blocked_until": blockedUntil,
		"updated_at":    time.Now().Unix(),
	}
	_, _ = hs.db.InsertRow("system", authRateLimitTable, row, true)
}

func stateRetryAfterSeconds(now time.Time, state *authRateState) int {
	if state == nil || !now.Before(state.blockedUntil) {
		return 0
	}
	remaining := int(state.blockedUntil.Sub(now).Seconds())
	if remaining < 1 {
		return 1
	}
	return remaining
}

func updateRateState(now time.Time, state *authRateState, maxAttempts int, window, blockFor time.Duration) (bool, int) {
	if state == nil {
		return true, 0
	}

	if now.Before(state.blockedUntil) {
		return false, stateRetryAfterSeconds(now, state)
	}

	if state.windowStart.IsZero() || now.Sub(state.windowStart) >= window {
		state.windowStart = now
		state.attempts = 0
	}

	if state.attempts >= maxAttempts {
		state.blockedUntil = now.Add(blockFor)
		return false, stateRetryAfterSeconds(now, state)
	}

	return true, 0
}

func incrementRateState(now time.Time, state *authRateState, window time.Duration) {
	if state.windowStart.IsZero() || now.Sub(state.windowStart) >= window {
		state.windowStart = now
		state.attempts = 0
	}
	state.attempts++
}

func (hs *HTTPServer) getClientIP(r *http.Request) string {
	forwarded := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if forwarded != "" {
		parts := strings.Split(forwarded, ",")
		if len(parts) > 0 {
			ip := strings.TrimSpace(parts[0])
			if ip != "" {
				return ip
			}
		}
	}

	realIP := strings.TrimSpace(r.Header.Get("X-Real-IP"))
	if realIP != "" {
		return realIP
	}

	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil && strings.TrimSpace(host) != "" {
		return host
	}

	if strings.TrimSpace(r.RemoteAddr) == "" {
		return "unknown"
	}

	return r.RemoteAddr
}

func (hs *HTTPServer) allowAuthAttempt(username, ip string) (bool, int) {
	now := time.Now()
	userKey := authUserIPKey(username, ip)
	var ipPersist *authRateState
	var userPersist *authRateState
	var shouldPersistIP bool
	var shouldPersistUser bool

	hs.mu.Lock()

	ipState := hs.authByIP[ip]
	if ipState == nil {
		ipState = &authRateState{}
		hs.authByIP[ip] = ipState
	}
	prevIPBlocked := ipState.blockedUntil
	allowedIP, retryIP := updateRateState(now, ipState, defaultMaxAuthAttemptsPerIP, hs.authWindow, hs.authBlockFor)
	if !ipState.blockedUntil.Equal(prevIPBlocked) {
		cp := *ipState
		ipPersist = &cp
		shouldPersistIP = true
	}
	if !allowedIP {
		hs.mu.Unlock()
		if shouldPersistIP {
			hs.persistAuthRateState("ip", ip, ipPersist)
		}
		return false, retryIP
	}

	userState := hs.authByUserIP[userKey]
	if userState == nil {
		userState = &authRateState{}
		hs.authByUserIP[userKey] = userState
	}
	prevUserBlocked := userState.blockedUntil
	allowedUser, retryUser := updateRateState(now, userState, defaultMaxAuthAttemptsPerUserIP, hs.authWindow, hs.authBlockFor)
	if !userState.blockedUntil.Equal(prevUserBlocked) {
		cp := *userState
		userPersist = &cp
		shouldPersistUser = true
	}
	hs.mu.Unlock()

	if shouldPersistIP {
		hs.persistAuthRateState("ip", ip, ipPersist)
	}
	if shouldPersistUser {
		hs.persistAuthRateState("userip", userKey, userPersist)
	}

	if !allowedUser {
		return false, retryUser
	}

	return true, 0
}

func (hs *HTTPServer) recordAuthResult(username, ip string, success bool) {
	now := time.Now()
	userKey := authUserIPKey(username, ip)
	var persistScopeKey []string
	var persistState []*authRateState

	hs.mu.Lock()

	if success {
		if st, ok := hs.authByUserIP[userKey]; ok {
			st.attempts = 0
			st.windowStart = now
			st.blockedUntil = time.Time{}
			cp := *st
			persistScopeKey = append(persistScopeKey, "userip", userKey)
			persistState = append(persistState, &cp)
		}
		hs.mu.Unlock()
		for i := 0; i < len(persistState); i++ {
			hs.persistAuthRateState(persistScopeKey[i*2], persistScopeKey[i*2+1], persistState[i])
		}
		return
	}

	ipState := hs.authByIP[ip]
	if ipState == nil {
		ipState = &authRateState{}
		hs.authByIP[ip] = ipState
	}
	incrementRateState(now, ipState, hs.authWindow)
	ipCopy := *ipState
	persistScopeKey = append(persistScopeKey, "ip", ip)
	persistState = append(persistState, &ipCopy)

	userState := hs.authByUserIP[userKey]
	if userState == nil {
		userState = &authRateState{}
		hs.authByUserIP[userKey] = userState
	}
	incrementRateState(now, userState, hs.authWindow)
	userCopy := *userState
	persistScopeKey = append(persistScopeKey, "userip", userKey)
	persistState = append(persistState, &userCopy)
	hs.mu.Unlock()

	for i := 0; i < len(persistState); i++ {
		hs.persistAuthRateState(persistScopeKey[i*2], persistScopeKey[i*2+1], persistState[i])
	}
}

// RegisterHandlers registers all HTTP handlers
func (hs *HTTPServer) RegisterHandlers() {
	http.HandleFunc("/api/query", hs.handleQuery)
	http.HandleFunc("/api/execute", hs.handleExecute)
	http.HandleFunc("/api/auth", hs.handleAuth)
	http.HandleFunc("/api/replicate", hs.handleReplicate)
	http.HandleFunc("/api/status", hs.handleStatus)
	http.HandleFunc("/api/databases/", hs.handleDatabasePublic)
	http.HandleFunc("/api/ws", hs.handleWebSocket)
	http.HandleFunc("/health", hs.handleHealth)
}

// handleQuery handles a query request with JWT token validation
func (hs *HTTPServer) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := hs.readRequestBody(r)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	// Authenticate and get username
	var username string
	var isAdmin bool
	var authErr error

	if req.Token != "" {
		// Validate JWT token
		username, isAdmin, authErr = hs.tokenManager.ValidateToken(req.Token)
		if authErr != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(QueryResponse{
				OK:    false,
				Error: authErr.Error(),
			})
			return
		}
		// Consume token (one-time use)
		hs.tokenManager.ConsumeToken(req.Token)
	} else if req.Username != "" && req.Password != "" {
		// Authenticate with credentials
		user, err := hs.db.AuthenticateUser(req.Username, req.Password)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(QueryResponse{
				OK:    false,
				Error: err.Error(),
			})
			return
		}
		username = user.Username
		isAdmin = user.IsAdmin
	} else {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: "authentication required",
		})
		return
	}

	// Check if this is a write operation
	isWriteOp := IsWriteOperation(req.Query)
	if isWriteOp && req.Token == "" && (req.Username == "" || req.Password == "") {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: "write operations require valid authentication",
		})
		return
	}

	// Parse and execute query
	cmd, err := hs.parser.Parse(req.Query)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: err.Error(),
		})
		return
	}

	// Execute query locally (replication to peers happens via write journal in background)
	result, err := hs.parser.Execute(cmd, username)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: err.Error(),
		})
		return
	}

	// Issue new token for next query (one-time use tokens)
	newToken, err := hs.tokenManager.IssueToken(username, isAdmin)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    true,
			Data:  result,
			Error: fmt.Sprintf("warning: failed to issue new token: %v", err),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(QueryResponse{
		OK:    true,
		Data:  result,
		Token: newToken,
	})
}

// handleExecute handles an execute request (same as query)
func (hs *HTTPServer) handleExecute(w http.ResponseWriter, r *http.Request) {
	hs.handleQuery(w, r)
}

// handleAuth handles authentication
func (hs *HTTPServer) handleAuth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := hs.readRequestBody(r)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var req QueryRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	ip := hs.getClientIP(r)
	allowed, retryAfter := hs.allowAuthAttempt(req.Username, ip)
	if !allowed {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", fmt.Sprintf("%d", retryAfter))
		w.WriteHeader(http.StatusTooManyRequests)
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: "too many authentication attempts, try again later",
		})
		return
	}

	if strings.TrimSpace(req.Username) == "" || strings.TrimSpace(req.Password) == "" {
		hs.recordAuthResult(req.Username, ip, false)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: genericAuthErrorMessage,
		})
		return
	}

	user, err := hs.db.AuthenticateUser(req.Username, req.Password)
	if err != nil {
		hs.recordAuthResult(req.Username, ip, false)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: genericAuthErrorMessage,
		})
		return
	}
	hs.recordAuthResult(req.Username, ip, true)

	// Issue JWT token
	token, err := hs.tokenManager.IssueToken(user.Username, user.IsAdmin)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(QueryResponse{
			OK:    false,
			Error: fmt.Sprintf("failed to issue token: %v", err),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(QueryResponse{
		OK:      true,
		Token:   token,
		Message: "authenticated",
	})
}

// handleReplicate handles replication events from peers
func (hs *HTTPServer) handleReplicate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Only accept from known source (in production, implement proper verification)
	isReplication := r.Header.Get("X-Replication") == "true"
	if !isReplication {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	if !hs.validateReplicationToken(r.Header.Get("X-Replication-Token")) {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	body, err := hs.readRequestBody(r)
	if err != nil {
		if errors.Is(err, errRequestBodyTooLarge) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var event ReplicationEvent
	if err := json.Unmarshal(body, &event); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if err := hs.replManager.ApplyReplicationEvent(event); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":    false,
			"error": err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok": true,
	})
}

// handleStatus handles status requests
func (hs *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	databases := hs.db.GetAllDatabases()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":        true,
		"databases": databases,
		"peers":     hs.replManager.GetPeers(),
	})
}

// handleWebSocket handles WebSocket upgrade requests
func (hs *HTTPServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if hs.wsServer == nil {
		http.Error(w, "WebSocket service not configured", http.StatusServiceUnavailable)
		return
	}
	hs.wsServer.HandleConnection(w, r)
}

// handleDatabasePublic handles GET requests for public database data
// Example: GET /api/databases/veiledersiden_no/public?table=posts
func (hs *HTTPServer) handleDatabasePublic(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse URL: /api/databases/{dbname}/public
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/databases/"), "/")
	if len(parts) < 2 {
		http.Error(w, "Invalid path format", http.StatusBadRequest)
		return
	}

	dbname := parts[0]
	endpoint := parts[1]

	if endpoint != "public" {
		http.Error(w, "Only public endpoint supported", http.StatusBadRequest)
		return
	}

	// Get table from query params
	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		http.Error(w, "table parameter required", http.StatusBadRequest)
		return
	}

	// Check if database and table exist
	// Lock the database for safe access
	hs.db.mu.RLock()
	dbInstance := hs.db.data[dbname]
	if dbInstance == nil {
		hs.db.mu.RUnlock()
		http.Error(w, "database not found", http.StatusNotFound)
		return
	}

	// Check if table is in public_tables or private_tables
	tableData, isPublic := dbInstance.PublicTables[tableName]
	privateTableData, isPrivate := dbInstance.PrivateTables[tableName]
	hs.db.mu.RUnlock()

	if isPublic {
		// Return public data without token needed
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":   true,
			"data": tableData,
		})
		return
	}

	if isPrivate {
		// Private table - require token
		token := r.URL.Query().Get("token")
		if token == "" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"ok":    false,
				"error": "token required to access private data",
			})
			return
		}

		// Validate token
		username, isAdmin, err := hs.tokenManager.ValidateToken(token)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"ok":    false,
				"error": err.Error(),
			})
			return
		}

		// Consume token and issue new one
		hs.tokenManager.ConsumeToken(token)
		newToken, _ := hs.tokenManager.IssueToken(username, isAdmin)

		// Return private data with new token
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"ok":    true,
			"data":  privateTableData,
			"token": newToken,
		})
		return
	}

	// Table not found
	http.Error(w, "table not found", http.StatusNotFound)
}

// handleHealth handles health checks
func (hs *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"ok":     true,
		"status": "healthy",
	})
}

// IsWriteOperation checks if a query is a write operation (INSERT/DELETE/UPDATE)
func IsWriteOperation(query string) bool {
	// Remove leading whitespace
	trimmed := strings.TrimSpace(strings.ToUpper(query))
	return strings.HasPrefix(trimmed, "INSERT") ||
		strings.HasPrefix(trimmed, "DELETE") ||
		strings.HasPrefix(trimmed, "UPDATE")
}
