package modules

import (
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WebSocketMessage represents a message sent to WebSocket clients
type WebSocketMessage struct {
	Type      string                 `json:"type"` // "data", "heartbeat", "token", "error"
	Data      map[string]interface{} `json:"data,omitempty"`
	Token     string                 `json:"token,omitempty"`
	Timestamp int64                  `json:"timestamp"`
	Database  string                 `json:"database,omitempty"`
	Query     string                 `json:"query,omitempty"`
}

// WebSocketRequest represents a request from WebSocket client
type WebSocketRequest struct {
	Type     string `json:"type"`     // "subscribe", "query", "auth"
	Database string `json:"database"` // Which database to monitor
	Table    string `json:"table"`    // Which table to monitor (empty = all tables)
	Query    string `json:"query"`    // SQL query for data
	Token    string `json:"token"`    // Access token
	Username string `json:"username"`
	Mode     int    `json:"mode"`     // 1 = onChange, 2 = interval
	Interval int    `json:"interval"` // Polling interval in seconds (for mode 2)
}

// WebSocketClient represents a connected WebSocket client
type WebSocketClient struct {
	ID              string
	conn            *websocket.Conn
	db              *Database
	tokenManager    *TokenManager
	mode            int // 1 = onChange, 2 = interval
	intervalSec     int
	database        string
	table           string
	query           string
	token           string
	user            *User
	send            chan *WebSocketMessage
	done            chan bool
	mu              sync.RWMutex
	lastHeartbeat   time.Time
	disconnectCount int
}

// WebSocketServer manages all WebSocket connections
type WebSocketServer struct {
	db           *Database
	tokenManager *TokenManager
	clients      map[string]*WebSocketClient
	mu           sync.RWMutex
	pingInterval time.Duration
	maxReconnect int
	upgrader     websocket.Upgrader
}

// NewWebSocketServer creates a new WebSocket server
func NewWebSocketServer(db *Database, tokenManager *TokenManager, pingIntervalSec int) *WebSocketServer {
	return &WebSocketServer{
		db:           db,
		tokenManager: tokenManager,
		clients:      make(map[string]*WebSocketClient),
		pingInterval: time.Duration(pingIntervalSec) * time.Second,
		maxReconnect: 5,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for now
			},
		},
	}
}

// HandleConnection upgrades HTTP connection to WebSocket
func (ws *WebSocketServer) HandleConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket upgrade error: %v", err)
		return
	}

	client := &WebSocketClient{
		ID:            fmt.Sprintf("ws-%d", time.Now().UnixNano()),
		conn:          conn,
		db:            ws.db,
		tokenManager:  ws.tokenManager,
		send:          make(chan *WebSocketMessage, 256),
		done:          make(chan bool),
		lastHeartbeat: time.Now(),
	}

	ws.mu.Lock()
	ws.clients[client.ID] = client
	ws.mu.Unlock()

	log.Printf("WebSocket client connected: %s", client.ID)

	// Handle incoming messages (blocking)
	go client.handleMessages()

	// Handle sending messages
	go client.handleSend()

	// Handle keep-alive ping
	go client.handleKeepAlive(ws.pingInterval)

	// Wait for done signal from handleMessages
	<-client.done

	// Cleanup
	conn.Close()
	ws.mu.Lock()
	delete(ws.clients, client.ID)
	ws.mu.Unlock()

	log.Printf("WebSocket client disconnected: %s", client.ID)
}

// handleMessages handles incoming messages from WebSocket client
func (client *WebSocketClient) handleMessages() {
	defer func() {
		client.conn.Close()
		client.done <- true
	}()

	// Don't set a global read deadline - use SetReadDeadline per message with pong handler
	client.conn.SetPongHandler(func(string) error {
		// Reset read deadline on pong
		client.conn.SetReadDeadline(time.Now().Add(120 * time.Second))
		client.mu.Lock()
		client.lastHeartbeat = time.Now()
		client.mu.Unlock()
		return nil
	})

	for {
		// Set read deadline for each message (3 minutes to be generous)
		client.conn.SetReadDeadline(time.Now().Add(3 * time.Minute))

		var req WebSocketRequest
		err := client.conn.ReadJSON(&req)
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error for client %s: %v", client.ID, err)
			}
			return
		}

		switch req.Type {
		case "auth":
			client.handleAuth(req)
		case "subscribe":
			client.handleSubscribe(req)
		case "query":
			client.handleQuery(req)
		default:
			client.sendError("Unknown request type: " + req.Type)
		}
	}
}

// handleAuth authenticates the client and issues a new token
func (client *WebSocketClient) handleAuth(req WebSocketRequest) {
	// Validate token if provided
	var username string
	var isAdmin bool
	var err error

	if req.Token != "" {
		// Validate existing token
		username, isAdmin, err = client.tokenManager.ValidateToken(req.Token)
		if err != nil {
			client.sendError("Invalid token: " + err.Error())
			return
		}
		// Consume the provided token
		client.tokenManager.ConsumeToken(req.Token)
	} else {
		client.sendError("No token provided")
		return
	}

	// Issue new token for this connection
	newToken, err := client.tokenManager.IssueToken(username, isAdmin)
	if err != nil {
		client.sendError("Failed to issue token: " + err.Error())
		return
	}

	// Store authenticated user and token
	client.mu.Lock()
	client.token = newToken
	client.user = &User{
		Username: username,
		IsAdmin:  isAdmin,
	}
	client.mu.Unlock()

	// Send new token to client
	client.send <- &WebSocketMessage{
		Type:      "token",
		Token:     newToken,
		Timestamp: time.Now().Unix(),
	}

	log.Printf("WebSocket client %s authenticated as %s", client.ID, username)
}

// handleSubscribe subscribes to database changes (optional authentication)
func (client *WebSocketClient) handleSubscribe(req WebSocketRequest) {
	// Authentication is optional:
	// - Without auth: access only public data
	// - With auth: access public + private (if user owns database)

	// Ensure this is a SELECT query if provided (read-only)
	if req.Query != "" && IsWriteOperation(req.Query) {
		client.sendError("WebSocket does not support write operations. Use HTTP API instead.")
		return
	}

	client.mu.Lock()
	client.mode = req.Mode
	client.database = req.Database
	client.table = req.Table
	client.query = req.Query
	// Set default interval to 5 seconds if not provided
	if req.Interval > 0 {
		client.intervalSec = req.Interval
	} else {
		client.intervalSec = 5
	}
	client.mu.Unlock()

	// Send initial data
	client.sendInitialData()

	// Start data watcher based on mode
	if req.Mode == 1 {
		// Mode 1: onChange - watch for database changes (TODO: implement change notification)
		log.Printf("Client %s subscribed to %s in onChange mode", client.ID, req.Database)
	} else if req.Mode == 2 {
		// Mode 2: interval - send data at regular intervals
		if client.user != nil {
			log.Printf("Client %s subscribed to %s in interval mode (%ds) as %s", client.ID, req.Database, client.intervalSec, client.user.Username)
		} else {
			log.Printf("Client %s subscribed to %s in interval mode (%ds) (unauthenticated)", client.ID, req.Database, client.intervalSec)
		}
		go client.handleIntervalMode()
	}
}

// handleQuery executes a query and sends results (requires authentication)
func (client *WebSocketClient) handleQuery(req WebSocketRequest) {
	if client.user == nil {
		client.sendError("Not authenticated. Send auth request first.")
		return
	}

	// Ensure this is a SELECT query (read-only)
	if IsWriteOperation(req.Query) {
		client.sendError("WebSocket does not support write operations. Use HTTP API instead.")
		return
	}

	// Execute query:
	// - owner gets both public + private rows for the requested table
	// - non-owner gets public rows only
	isOwner := client.user.Username == req.Database
	result, err := client.selectTableWithOwnership(req.Database, req.Query, isOwner)
	if err != nil {
		client.sendError(fmt.Sprintf("Query error: %v", err))
		return
	}

	client.send <- &WebSocketMessage{
		Type:      "data",
		Data:      map[string]interface{}{"result": result},
		Database:  req.Database,
		Query:     req.Query,
		Timestamp: time.Now().Unix(),
	}
}

// selectTableWithOwnership returns rows for a specific table based on ownership.
// Owners receive public + private rows; non-owners receive public rows only.
func (client *WebSocketClient) selectTableWithOwnership(database, table string, isOwner bool) (interface{}, error) {
	if !isOwner {
		return client.db.SelectRows(database, table, make(map[string]string), false)
	}

	publicRowsRaw, err := client.db.SelectRows(database, table, make(map[string]string), false)
	if err != nil {
		return nil, err
	}

	privateRowsRaw, err := client.db.SelectRows(database, table, make(map[string]string), true)
	if err != nil {
		return nil, err
	}

	publicRows, _ := publicRowsRaw.([]interface{})
	privateRows, _ := privateRowsRaw.([]interface{})

	merged := make([]interface{}, 0, len(publicRows)+len(privateRows))
	merged = append(merged, publicRows...)
	merged = append(merged, privateRows...)

	return merged, nil
}

// buildSubscriptionResult builds explicit public/private subscription payload.
// For owners: {"public": {...}, "private": {...}}
// For guests: {"public": {...}}
func (client *WebSocketClient) buildSubscriptionResult(database, table string, isOwner bool) (interface{}, error) {
	result := make(map[string]interface{})

	if table != "" {
		publicRowsRaw, err := client.db.SelectRows(database, table, make(map[string]string), false)
		if err != nil {
			return nil, err
		}
		publicRows, _ := publicRowsRaw.([]interface{})
		result["public"] = map[string]interface{}{table: publicRows}

		if isOwner {
			privateRowsRaw, err := client.db.SelectRows(database, table, make(map[string]string), true)
			if err != nil {
				return nil, err
			}
			privateRows, _ := privateRowsRaw.([]interface{})
			result["private"] = map[string]interface{}{table: privateRows}
		}

		return result, nil
	}

	publicTables, err := client.db.GetAllTables(database, false)
	if err != nil {
		return nil, err
	}
	result["public"] = publicTables

	if isOwner {
		privateTables, err := client.db.GetAllTables(database, true)
		if err != nil {
			return nil, err
		}
		result["private"] = privateTables
	}

	return result, nil
}

// sendInitialData sends the initial data to the client
func (client *WebSocketClient) sendInitialData() {
	client.mu.RLock()
	database := client.database
	table := client.table
	var username string
	var isOwner bool
	if client.user != nil {
		username = client.user.Username
		isOwner = username == database
	}
	client.mu.RUnlock()

	if database == "" {
		return
	}

	// Get data based on authentication status
	var result interface{}
	var err error

	result, err = client.buildSubscriptionResult(database, table, isOwner)

	if err != nil {
		client.sendError(fmt.Sprintf("Failed to get initial data: %v", err))
		return
	}

	client.send <- &WebSocketMessage{
		Type:      "data",
		Data:      map[string]interface{}{"result": result},
		Database:  database,
		Timestamp: time.Now().Unix(),
	}
}

// handleIntervalMode sends data at regular intervals
func (client *WebSocketClient) handleIntervalMode() {
	client.mu.RLock()
	interval := client.intervalSec
	database := client.database
	client.mu.RUnlock()

	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-client.done:
			return
		case <-ticker.C:
			// Check current auth state (may have changed since subscription started)
			client.mu.RLock()
			var isOwner bool
			if client.user != nil {
				isOwner = client.user.Username == database
			}
			currentDatabase := client.database
			currentTable := client.table
			client.mu.RUnlock()

			// Fetch current data
			var result interface{}
			var err error

			result, err = client.buildSubscriptionResult(currentDatabase, currentTable, isOwner)

			if err != nil {
				client.sendError(fmt.Sprintf("Interval query error: %v", err))
				continue
			}

			client.send <- &WebSocketMessage{
				Type:      "data",
				Data:      map[string]interface{}{"result": result},
				Database:  currentDatabase,
				Timestamp: time.Now().Unix(),
			}
		}
	}
}

// handleKeepAlive sends periodic ping messages
func (client *WebSocketClient) handleKeepAlive(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-client.done:
			return
		case <-ticker.C:
			err := client.conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second))
			if err != nil {
				log.Printf("Ping error for client %s: %v", client.ID, err)
				return
			}
		}
	}
}

// handleSend handles outgoing messages
func (client *WebSocketClient) handleSend() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msg := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.conn.WriteJSON(msg); err != nil {
				log.Printf("Write error for client %s: %v", client.ID, err)
				return
			}

		case <-ticker.C:
			// Send heartbeat every 30 seconds
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.conn.WriteJSON(&WebSocketMessage{
				Type:      "heartbeat",
				Timestamp: time.Now().Unix(),
			}); err != nil {
				log.Printf("Heartbeat error for client %s: %v", client.ID, err)
				return
			}

		case <-client.done:
			return
		}
	}
}

// sendError sends an error message to the client
func (client *WebSocketClient) sendError(errMsg string) {
	client.send <- &WebSocketMessage{
		Type:      "error",
		Data:      map[string]interface{}{"message": errMsg},
		Timestamp: time.Now().Unix(),
	}
}

// RegisterWebSocketHandler registers the WebSocket handler on the HTTP server
func RegisterWebSocketHandler(ws *WebSocketServer) {
	http.HandleFunc("/ws", ws.HandleConnection)
	http.HandleFunc("/websocket", ws.HandleConnection)
}
