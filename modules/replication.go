package modules

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ReplicationManager handles replication to peer servers
type ReplicationManager struct {
	db              *Database
	peers           []string // peer server addresses like "localhost:9090"
	replToken       string
	mu              sync.RWMutex
	replicationChan chan ReplicationEvent
	maxRetries      int
	retryDelay      time.Duration
	eventCounter    uint64
	processedEvents map[string]time.Time
	processedOrder  []string
	maxProcessed    int
	httpClient      *http.Client // Shared HTTP client with connection pooling
}

// ReplicationEvent represents an event to replicate to peers
type ReplicationEvent struct {
	EventID   string            `json:"event_id,omitempty"`
	Type      string            `json:"type"` // INSERT, DELETE, UPDATE, USER_ADD
	Database  string            `json:"database"`
	Table     string            `json:"table"`
	Data      interface{}       `json:"data"`
	Where     map[string]string `json:"where,omitempty"`
	IsPrivate bool              `json:"is_private"`
	Timestamp time.Time         `json:"timestamp"`
	Username  string            `json:"username"`
}

// NewReplicationManager creates a new replication manager
func NewReplicationManager(db *Database, peers []string, replToken string) *ReplicationManager {
	// Create HTTP client with connection pooling
	transport := &http.Transport{
		MaxIdleConns:        100,              // Total idle connections across all hosts
		MaxIdleConnsPerHost: 10,               // Idle connections per peer
		IdleConnTimeout:     90 * time.Second, // Keep connections alive
		DisableKeepAlives:   false,            // Enable keep-alive
		ForceAttemptHTTP2:   true,             // Use HTTP/2 if available
	}

	rm := &ReplicationManager{
		db:              db,
		peers:           peers,
		replToken:       replToken,
		replicationChan: make(chan ReplicationEvent, 100),
		maxRetries:      3,
		retryDelay:      1 * time.Second,
		processedEvents: make(map[string]time.Time),
		processedOrder:  make([]string, 0, 1024),
		maxProcessed:    10000,
		httpClient: &http.Client{
			Timeout:   5 * time.Second,
			Transport: transport,
		},
	}

	// Start replication worker
	go rm.replicationWorker()

	return rm
}

// replicationWorker processes replication events
func (rm *ReplicationManager) replicationWorker() {
	for event := range rm.replicationChan {
		rm.replicateEvent(event)
	}
}

// EnqueueInsertEvent enqueues an INSERT event for replication
func (rm *ReplicationManager) EnqueueInsertEvent(database, table string, data interface{}, isPrivate bool, username string) {
	rm.replicationChan <- ReplicationEvent{
		EventID:   rm.nextEventID("INSERT"),
		Type:      "INSERT",
		Database:  database,
		Table:     table,
		Data:      data,
		IsPrivate: isPrivate,
		Timestamp: time.Now(),
		Username:  username,
	}
}

// EnqueueDeleteEvent enqueues a DELETE event for replication
func (rm *ReplicationManager) EnqueueDeleteEvent(database, table string, where map[string]string, isPrivate bool, username string) {
	rm.replicationChan <- ReplicationEvent{
		EventID:   rm.nextEventID("DELETE"),
		Type:      "DELETE",
		Database:  database,
		Table:     table,
		Where:     where,
		IsPrivate: isPrivate,
		Timestamp: time.Now(),
		Username:  username,
	}
}

// EnqueueUserAddEvent enqueues a USER_ADD event for replication
func (rm *ReplicationManager) EnqueueUserAddEvent(username, passwordHash string) {
	rm.replicationChan <- ReplicationEvent{
		EventID:   rm.nextEventID("USER_ADD"),
		Type:      "USER_ADD",
		Username:  username,
		Data:      map[string]string{"username": username, "password_hash": passwordHash},
		Timestamp: time.Now(),
	}
}

// replicateEvent sends a replication event to all peers
func (rm *ReplicationManager) replicateEvent(event ReplicationEvent) {
	if strings.TrimSpace(event.EventID) == "" {
		event.EventID = rm.nextEventID(event.Type)
	}

	rm.mu.RLock()
	peers := make([]string, len(rm.peers))
	copy(peers, rm.peers)
	rm.mu.RUnlock()

	payload, err := json.Marshal(event)
	if err != nil {
		fmt.Printf("failed to marshal replication event: %v\n", err)
		return
	}

	for _, peer := range peers {
		go rm.sendToPeer(peer, payload, 0)
	}
}

func (rm *ReplicationManager) nextEventID(eventType string) string {
	seq := atomic.AddUint64(&rm.eventCounter, 1)
	etype := strings.ToLower(strings.TrimSpace(eventType))
	if etype == "" {
		etype = "event"
	}
	return fmt.Sprintf("%s-%d-%d", etype, time.Now().UnixNano(), seq)
}

func (rm *ReplicationManager) isDuplicateEvent(eventID string) bool {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return false
	}

	rm.mu.RLock()
	_, exists := rm.processedEvents[eventID]
	rm.mu.RUnlock()
	return exists
}

func (rm *ReplicationManager) markEventProcessed(eventID string) {
	eventID = strings.TrimSpace(eventID)
	if eventID == "" {
		return
	}

	rm.mu.Lock()
	defer rm.mu.Unlock()

	if _, exists := rm.processedEvents[eventID]; exists {
		return
	}

	rm.processedEvents[eventID] = time.Now()
	rm.processedOrder = append(rm.processedOrder, eventID)

	if len(rm.processedOrder) > rm.maxProcessed {
		oldest := rm.processedOrder[0]
		rm.processedOrder = rm.processedOrder[1:]
		delete(rm.processedEvents, oldest)
	}
}

func getInt64Value(data map[string]interface{}, key string) int64 {
	v, ok := data[key]
	if !ok {
		return 0
	}

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
		i, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64)
		if err == nil {
			return i
		}
	}

	return 0
}

// sendToPeer sends a replication event to a peer
func (rm *ReplicationManager) sendToPeer(peer string, payload []byte, attempt int) {
	if attempt >= rm.maxRetries {
		fmt.Printf("failed to replicate to peer %s after %d attempts\n", peer, rm.maxRetries)
		return
	}

	url := fmt.Sprintf("http://%s/api/replicate", peer)
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(payload))
	if err != nil {
		fmt.Printf("failed to create request to peer %s: %v\n", peer, err)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Replication", "true")
	if rm.replToken != "" {
		req.Header.Set("X-Replication-Token", rm.replToken)
	}

	// Use shared HTTP client with connection pooling
	resp, err := rm.httpClient.Do(req)
	if err != nil {
		fmt.Printf("failed to send replication to peer %s (attempt %d): %v\n", peer, attempt+1, err)
		time.Sleep(rm.retryDelay)
		rm.sendToPeer(peer, payload, attempt+1)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Printf("replication to peer %s returned status %d: %s\n", peer, resp.StatusCode, string(body))
		time.Sleep(rm.retryDelay)
		rm.sendToPeer(peer, payload, attempt+1)
		return
	}
}

// AddPeer adds a new peer to the replication list
func (rm *ReplicationManager) AddPeer(peer string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	for _, p := range rm.peers {
		if p == peer {
			return // Already exists
		}
	}

	rm.peers = append(rm.peers, peer)
}

// RemovePeer removes a peer from the replication list
func (rm *ReplicationManager) RemovePeer(peer string) {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	var newPeers []string
	for _, p := range rm.peers {
		if p != peer {
			newPeers = append(newPeers, p)
		}
	}

	rm.peers = newPeers
}

// GetPeers returns all current peers
func (rm *ReplicationManager) GetPeers() []string {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	peers := make([]string, len(rm.peers))
	copy(peers, rm.peers)
	return peers
}

// Close gracefully shuts down the replication manager
func (rm *ReplicationManager) Close() {
	close(rm.replicationChan)
}

// ApplyReplicationEvent applies a replication event received from a peer
func (rm *ReplicationManager) ApplyReplicationEvent(event ReplicationEvent) error {
	if rm.isDuplicateEvent(event.EventID) {
		return nil
	}

	switch event.Type {
	case "INSERT":
		// Convert to map if needed
		if rowData, ok := event.Data.(map[string]interface{}); ok {
			err := rm.db.InsertRowDirect(event.Database, event.Table, rowData, event.IsPrivate)
			if err == nil {
				rm.markEventProcessed(event.EventID)
			}
			return err
		}
		return fmt.Errorf("invalid INSERT event data")

	case "DELETE":
		_, err := rm.db.DeleteRowsDirect(event.Database, event.Table, event.Where, event.IsPrivate)
		if err == nil {
			rm.markEventProcessed(event.EventID)
		}
		return err

	case "USER_ADD":
		// User addition is handled separately
		rm.markEventProcessed(event.EventID)
		return nil

	case "TOKEN_ISSUE":
		// Token issuance - add to system.tokens table
		if tokenData, ok := event.Data.(map[string]interface{}); ok {
			err := rm.db.InsertRowDirect("system", "tokens", tokenData, true)
			if err == nil {
				rm.markEventProcessed(event.EventID)
			}
			return err
		}
		return fmt.Errorf("invalid TOKEN_ISSUE event data")

	case "TOKEN_CONSUME":
		// Token consumption - mark as used
		if tokenData, ok := event.Data.(map[string]interface{}); ok {
			token, _ := tokenData["token"].(string)
			usedAt := getInt64Value(tokenData, "used_at")

			rm.db.mu.Lock()
			tokens := rm.db.data["system"].PrivateTables["tokens"]
			for _, t := range tokens {
				if tokenMap, ok := t.(map[string]interface{}); ok {
					if tokenMap["token"] == token {
						tokenMap["used"] = true
						tokenMap["used_at"] = usedAt
						break
					}
				}
			}
			rm.db.mu.Unlock()
			rm.markEventProcessed(event.EventID)
			return nil
		}
		return fmt.Errorf("invalid TOKEN_CONSUME event data")

	default:
		return fmt.Errorf("unknown replication event type: %s", event.Type)
	}
}

// EnqueueTokenIssueEvent enqueues a token issuance event for replication
func (rm *ReplicationManager) EnqueueTokenIssueEvent(tokenData map[string]interface{}) {
	event := ReplicationEvent{
		EventID:   rm.nextEventID("TOKEN_ISSUE"),
		Type:      "TOKEN_ISSUE",
		Database:  "system",
		Table:     "tokens",
		Data:      tokenData,
		IsPrivate: true,
		Timestamp: time.Now(),
	}

	select {
	case rm.replicationChan <- event:
	default:
		// Channel full, log warning
		fmt.Println("Warning: replication channel full, token issue event dropped")
	}
}

// EnqueueTokenConsumeEvent enqueues a token consumption event for replication
func (rm *ReplicationManager) EnqueueTokenConsumeEvent(token string, usedAt time.Time) {
	event := ReplicationEvent{
		EventID:  rm.nextEventID("TOKEN_CONSUME"),
		Type:     "TOKEN_CONSUME",
		Database: "system",
		Table:    "tokens",
		Data: map[string]interface{}{
			"token":   token,
			"used_at": usedAt.Unix(),
		},
		IsPrivate: true,
		Timestamp: time.Now(),
	}

	select {
	case rm.replicationChan <- event:
	default:
		// Channel full, log warning
		fmt.Println("Warning: replication channel full, token consume event dropped")
	}
}
