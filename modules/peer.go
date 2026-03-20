package modules

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// PeerServer handles TCP connections from other cluster peers
type PeerServer struct {
	mu                 sync.RWMutex
	listener           net.Listener
	peers              map[string]*PeerConnection // address -> PeerConnection
	clusterManager     *ClusterManager
	db                 *Database
	logger             *Logger
	isRunning          bool
	writeOperationChan chan *WriteOperation
	applyRemoteWrite   func(*WriteOperation) error
	// Replication stats for metrics
	statsLock           sync.RWMutex
	writesReplicated    map[string]int64     // peer address -> count
	replicationFailures map[string]int64     // peer address -> count
	replicationLag      map[string]int64     // peer address -> milliseconds
	lastPingTime        map[string]time.Time // peer address -> last ping
}

// PeerConnection represents a connection to a remote peer
type PeerConnection struct {
	address    string
	conn       net.Conn
	isOutbound bool // true if we initiated the connection
	lastSeen   time.Time
}

// WriteOperation represents a database write to replicate
type WriteOperation struct {
	ID         string                 `json:"id"`
	Database   string                 `json:"database"`
	Table      string                 `json:"table"`
	Operation  string                 `json:"operation"` // insert, update, delete, add_user, remove_user, flush_tokens, configure_service
	Data       map[string]interface{} `json:"data"`
	Where      map[string]string      `json:"where,omitempty"`
	IsPrivate  bool                   `json:"is_private"`
	Timestamp  int64                  `json:"timestamp"`
	SourcePeer string                 `json:"source_peer"`
}

// ReplicationMessage is sent between peers
type ReplicationMessage struct {
	Type         string          `json:"type"` // "write", "ack", "heartbeat", "sync_request", "sync_response"
	WriteOp      *WriteOperation `json:"write_op,omitempty"`
	MessageID    string          `json:"message_id"`
	FromPeer     string          `json:"from_peer,omitempty"`
	Timestamp    int64           `json:"timestamp"`
	SyncData     []byte          `json:"sync_data,omitempty"`     // Full DB snapshot for sync_response
	SyncChecksum string          `json:"sync_checksum,omitempty"` // Hash of database state
}

// NewPeerServer creates a new peer server
func NewPeerServer(localPeerAddr string, clusterManager *ClusterManager, db *Database, logger *Logger, applyRemoteWrite func(*WriteOperation) error) (*PeerServer, error) {
	ps := &PeerServer{
		peers:               make(map[string]*PeerConnection),
		clusterManager:      clusterManager,
		db:                  db,
		logger:              logger,
		writeOperationChan:  make(chan *WriteOperation, 100),
		applyRemoteWrite:    applyRemoteWrite,
		writesReplicated:    make(map[string]int64),
		replicationFailures: make(map[string]int64),
		replicationLag:      make(map[string]int64),
		lastPingTime:        make(map[string]time.Time),
	}

	// Start listening for incoming peer connections
	listener, err := net.Listen("tcp", localPeerAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", localPeerAddr, err)
	}

	ps.listener = listener
	ps.isRunning = true

	logger.Info("Peer server listening on %s", localPeerAddr)

	// Start accepting connections in background
	go ps.acceptConnections()

	// Start outbound connection manager
	go ps.manageOutboundConnections()

	// Start replication handler
	go ps.handleReplication()

	// Start write journal watcher to replicate writes to peers
	go ps.watchWriteJournal()

	return ps, nil
}

// acceptConnections accepts incoming connections from remote peers
func (ps *PeerServer) acceptConnections() {
	for ps.isRunning {
		conn, err := ps.listener.Accept()
		if err != nil {
			if ps.isRunning {
				if !strings.Contains(strings.ToLower(err.Error()), "closed") {
					ps.logger.Error("Peer server accept error: %v", err)
				}
			}
			continue
		}

		ps.logger.Debug("Incoming peer connection from %s", conn.RemoteAddr())
		go ps.handlePeerConnection(conn, false)
	}
}

// handlePeerConnection handles a single peer connection
func (ps *PeerServer) handlePeerConnection(conn net.Conn, isOutbound bool) {
	defer conn.Close()

	peerAddr := conn.RemoteAddr().String()
	if isOutbound {
		peerAddr = conn.RemoteAddr().String()
	}
	ps.mu.Lock()
	ps.peers[peerAddr] = &PeerConnection{
		address:    peerAddr,
		conn:       conn,
		isOutbound: isOutbound,
		lastSeen:   time.Now(),
	}
	ps.mu.Unlock()

	ps.logger.Debug("Peer connection established: %s (outbound: %v)", peerAddr, isOutbound)

	// Read messages from peer
	decoder := json.NewDecoder(conn)
	for {
		var msg ReplicationMessage
		err := decoder.Decode(&msg)
		if err != nil {
			if err != io.EOF {
				ps.logger.Debug("Peer connection decode error from %s: %v", peerAddr, err)
			}
			ps.logger.Debug("Peer connection closed: %s", peerAddr)
			break
		}

		ps.logger.Debug("Received message from %s: type=%s", peerAddr, msg.Type)

		switch msg.Type {
		case "write":
			if msg.WriteOp != nil {
				ps.handleRemoteWrite(msg.WriteOp, peerAddr, conn)
			}
		case "ack":
			ackPeer := msg.FromPeer
			if ackPeer == "" {
				ackPeer = peerAddr
			}
			ps.clusterManager.MarkCommitted(msg.MessageID, ackPeer)
		case "heartbeat":
			// Just update last seen time
			ps.mu.Lock()
			if peer, exists := ps.peers[peerAddr]; exists {
				peer.lastSeen = time.Now()
			}
			ps.mu.Unlock()
		case "sync_request":
			// Peer is requesting our current state
			ps.handleSyncRequest(peerAddr, conn)
		case "sync_response":
			// We received full state from peer
			ps.handleSyncResponse(&msg)
		}
	}

	// Clean up
	ps.mu.Lock()
	delete(ps.peers, peerAddr)
	ps.mu.Unlock()
}

// manageOutboundConnections manages outbound connections to remote peers
func (ps *PeerServer) manageOutboundConnections() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for ps.isRunning {
		<-ticker.C

		remotePeers := ps.clusterManager.GetRemotePeers()
		for _, peer := range remotePeers {
			ps.mu.RLock()
			_, exists := ps.peers[peer.Address]
			// Inbound connections are keyed by ephemeral port (e.g. "host:54321"),
			// not the configured port ("host:8001"). Check by host so we don't
			// open a redundant outbound connection — which would send a sync_request
			// back to a peer that just reconnected and is still empty, causing the
			// data-holding node to wipe itself.
			if !exists {
				peerHost, _, _ := net.SplitHostPort(peer.Address)
				for addr := range ps.peers {
					if connHost, _, err := net.SplitHostPort(addr); err == nil && connHost == peerHost {
						exists = true
						break
					}
				}
			}
			ps.mu.RUnlock()

			if !exists {
				// Try to establish outbound connection
				go ps.connectToPeer(peer.Address)
			}
		}
	}
}

// connectToPeer establishes an outbound connection to a remote peer
func (ps *PeerServer) connectToPeer(peerAddr string) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		ps.logger.Debug("Failed to connect to peer %s: %v", peerAddr, err)
		return
	}

	ps.logger.Debug("Outbound connection to peer %s established", peerAddr)
	ps.mu.Lock()
	ps.peers[peerAddr] = &PeerConnection{
		address:    peerAddr,
		conn:       conn,
		isOutbound: true,
		lastSeen:   time.Now(),
	}
	ps.mu.Unlock()

	// Send sync request to get current state from peer
	ps.sendSyncRequest(peerAddr, conn)

	ps.handlePeerConnection(conn, true)
}

// handleRemoteWrite processes a write operation from a remote peer
func (ps *PeerServer) handleRemoteWrite(writeOp *WriteOperation, sourcePeer string, conn net.Conn) {
	ps.logger.Audit("REMOTE_WRITE", fmt.Sprintf("db=%s table=%s op=%s source=%s", writeOp.Database, writeOp.Table, writeOp.Operation, sourcePeer))

	// Apply write to local database
	if ps.applyRemoteWrite != nil {
		if err := ps.applyRemoteWrite(writeOp); err != nil {
			ps.logger.Error("Failed applying remote write %s: %v", writeOp.ID, err)
			return
		}
	}
	if ps.db != nil {
		if err := ps.db.SaveRuntimeSnapshot(); err != nil {
			ps.logger.Warn("Failed saving runtime after remote write %s: %v", writeOp.ID, err)
		}
	}

	ps.logger.Info("Applied remote write: %s from %s", writeOp.ID, sourcePeer)

	// Send ACK to source peer
	ack := ReplicationMessage{
		Type:      "ack",
		MessageID: writeOp.ID,
		FromPeer: func() string {
			if ps.clusterManager != nil && ps.clusterManager.GetLocalPeer() != nil {
				return ps.clusterManager.GetLocalPeer().Address
			}
			return "unknown"
		}(),
		Timestamp: time.Now().Unix(),
	}
	if err := json.NewEncoder(conn).Encode(ack); err != nil {
		ps.logger.Warn("Failed to send ACK for write %s to %s: %v", writeOp.ID, sourcePeer, err)
	}
}

// ReplicateWrite sends a write operation to all remote peers
func (ps *PeerServer) ReplicateWrite(writeOp *WriteOperation) error {
	if writeOp == nil {
		return fmt.Errorf("cannot replicate nil write operation")
	}
	if ps.clusterManager == nil {
		return fmt.Errorf("cluster manager not initialized")
	}

	msg := ReplicationMessage{
		Type:      "write",
		WriteOp:   writeOp,
		MessageID: writeOp.ID,
		FromPeer: func() string {
			if ps.clusterManager != nil && ps.clusterManager.GetLocalPeer() != nil {
				return ps.clusterManager.GetLocalPeer().Address
			}
			return "unknown"
		}(),
		Timestamp: time.Now().Unix(),
	}

	remotePeers := ps.clusterManager.GetRemotePeers()

	if len(remotePeers) == 0 {
		// Single server - mark as committed locally
		ps.clusterManager.MarkCommitted(writeOp.ID, "local")
		return nil
	}

	// Send to all peers
	for _, rp := range remotePeers {
		addr := rp.Address
		ps.mu.RLock()
		peer, ok := ps.peers[addr]
		ps.mu.RUnlock()
		if !ok || peer == nil || peer.conn == nil {
			ps.logger.Warn("No active connection to peer %s", addr)
			ps.clusterManager.MarkFailed(writeOp.ID, addr)
			continue
		}

		go func(address string, peerConn *PeerConnection) {
			encoder := json.NewEncoder(peerConn.conn)
			err := encoder.Encode(msg)
			if err != nil {
				ps.logger.Warn("Failed to send write to peer %s: %v", address, err)
				ps.clusterManager.MarkFailed(writeOp.ID, address)
			} else {
				ps.logger.Debug("Write %s sent to peer %s", writeOp.ID, address)
			}
		}(addr, peer)
	}

	return nil
}

// ReplicateWriteAndWait replicates a write and waits for ACK from all peers
func (ps *PeerServer) ReplicateWriteAndWait(writeOp *WriteOperation, timeout time.Duration) error {
	if err := ps.ReplicateWrite(writeOp); err != nil {
		return err
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ps.clusterManager.IsFullyCommitted(writeOp.ID) {
			ps.clusterManager.PurgeCommittedEntries()
			return nil
		}

		// Fail-open if all configured remote peers are offline.
		// This prevents write APIs from timing out when a cluster peer is down.
		if !ps.hasAnyActiveRemoteConnection() {
			ps.logger.Warn("No active peer connections; proceeding with local commit for write %s", writeOp.ID)
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("timed out waiting for cluster commit for write %s - network connectivity issue or peer(s) offline", writeOp.ID)
}

// hasAnyActiveRemoteConnection checks if at least one configured remote peer currently has an active TCP connection.
func (ps *PeerServer) hasAnyActiveRemoteConnection() bool {
	if ps.clusterManager == nil {
		return false
	}

	remotePeers := ps.clusterManager.GetRemotePeers()
	if len(remotePeers) == 0 {
		return false
	}

	ps.mu.RLock()
	defer ps.mu.RUnlock()

	for _, rp := range remotePeers {
		// Direct key match (outbound connection style: host:port)
		if peer, ok := ps.peers[rp.Address]; ok && peer != nil && peer.conn != nil {
			return true
		}

		// Inbound connection fallback: keyed by ephemeral source port.
		rHost, _, rErr := net.SplitHostPort(rp.Address)
		if rErr != nil {
			continue
		}
		for addr, peer := range ps.peers {
			if peer == nil || peer.conn == nil {
				continue
			}
			host, _, err := net.SplitHostPort(addr)
			if err == nil && host == rHost {
				return true
			}
		}
	}

	return false
}

// handleReplication processes the write operation channel
func (ps *PeerServer) handleReplication() {
	for ps.isRunning {
		select {
		case writeOp := <-ps.writeOperationChan:
			if err := ps.ReplicateWrite(writeOp); err != nil {
				ps.logger.Error("Failed to replicate write: %v", err)
			}
		case <-time.After(1 * time.Second):
			// Periodic heartbeat to maintain connections
			ps.sendHeartbeats()
		}
	}
}

// sendHeartbeats sends heartbeat messages to all peers and updates metrics
func (ps *PeerServer) sendHeartbeats() {
	msg := ReplicationMessage{
		Type:      "heartbeat",
		MessageID: fmt.Sprintf("hb-%d", time.Now().UnixNano()),
		FromPeer: func() string {
			if ps.clusterManager != nil && ps.clusterManager.GetLocalPeer() != nil {
				return ps.clusterManager.GetLocalPeer().Address
			}
			return "unknown"
		}(),
		Timestamp: time.Now().Unix(),
	}

	ps.mu.RLock()
	peers := make(map[string]*PeerConnection)
	for addr, peer := range ps.peers {
		peers[addr] = peer
	}
	ps.mu.RUnlock()

	// Update metrics for each peer
	ps.statsLock.Lock()
	for addr, peer := range peers {
		encoder := json.NewEncoder(peer.conn)
		encoder.Encode(msg)
		ps.lastPingTime[addr] = time.Now()

		// Update peer metrics in database
		if ps.db != nil {
			writes := ps.writesReplicated[addr]
			failures := ps.replicationFailures[addr]
			lag := ps.replicationLag[addr]
			ps.db.UpdatePeerMetrics(addr, true, time.Now(), lag, writes, failures)
		}
	}
	ps.statsLock.Unlock()
}

// RecordWrite records a successful write replication to a peer
func (ps *PeerServer) RecordWrite(peerAddr string) {
	ps.statsLock.Lock()
	ps.writesReplicated[peerAddr]++
	ps.statsLock.Unlock()
}

// RecordFailure records a failed write replication to a peer
func (ps *PeerServer) RecordFailure(peerAddr string) {
	ps.statsLock.Lock()
	ps.replicationFailures[peerAddr]++
	ps.statsLock.Unlock()
}

// SetReplicationLag sets the replication lag for a peer in milliseconds
func (ps *PeerServer) SetReplicationLag(peerAddr string, lagMs int64) {
	ps.statsLock.Lock()
	ps.replicationLag[peerAddr] = lagMs
	ps.statsLock.Unlock()
}

// QueueWrite queues a write operation for replication
func (ps *PeerServer) QueueWrite(writeOp *WriteOperation) {
	select {
	case ps.writeOperationChan <- writeOp:
		ps.logger.Debug("Write operation queued for replication: %s", writeOp.ID)
	default:
		ps.logger.Warn("Replication queue full, dropping write: %s", writeOp.ID)
	}
}

// Close closes the peer server
func (ps *PeerServer) Close() {
	ps.isRunning = false
	if ps.listener != nil {
		ps.listener.Close()
	}
	if ps.writeOperationChan != nil {
		close(ps.writeOperationChan)
	}

	ps.mu.Lock()
	for addr, peer := range ps.peers {
		if peer != nil && peer.conn != nil {
			_ = peer.conn.Close()
		}
		delete(ps.peers, addr)
	}
	ps.mu.Unlock()

	ps.logger.Info("Peer server shutdown")
}

// GetPeerCount returns the number of connected peers
func (ps *PeerServer) GetPeerCount() int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return len(ps.peers)
}

// GetConnectedPeers returns list of connected peer addresses
func (ps *PeerServer) GetConnectedPeers() []string {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	peers := make([]string, 0, len(ps.peers))
	for addr := range ps.peers {
		peers = append(peers, addr)
	}
	return peers
}

// sendSyncRequest sends a sync request to a peer
func (ps *PeerServer) sendSyncRequest(peerAddr string, conn net.Conn) {
	msg := ReplicationMessage{
		Type:      "sync_request",
		MessageID: fmt.Sprintf("sync-req-%d", time.Now().UnixNano()),
		FromPeer: func() string {
			if ps.clusterManager != nil && ps.clusterManager.GetLocalPeer() != nil {
				return ps.clusterManager.GetLocalPeer().Address
			}
			return "unknown"
		}(),
		Timestamp: time.Now().Unix(),
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		ps.logger.Warn("Failed to send sync request to %s: %v", peerAddr, err)
	} else {
		ps.logger.Info("Sync request sent to peer %s", peerAddr)
	}
}

// handleSyncRequest responds with full database state
func (ps *PeerServer) handleSyncRequest(peerAddr string, conn net.Conn) {
	ps.logger.Info("Peer %s requested state sync", peerAddr)

	// Export current database
	data, err := ps.db.ExportToJSON()
	if err != nil {
		ps.logger.Error("Failed to export database for sync: %v", err)
		return
	}

	// Calculate checksum
	hash := fmt.Sprintf("%x", sha256.Sum256(data))

	msg := ReplicationMessage{
		Type:         "sync_response",
		MessageID:    fmt.Sprintf("sync-resp-%d", time.Now().UnixNano()),
		FromPeer:     ps.clusterManager.GetLocalPeer().Address,
		Timestamp:    time.Now().Unix(),
		SyncData:     data,
		SyncChecksum: hash,
	}

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(msg); err != nil {
		ps.logger.Error("Failed to send sync response to %s: %v", peerAddr, err)
	} else {
		ps.logger.Info("Sync response sent to peer %s (size: %d bytes, checksum: %s)", peerAddr, len(data), hash)
	}
}

// handleSyncResponse applies full database state from peer
func (ps *PeerServer) handleSyncResponse(msg *ReplicationMessage) {
	if len(msg.SyncData) == 0 {
		ps.logger.Warn("Received empty sync response")
		return
	}

	ps.logger.Info("Received state sync from peer %s (size: %d bytes, checksum: %s)", msg.FromPeer, len(msg.SyncData), msg.SyncChecksum)

	// Verify checksum
	hash := fmt.Sprintf("%x", sha256.Sum256(msg.SyncData))
	if hash != msg.SyncChecksum {
		ps.logger.Error("Sync checksum mismatch! Expected %s, got %s", msg.SyncChecksum, hash)
		return
	}

	// Guard against split-brain data loss: if our local database is larger than
	// the incoming snapshot, we have more data than the peer sending it. Refuse
	// the import so we don't overwrite a richer local state with a stale/empty
	// peer's snapshot. This is a safety net for the race where both nodes dial
	// outbound to each other on reconnect and both send a sync_request.
	if localData, err := ps.db.ExportToJSON(); err == nil && len(localData) > len(msg.SyncData) {
		ps.logger.Warn("Ignoring sync response from %s: local data (%d bytes) > received data (%d bytes) — local state is richer, preserving",
			msg.FromPeer, len(localData), len(msg.SyncData))
		return
	}

	// Import the synced database (overwrites local state)
	if err := ps.db.ImportFromJSON(msg.SyncData); err != nil {
		ps.logger.Error("Failed to import synced state: %v", err)
		return
	}

	// Save to disk immediately
	if err := ps.db.SaveRuntimeSnapshot(); err != nil {
		ps.logger.Error("Failed to save synced state to disk: %v", err)
		return
	}

	ps.logger.Info("Successfully synced state from peer %s", msg.FromPeer)
}

// watchWriteJournal watches the database write journal and replicates writes to peers
func (ps *PeerServer) watchWriteJournal() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for ps.isRunning {
		<-ticker.C

		// Get all unreplicated writes from the journal
		writes := ps.db.GetUnreplicatedWrites()
		if len(writes) == 0 {
			continue
		}

		// Convert journal entries to write operations and replicate
		for _, journalEntry := range writes {
			writeOp := &WriteOperation{
				ID:        journalEntry.ID,
				Operation: journalEntry.Operation,
				Database:  journalEntry.Database,
				Table:     journalEntry.Table,
				Data:      journalEntry.Data,
				Where:     journalEntry.Where,
				IsPrivate: journalEntry.IsPrivate,
				Timestamp: journalEntry.Timestamp,
				SourcePeer: func() string {
					if ps.clusterManager != nil && ps.clusterManager.GetLocalPeer() != nil {
						return ps.clusterManager.GetLocalPeer().Address
					}
					return "local"
				}(),
			}

			// Send to all remote peers asynchronously
			if err := ps.ReplicateWrite(writeOp); err != nil {
				ps.logger.Warn("Failed to replicate write %s: %v", writeOp.ID, err)
			} else {
				// Mark as replicated in journal
				ps.db.MarkWriteReplicated(journalEntry.ID)
			}
		}

		// Clear old journal entries periodically
		ps.db.ClearOldJournalEntries()
	}
}
