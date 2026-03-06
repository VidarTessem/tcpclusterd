package modules

import (
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"
)

// PeerInfo represents a cluster peer
type PeerInfo struct {
	Address string // "host:port"
	IsLocal bool   // true if this is the current server
}

// ClusterManager handles cluster peer communication
type ClusterManager struct {
	mu            sync.RWMutex
	peers         []PeerInfo
	localPeer     *PeerInfo
	changeJournal *ChangeJournal
	logger        *Logger
}

// ChangeJournal tracks pending writes that need to be replicated
type ChangeJournal struct {
	mu      sync.Mutex
	entries []JournalEntry
}

// JournalEntry represents a pending write operation
type JournalEntry struct {
	ID             string // unique ID
	Database       string // database name
	Table          string // table name
	Operation      string // "insert", "update", "delete"
	Data           map[string]interface{}
	Timestamp      time.Time
	CommittedPeers map[string]bool // peers that have acknowledged
	Failed         bool
	RetryCount     int
	LastRetryTime  time.Time
}

// NewClusterManager creates a cluster manager from comma-separated peer addresses
func NewClusterManager(clusterPeersStr string, logger *Logger) (*ClusterManager, error) {
	if logger == nil {
		logger = GlobalLogger
	}

	cm := &ClusterManager{
		peers:         []PeerInfo{},
		changeJournal: &ChangeJournal{entries: []JournalEntry{}},
		logger:        logger,
	}

	// Parse peers from comma-separated string
	peerAddrs := strings.Split(clusterPeersStr, ",")
	for _, addr := range peerAddrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		cm.peers = append(cm.peers, PeerInfo{Address: addr, IsLocal: false})
	}

	// Identify local peer
	localPeer, err := cm.identifyLocalPeer()
	if err != nil {
		logger.Error("Failed to identify local peer: %v", err)
		return nil, err
	}

	cm.localPeer = localPeer
	logger.Info("Cluster manager initialized. Local peer: %s. Total peers: %d", localPeer.Address, len(cm.peers))

	return cm, nil
}

// identifyLocalPeer tries to bind to each peer address to find which one is local
func (cm *ClusterManager) identifyLocalPeer() (*PeerInfo, error) {
	for i := range cm.peers {
		// Determine if IPv6 or IPv4
		addr := cm.peers[i].Address
		network := "tcp"
		if strings.Contains(addr, ":") && strings.HasPrefix(addr, "[") {
			// IPv6 address format: [::1]:5000
			network = "tcp6"
		}

		// Try to bind to this address
		listener, err := net.Listen(network, addr)
		if err == nil {
			// Successfully bound - this is the local peer
			listener.Close()
			cm.peers[i].IsLocal = true
			cm.logger.Info("Identified local peer: %s", cm.peers[i].Address)
			return &cm.peers[i], nil
		}

		cm.logger.Debug("Could not bind to %s (%s): %v", addr, network, err)
	}

	return nil, fmt.Errorf("could not identify local peer - unable to bind to any cluster address")
}

// GetLocalPeer returns the local peer info
func (cm *ClusterManager) GetLocalPeer() *PeerInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	if cm.localPeer != nil {
		return &PeerInfo{Address: cm.localPeer.Address, IsLocal: cm.localPeer.IsLocal}
	}
	return nil
}

// GetRemotePeers returns all remote peers
func (cm *ClusterManager) GetRemotePeers() []PeerInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	var remote []PeerInfo
	for _, p := range cm.peers {
		if !p.IsLocal {
			remote = append(remote, p)
		}
	}
	return remote
}

// GetPeers returns all peers (including local)
func (cm *ClusterManager) GetPeers() []PeerInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return append([]PeerInfo(nil), cm.peers...)
}

// GetAllPeers returns all peers
func (cm *ClusterManager) GetAllPeers() []PeerInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	peers := make([]PeerInfo, len(cm.peers))
	copy(peers, cm.peers)
	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Address < peers[j].Address
	})
	return peers
}

// LogWrite records a database write to the change journal
func (cm *ClusterManager) LogWrite(database, table, operation string, data map[string]interface{}) string {
	// Generate unique ID
	id := fmt.Sprintf("%s-%d-%d", operation, time.Now().UnixNano(), len(cm.changeJournal.entries))
	cm.TrackWrite(id, database, table, operation, data)
	return id
}

// TrackWrite records a write with a caller-provided ID.
func (cm *ClusterManager) TrackWrite(id, database, table, operation string, data map[string]interface{}) {
	cm.changeJournal.mu.Lock()
	defer cm.changeJournal.mu.Unlock()

	entry := JournalEntry{
		ID:             id,
		Database:       database,
		Table:          table,
		Operation:      operation,
		Data:           data,
		Timestamp:      time.Now(),
		CommittedPeers: make(map[string]bool),
		Failed:         false,
		RetryCount:     0,
	}

	// Log to audit
	details := fmt.Sprintf("db=%s table=%s op=%s data_keys=%v", database, table, operation, getMapKeys(data))
	cm.logger.Audit("WRITE", details)

	cm.changeJournal.entries = append(cm.changeJournal.entries, entry)
}

// MarkCommitted marks a write as committed on a peer
func (cm *ClusterManager) MarkCommitted(journalID, peerAddr string) {
	cm.changeJournal.mu.Lock()
	defer cm.changeJournal.mu.Unlock()

	for i, entry := range cm.changeJournal.entries {
		if entry.ID == journalID {
			cm.changeJournal.entries[i].CommittedPeers[peerAddr] = true
			cm.logger.Debug("Journal entry %s committed on peer %s", journalID, peerAddr)
			break
		}
	}
}

// MarkFailed marks a write as failed on a peer
func (cm *ClusterManager) MarkFailed(journalID, peerAddr string) {
	cm.changeJournal.mu.Lock()
	defer cm.changeJournal.mu.Unlock()

	for i, entry := range cm.changeJournal.entries {
		if entry.ID == journalID {
			cm.changeJournal.entries[i].Failed = true
			cm.changeJournal.entries[i].RetryCount++
			cm.changeJournal.entries[i].LastRetryTime = time.Now()
			cm.logger.Warn("Journal entry %s failed on peer %s (retry count: %d)", journalID, peerAddr, cm.changeJournal.entries[i].RetryCount)
			break
		}
	}
}

// GetFailedEntries returns all failed entries that need retry
func (cm *ClusterManager) GetFailedEntries() []JournalEntry {
	cm.changeJournal.mu.Lock()
	defer cm.changeJournal.mu.Unlock()

	var failed []JournalEntry
	now := time.Now()

	for _, entry := range cm.changeJournal.entries {
		if entry.Failed && now.Sub(entry.LastRetryTime) >= time.Minute {
			failed = append(failed, entry)
		}
	}

	return failed
}

// IsFullyCommitted checks if a write has been committed on all peers
func (cm *ClusterManager) IsFullyCommitted(journalID string) bool {
	cm.changeJournal.mu.Lock()
	defer cm.changeJournal.mu.Unlock()

	remotePeers := cm.getRemotePeersLocked()
	if len(remotePeers) == 0 {
		// Single server - always committed
		return true
	}

	for _, entry := range cm.changeJournal.entries {
		if entry.ID == journalID {
			for _, peer := range remotePeers {
				if !entry.CommittedPeers[peer.Address] {
					return false
				}
			}
			return true
		}
	}

	return false
}

// PurgeCommittedEntries removes entries that have been fully committed
func (cm *ClusterManager) PurgeCommittedEntries() {
	cm.changeJournal.mu.Lock()
	defer cm.changeJournal.mu.Unlock()

	remotePeers := cm.getRemotePeersLocked()
	var remaining []JournalEntry

	for _, entry := range cm.changeJournal.entries {
		// Keep if not fully committed on all peers
		fullyCommitted := true
		if len(remotePeers) > 0 {
			for _, peer := range remotePeers {
				if !entry.CommittedPeers[peer.Address] {
					fullyCommitted = false
					break
				}
			}
		}

		if !fullyCommitted {
			remaining = append(remaining, entry)
		} else {
			cm.logger.Debug("Purging committed journal entry: %s", entry.ID)
		}
	}

	cm.changeJournal.entries = remaining
}

// getRemotePeersLocked returns remote peers (called with mutex held)
func (cm *ClusterManager) getRemotePeersLocked() []PeerInfo {
	var remote []PeerInfo
	for _, p := range cm.peers {
		if !p.IsLocal {
			remote = append(remote, p)
		}
	}
	return remote
}

// Close closes the cluster manager
func (cm *ClusterManager) Close() {
	cm.logger.Info("Cluster manager shutdown")
}

// Helper function to extract keys from map
func getMapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
