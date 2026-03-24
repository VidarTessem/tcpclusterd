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
	mu        sync.RWMutex
	peers     []PeerInfo
	localPeer *PeerInfo
	logger    *Logger
}

// JournalEntry represents a pending write operation
type JournalEntry struct {
	ID             string                 `json:"id"`
	Database       string                 `json:"database"`
	Table          string                 `json:"table"`
	Operation      string                 `json:"operation"`
	Data           map[string]interface{} `json:"data,omitempty"`
	Timestamp      time.Time              `json:"timestamp"`
	CommittedPeers map[string]bool        `json:"committed_peers,omitempty"`
	Failed         bool                   `json:"failed"`
	RetryCount     int                    `json:"retry_count"`
	LastRetryTime  time.Time              `json:"last_retry_time"`
}

// NewClusterManager creates a cluster manager from comma-separated peer addresses
func NewClusterManager(clusterPeersStr string, logger *Logger) (*ClusterManager, error) {
	if logger == nil {
		logger = GlobalLogger
	}

	cm := &ClusterManager{
		peers:  []PeerInfo{},
		logger: logger,
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
	id := fmt.Sprintf("%s-%d", operation, time.Now().UnixNano())
	cm.TrackWrite(id, database, table, operation, data)
	return id
}

// TrackWrite records a write with a caller-provided ID.
func (cm *ClusterManager) TrackWrite(id, database, table, operation string, data map[string]interface{}) {
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

	if err := appendJournalEnvelope(journalEnvelope{Kind: "cluster", Cluster: &entry}); err != nil && cm.logger != nil {
		cm.logger.Warn("Failed to append cluster journal entry %s: %v", id, err)
	}
}

// MarkCommitted marks a write as committed on a peer
func (cm *ClusterManager) MarkCommitted(journalID, peerAddr string) {
	_ = updateJournalEnvelopes(func(records []journalEnvelope) ([]journalEnvelope, error) {
		for i, record := range records {
			if record.Kind != "cluster" || record.Cluster == nil || record.Cluster.ID != journalID {
				continue
			}
			if record.Cluster.CommittedPeers == nil {
				record.Cluster.CommittedPeers = make(map[string]bool)
			}
			record.Cluster.CommittedPeers[peerAddr] = true
			records[i] = record
			if cm.logger != nil {
				cm.logger.Debug("Journal entry %s committed on peer %s", journalID, peerAddr)
			}
			break
		}
		return records, nil
	})
}

// MarkFailed marks a write as failed on a peer
func (cm *ClusterManager) MarkFailed(journalID, peerAddr string) {
	_ = updateJournalEnvelopes(func(records []journalEnvelope) ([]journalEnvelope, error) {
		for i, record := range records {
			if record.Kind != "cluster" || record.Cluster == nil || record.Cluster.ID != journalID {
				continue
			}
			record.Cluster.Failed = true
			record.Cluster.RetryCount++
			record.Cluster.LastRetryTime = time.Now()
			records[i] = record
			if cm.logger != nil {
				cm.logger.Warn("Journal entry %s failed on peer %s (retry count: %d)", journalID, peerAddr, record.Cluster.RetryCount)
			}
			break
		}
		return records, nil
	})
}

// GetFailedEntries returns all failed entries that need retry
func (cm *ClusterManager) GetFailedEntries() []JournalEntry {
	var failed []JournalEntry
	now := time.Now()
	records, err := readJournalEnvelopes()
	if err != nil {
		if cm.logger != nil {
			cm.logger.Warn("Failed to read cluster journal: %v", err)
		}
		return failed
	}
	for _, record := range records {
		if record.Kind == "cluster" && record.Cluster != nil && record.Cluster.Failed && now.Sub(record.Cluster.LastRetryTime) >= time.Minute {
			failed = append(failed, *record.Cluster)
		}
	}

	return failed
}

// IsFullyCommitted checks if a write has been committed on all peers
func (cm *ClusterManager) IsFullyCommitted(journalID string) bool {
	remotePeers := cm.getRemotePeersLocked()
	if len(remotePeers) == 0 {
		// Single server - always committed
		return true
	}
	records, err := readJournalEnvelopes()
	if err != nil {
		if cm.logger != nil {
			cm.logger.Warn("Failed to read cluster journal: %v", err)
		}
		return false
	}
	for _, record := range records {
		if record.Kind == "cluster" && record.Cluster != nil && record.Cluster.ID == journalID {
			for _, peer := range remotePeers {
				if !record.Cluster.CommittedPeers[peer.Address] {
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
	remotePeers := cm.getRemotePeersLocked()
	_ = updateJournalEnvelopes(func(records []journalEnvelope) ([]journalEnvelope, error) {
		remaining := make([]journalEnvelope, 0, len(records))
		for _, record := range records {
			if record.Kind != "cluster" || record.Cluster == nil {
				remaining = append(remaining, record)
				continue
			}
			fullyCommitted := true
			if len(remotePeers) > 0 {
				for _, peer := range remotePeers {
					if !record.Cluster.CommittedPeers[peer.Address] {
						fullyCommitted = false
						break
					}
				}
			}
			if fullyCommitted {
				if cm.logger != nil {
					cm.logger.Debug("Purging committed journal entry: %s", record.Cluster.ID)
				}
				continue
			}
			remaining = append(remaining, record)
		}
		return remaining, nil
	})
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
