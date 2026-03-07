package modules

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
)

// ShardManager handles table sharding across multiple nodes
type ShardManager struct {
	mu            sync.RWMutex
	shards        map[string]*ShardConfig // table name -> shard config
	logger        *Logger
	localShardIDs []int // Which shards this node is responsible for
}

// ShardConfig defines how a table is sharded
type ShardConfig struct {
	TableName     string   `json:"table_name"`
	ShardKey      string   `json:"shard_key"`      // Field to shard on (e.g., "user_id", "id")
	NumShards     int      `json:"num_shards"`     // Total number of shards
	ShardNodes    []string `json:"shard_nodes"`    // Node addresses responsible for each shard
	ReplicaFactor int      `json:"replica_factor"` // Number of replicas per shard
	Enabled       bool     `json:"enabled"`
}

// NewShardManager creates a new shard manager
func NewShardManager(logger *Logger) *ShardManager {
	if logger == nil {
		logger = GlobalLogger
	}

	return &ShardManager{
		shards:        make(map[string]*ShardConfig),
		logger:        logger,
		localShardIDs: []int{},
	}
}

// ConfigureShard configures sharding for a table
func (sm *ShardManager) ConfigureShard(tableName, shardKey string, numShards int, nodes []string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if numShards <= 0 {
		return fmt.Errorf("numShards must be > 0")
	}

	if shardKey == "" {
		return fmt.Errorf("shardKey cannot be empty")
	}

	sm.shards[tableName] = &ShardConfig{
		TableName:     tableName,
		ShardKey:      shardKey,
		NumShards:     numShards,
		ShardNodes:    nodes,
		ReplicaFactor: 1, // Default: no replication
		Enabled:       true,
	}

	sm.logger.Info("Configured sharding for table %s: %d shards on key %s", tableName, numShards, shardKey)

	return nil
}

// GetShardID calculates which shard a row belongs to
func (sm *ShardManager) GetShardID(tableName string, row map[string]interface{}) (int, error) {
	sm.mu.RLock()
	config, exists := sm.shards[tableName]
	sm.mu.RUnlock()

	if !exists || !config.Enabled {
		return -1, nil // Not sharded
	}

	// Get shard key value
	keyValue, exists := row[config.ShardKey]
	if !exists {
		return -1, fmt.Errorf("shard key %s not found in row", config.ShardKey)
	}

	// Convert to string for hashing
	var keyStr string
	switch v := keyValue.(type) {
	case string:
		keyStr = v
	case int, int64, float64:
		keyStr = fmt.Sprintf("%v", v)
	default:
		// Use JSON marshaling for complex types
		b, _ := json.Marshal(v)
		keyStr = string(b)
	}

	// Hash the key and mod by number of shards
	hash := sha256.Sum256([]byte(keyStr))
	shardID := int(hash[0])%config.NumShards + int(hash[1])%config.NumShards
	shardID = shardID % config.NumShards

	return shardID, nil
}

// GetShardNode returns the node address responsible for a shard
func (sm *ShardManager) GetShardNode(tableName string, shardID int) (string, error) {
	sm.mu.RLock()
	config, exists := sm.shards[tableName]
	sm.mu.RUnlock()

	if !exists || !config.Enabled {
		return "", fmt.Errorf("table %s is not sharded", tableName)
	}

	if shardID < 0 || shardID >= config.NumShards {
		return "", fmt.Errorf("invalid shard ID %d for table %s", shardID, tableName)
	}

	if len(config.ShardNodes) == 0 {
		return "", fmt.Errorf("no shard nodes configured")
	}

	// Simple round-robin assignment
	nodeIndex := shardID % len(config.ShardNodes)
	return config.ShardNodes[nodeIndex], nil
}

// IsLocalShard checks if this node is responsible for a shard
func (sm *ShardManager) IsLocalShard(shardID int) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	for _, localID := range sm.localShardIDs {
		if localID == shardID {
			return true
		}
	}
	return false
}

// SetLocalShards sets which shards this node is responsible for
func (sm *ShardManager) SetLocalShards(shardIDs []int) {
	sm.mu.Lock()
	sm.localShardIDs = shardIDs
	sm.mu.Unlock()

	sm.logger.Info("Local node responsible for shards: %v", shardIDs)
}

// GetShardConfig returns the shard configuration for a table
func (sm *ShardManager) GetShardConfig(tableName string) (*ShardConfig, bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	config, exists := sm.shards[tableName]
	return config, exists
}

// IsSharded checks if a table is configured for sharding
func (sm *ShardManager) IsSharded(tableName string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	config, exists := sm.shards[tableName]
	return exists && config.Enabled
}

// DisableSharding disables sharding for a table
func (sm *ShardManager) DisableSharding(tableName string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if config, exists := sm.shards[tableName]; exists {
		config.Enabled = false
		sm.logger.Info("Disabled sharding for table %s", tableName)
	}
}

// EnableSharding enables sharding for a table
func (sm *ShardManager) EnableSharding(tableName string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	config, exists := sm.shards[tableName]
	if !exists {
		return fmt.Errorf("table %s has no shard configuration", tableName)
	}

	config.Enabled = true
	sm.logger.Info("Enabled sharding for table %s", tableName)
	return nil
}

// GetAllShardConfigs returns all shard configurations
func (sm *ShardManager) GetAllShardConfigs() map[string]*ShardConfig {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	configs := make(map[string]*ShardConfig)
	for k, v := range sm.shards {
		configCopy := *v
		configs[k] = &configCopy
	}
	return configs
}
