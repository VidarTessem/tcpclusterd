package modules

import (
	"fmt"
	"sync"
	"time"
)

// ServiceConfig represents configuration for a specific service
type ServiceConfig struct {
	Enabled   bool     `json:"enabled"`
	Host      string   `json:"host"`
	Port      int      `json:"port"`
	Whitelist []string `json:"whitelist,omitempty"` // IP whitelist, empty means allow all
	TLS       bool     `json:"tls,omitempty"`       // For HTTP/HTTPS
	CertPath  string   `json:"cert_path,omitempty"`
	KeyPath   string   `json:"key_path,omitempty"`
}

// WebSocketConfig extends ServiceConfig with WebSocket-specific settings
type WebSocketConfig struct {
	ServiceConfig
	Mode             int `json:"mode"`              // 1 = onChange, 2 = interval
	IntervalSeconds  int `json:"interval_seconds"`  // For mode 2
	PingIntervalSec  int `json:"ping_interval_sec"` // Default 3
	ReconnectMaxWait int `json:"reconnect_max_wait"`
}

// ServiceManager manages service configuration and state
type ServiceManager struct {
	db         *Database
	mu         sync.RWMutex
	serverID   string
	httpConfig *ServiceConfig
	tcpConfig  *ServiceConfig
	wsConfig   *WebSocketConfig
}

// NewServiceManager creates a new service manager
func NewServiceManager(db *Database, serverID string) *ServiceManager {
	return &ServiceManager{
		db:       db,
		serverID: serverID,
	}
}

// LoadConfigFromDatabase loads service configuration from the system database
func (sm *ServiceManager) LoadConfigFromDatabase() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Load HTTP config - find first entry with enabled field
	httpData, err := sm.db.SelectRows("system", "services_config", map[string]string{"service": "http"}, true)
	sm.httpConfig = nil
	if err == nil && httpData != nil {
		if rows, ok := httpData.([]interface{}); ok && len(rows) > 0 {
			for _, r := range rows {
				if row, ok := r.(map[string]interface{}); ok {
					// Only use if it has at least enabled field
					if _, hasEnabled := row["enabled"]; hasEnabled {
						sm.httpConfig = parseServiceConfig(row)
						break
					}
				}
			}
		}
	}

	// Load TCP config - find first entry with enabled field
	tcpData, err := sm.db.SelectRows("system", "services_config", map[string]string{"service": "tcp"}, true)
	sm.tcpConfig = nil
	if err == nil && tcpData != nil {
		if rows, ok := tcpData.([]interface{}); ok && len(rows) > 0 {
			for _, r := range rows {
				if row, ok := r.(map[string]interface{}); ok {
					// Only use if it has at least enabled field
					if _, hasEnabled := row["enabled"]; hasEnabled {
						sm.tcpConfig = parseServiceConfig(row)
						break
					}
				}
			}
		}
	}

	// Load WebSocket config - find first entry with enabled field
	wsData, err := sm.db.SelectRows("system", "services_config", map[string]string{"service": "websocket"}, true)
	sm.wsConfig = nil
	if err == nil && wsData != nil {
		if rows, ok := wsData.([]interface{}); ok && len(rows) > 0 {
			for _, r := range rows {
				if row, ok := r.(map[string]interface{}); ok {
					// Only use if it has at least enabled field
					if _, hasEnabled := row["enabled"]; hasEnabled {
						sm.wsConfig = parseWebSocketConfig(row)
						break
					}
				}
			}
		}
	}

	return nil
}

// GetHTTPConfig returns the HTTP service configuration
func (sm *ServiceManager) GetHTTPConfig() *ServiceConfig {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.httpConfig
}

// GetTCPConfig returns the TCP service configuration
func (sm *ServiceManager) GetTCPConfig() *ServiceConfig {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.tcpConfig
}

// GetWebSocketConfig returns the WebSocket service configuration
func (sm *ServiceManager) GetWebSocketConfig() *WebSocketConfig {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.wsConfig
}

// parseServiceConfig parses a database row into ServiceConfig
func parseServiceConfig(row map[string]interface{}) *ServiceConfig {
	config := &ServiceConfig{}

	if enabled, ok := row["enabled"].(bool); ok {
		config.Enabled = enabled
	} else if enabledStr, ok := row["enabled"].(string); ok {
		config.Enabled = enabledStr == "true" || enabledStr == "1"
	}

	if host, ok := row["host"].(string); ok {
		config.Host = host
	}

	if port, ok := row["port"].(float64); ok {
		config.Port = int(port)
	} else if port, ok := row["port"].(int); ok {
		config.Port = port
	}

	if tls, ok := row["tls"].(bool); ok {
		config.TLS = tls
	}

	if cert, ok := row["cert_path"].(string); ok {
		config.CertPath = cert
	}

	if key, ok := row["key_path"].(string); ok {
		config.KeyPath = key
	}

	// Parse whitelist if present
	if wl, ok := row["whitelist"].(string); ok && wl != "" {
		// Assume comma-separated or JSON array format
		// For now, just store the string; caller can parse it
		config.Whitelist = []string{wl}
	}

	return config
}

// parseWebSocketConfig parses a database row into WebSocketConfig
func parseWebSocketConfig(row map[string]interface{}) *WebSocketConfig {
	config := &WebSocketConfig{
		ServiceConfig:    *parseServiceConfig(row),
		Mode:             1,  // Default to onChange
		IntervalSeconds:  60, // Default to 60 seconds
		PingIntervalSec:  3,  // Default to 3 seconds
		ReconnectMaxWait: 30, // Default to 30 seconds
	}

	if mode, ok := row["mode"].(float64); ok {
		config.Mode = int(mode)
	} else if mode, ok := row["mode"].(int); ok {
		config.Mode = mode
	}

	if interval, ok := row["interval_seconds"].(float64); ok {
		config.IntervalSeconds = int(interval)
	} else if interval, ok := row["interval_seconds"].(int); ok {
		config.IntervalSeconds = interval
	}

	if ping, ok := row["ping_interval_sec"].(float64); ok {
		config.PingIntervalSec = int(ping)
	} else if ping, ok := row["ping_interval_sec"].(int); ok {
		config.PingIntervalSec = ping
	}

	if reconnect, ok := row["reconnect_max_wait"].(float64); ok {
		config.ReconnectMaxWait = int(reconnect)
	} else if reconnect, ok := row["reconnect_max_wait"].(int); ok {
		config.ReconnectMaxWait = reconnect
	}

	return config
}

// InitializeDatabaseSchema creates the necessary tables if they don't exist
func InitializeDatabaseSchema(db *Database) error {
	// Create services_config table
	_, err := db.InsertRow("system", "services_config", map[string]interface{}{
		"service":    "http",
		"enabled":    true,
		"host":       "0.0.0.0",
		"port":       9090,
		"tls":        false,
		"created_at": time.Now().Unix(),
	}, true)
	if err != nil {
		// Table might already exist, that's okay
		fmt.Printf("Note: services_config table initialization: %v\n", err)
	}

	return nil
}
