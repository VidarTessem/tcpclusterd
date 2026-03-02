package modules

import (
	"bufio"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	peerHeartbeatInterval = 1 * time.Second
	peerAliveTTL          = 3 * time.Second
	peerHeartbeatTimeout  = 1200 * time.Millisecond
)

// WebSocketUpdate represents an update pushed to WebSocket clients
type WebSocketUpdate struct {
	ArrayName string            `json:"array_name"`
	Data      map[string]string `json:"data"`
	Timestamp time.Time         `json:"timestamp"`
}

// Cluster represents a distributed cluster array instance
type Cluster struct {
	data               map[string]map[string]string
	privateData        map[string]map[string]string // Private arrays requiring authentication
	timestamps         map[string]time.Time
	mu                 sync.RWMutex
	port               string
	listenAddr         string
	peers              []string
	lastSyncTime       time.Time
	syncCount          int64
	failedSyncs        int64
	totalSyncBytes     int64
	errorCount         int64 // Total error count across all operations
	runtimePath        string
	broadcastChan      chan bool
	aesKey             []byte
	peerSecret         string
	httpWhitelistRead  []string // IP addresses allowed to read from HTTP
	httpWhitelistWrite []string // IP addresses allowed to write to HTTP
	clusterWhitelist   []string // IP addresses allowed to access peer sync port
	tcpWhitelist       []string // IP addresses allowed to access TCP port
	persistenceChan    chan persistenceTask
	tcpPort            string
	tcpListenAddr      string
	tcpKey             string
	TcpEnabled         bool
	WsEnabled          bool
	HttpEnabled        bool
	ClusterHideIP      bool                              // Hide actual peer IPs in responses, use node$i instead
	bootupTime         time.Time                         // Time when this cluster node started
	peerBootupTimes    map[string]time.Time              // Bootup times of peer nodes
	peerLastSeen       map[string]time.Time              // Last successful contact time per peer
	wsSubscriptions    map[string][]chan WebSocketUpdate // Array name -> list of subscriber channels
	wsSubMu            sync.RWMutex                      // Lock for wsSubscriptions
	adminUsername      string                            // Admin username for authentication
	adminPassword      string                            // Admin password for authentication
	tcpTimeout         time.Duration                     // TCP connection timeout
	nodeID             string                            // Cached node identifier for logging
}

type persistenceTask struct {
	arrayName string
	delete    bool
	isPrivate bool
}

var clusterArray *Cluster

func init() {
	clusterArray = &Cluster{
		data:            make(map[string]map[string]string),
		privateData:     make(map[string]map[string]string),
		timestamps:      make(map[string]time.Time),
		port:            "9000",
		listenAddr:      "[::]",
		peers:           []string{},
		runtimePath:     "",                   // Will be set by InitClusterArray or CreateRuntimeDirectory
		broadcastChan:   make(chan bool, 100), // Buffered channel
		persistenceChan: make(chan persistenceTask, 2048),
		bootupTime:      time.Now(),
		peerBootupTimes: make(map[string]time.Time),
		peerLastSeen:    make(map[string]time.Time),
		wsSubscriptions: make(map[string][]chan WebSocketUpdate),
		tcpTimeout:      30 * time.Second, // Default 30 second timeout
		TcpEnabled:      true,
	}

	// Start background broadcast worker
	go clusterArray.broadcastWorker()
	go clusterArray.persistenceWorker()
}

// getNodeID returns the node identifier for logging
func (c *Cluster) getNodeID() string {
	// Explicit override takes priority (1-based index).
	if idxStr := strings.TrimSpace(os.Getenv("CLUSTER_NODE_INDEX")); idxStr != "" {
		if idx, err := strconv.Atoi(idxStr); err == nil && idx >= 1 {
			return fmt.Sprintf("node-%d", idx)
		}
	}

	if c.nodeID != "" {
		return c.nodeID
	}

	// Fallback if not yet initialized.
	return "node-unknown"
}

func normalizeNodeAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return ""
	}

	// Try host:port first (works for bracketed IPv6 and hostnames with port).
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}

	// Strip brackets if present.
	addr = strings.TrimPrefix(strings.TrimSuffix(addr, "]"), "[")

	if ip := net.ParseIP(addr); ip != nil {
		return ip.String()
	}

	return strings.ToLower(addr)
}

func (c *Cluster) detectNodeID() string {
	localCandidates := map[string]struct{}{}

	// Include explicit listen address when it is a concrete IP.
	if n := normalizeNodeAddr(c.listenAddr); n != "" && n != "::" && n != "0.0.0.0" {
		localCandidates[n] = struct{}{}
	}

	// Include all interface IPs.
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, a := range addrs {
			switch v := a.(type) {
			case *net.IPNet:
				if v.IP != nil {
					localCandidates[v.IP.String()] = struct{}{}
				}
			case *net.IPAddr:
				if v.IP != nil {
					localCandidates[v.IP.String()] = struct{}{}
				}
			}
		}
	}

	// Match local IP/host against peers in configured order.
	for i, peer := range c.peers {
		normalizedPeer := normalizeNodeAddr(peer)
		if normalizedPeer == "" {
			continue
		}
		if _, ok := localCandidates[normalizedPeer]; ok {
			return fmt.Sprintf("node-%d", i+1)
		}
	}

	// If no local match, keep deterministic fallback for troubleshooting.
	return "node-unknown"
}

// CreateRuntimeDirectory creates a new runtime directory with current timestamp
func (c *Cluster) CreateRuntimeDirectory() error {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	runtimePath := filepath.Join("runtime", timestamp)

	if err := os.MkdirAll(runtimePath, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %v", err)
	}

	c.runtimePath = runtimePath
	log.Printf("[CLUSTER:%s] Runtime directory: %s\n", c.getNodeID(), runtimePath)
	return nil
}

// InitClusterArray initializes the cluster from environment variables
func InitClusterArray(env map[string]string, loadLastConfig bool) {
	// Initialize logging system
	if err := InitLogger(env); err != nil {
		log.Printf("[CLUSTER:%s] Failed to initialize logger: %v", clusterArray.getNodeID(), err)
	}

	// Reset slice-based configuration to avoid duplicates when re-initialized.
	clusterArray.peers = clusterArray.peers[:0]
	clusterArray.httpWhitelistRead = clusterArray.httpWhitelistRead[:0]
	clusterArray.httpWhitelistWrite = clusterArray.httpWhitelistWrite[:0]
	clusterArray.clusterWhitelist = clusterArray.clusterWhitelist[:0]

	if peersStr, ok := env["CLUSTER_PEERS"]; ok {
		peers := strings.Split(peersStr, ",")
		for _, peer := range peers {
			peer = strings.TrimSpace(peer)
			if peer != "" {
				// Normalize IPv6 addresses by stripping brackets for consistent key format
				peer = strings.TrimPrefix(strings.TrimSuffix(peer, "]"), "[")
				clusterArray.peers = append(clusterArray.peers, peer)
			}
		}
	}

	// Read cluster port and listen address from environment
	if port, ok := env["CLUSTER_PORT"]; ok && port != "" {
		clusterArray.port = port
	}
	if addr, ok := env["CLUSTER_LISTEN_ADDR"]; ok && addr != "" {
		clusterArray.listenAddr = addr
	}

	// Derive node identifier after peers/listen address are loaded.
	clusterArray.nodeID = clusterArray.detectNodeID()

	// Read encryption key and peer secret from environment
	if cipherKey, ok := env["CLUSTER_CIPHER_KEY"]; ok && cipherKey != "" {
		// Try to decode from base64 first, fall back to raw bytes
		var key []byte

		// Try base64 decoding
		decoded, err := base64.StdEncoding.DecodeString(cipherKey)
		if err == nil && len(decoded) == 32 {
			// Successfully decoded from base64 to 32 bytes
			key = decoded
		} else if len(cipherKey) == 32 {
			// Use raw 32 bytes
			key = []byte(cipherKey)
		} else {
			// Invalid key length
			log.Printf("[CLUSTER:%s] Warning: CLUSTER_CIPHER_KEY must be 32 bytes (or base64-encoded 32 bytes), got %d\n", clusterArray.getNodeID(), len(cipherKey))
			key = []byte(cipherKey) // Use as-is (will fail at encryption time with clear error)
		}

		clusterArray.aesKey = key
	}

	if peerSecret, ok := env["CLUSTER_PEER_SECRET"]; ok && peerSecret != "" {
		clusterArray.peerSecret = peerSecret
	}

	// Parse HTTP read whitelist IPs
	if httpWhitelist, ok := env["HTTP_WHITELIST_READ"]; ok && httpWhitelist != "" {
		for _, ip := range strings.Split(httpWhitelist, ",") {
			ip = strings.TrimSpace(ip)
			if ip != "" {
				clusterArray.httpWhitelistRead = append(clusterArray.httpWhitelistRead, ip)
			}
		}
	}

	// Parse HTTP write whitelist IPs
	if httpWhitelist, ok := env["HTTP_WHITELIST_WRITE"]; ok && httpWhitelist != "" {
		for _, ip := range strings.Split(httpWhitelist, ",") {
			ip = strings.TrimSpace(ip)
			if ip != "" {
				clusterArray.httpWhitelistWrite = append(clusterArray.httpWhitelistWrite, ip)
			}
		}
	}

	// Parse cluster peer whitelist IPs
	if clusterWhitelist, ok := env["CLUSTER_WHITELIST_IPS"]; ok && clusterWhitelist != "" {
		for _, ip := range strings.Split(clusterWhitelist, ",") {
			ip = strings.TrimSpace(ip)
			if ip != "" {
				clusterArray.clusterWhitelist = append(clusterArray.clusterWhitelist, ip)
			}
		}
	}

	// Parse TCP configuration
	if tcpPort, ok := env["TCP_PORT"]; ok && tcpPort != "" {
		clusterArray.tcpPort = tcpPort
	} else {
		clusterArray.tcpPort = "9001"
	}
	if tcpAddr, ok := env["TCP_LISTEN_ADDR"]; ok && tcpAddr != "" {
		clusterArray.tcpListenAddr = tcpAddr
	} else {
		clusterArray.tcpListenAddr = "0.0.0.0"
	}
	if tcpKey, ok := env["TCP_KEY"]; ok && tcpKey != "" {
		clusterArray.tcpKey = tcpKey
	}

	// Parse TCP enabled flag (default: true)
	clusterArray.TcpEnabled = true
	if tcpEnabled, ok := env["TCP_ENABLED"]; ok {
		tcpEnabled = strings.ToLower(strings.TrimSpace(tcpEnabled))
		if tcpEnabled == "false" || tcpEnabled == "0" || tcpEnabled == "no" || tcpEnabled == "off" {
			clusterArray.TcpEnabled = false
		}
	}

	// Parse TCP whitelist IPs
	if tcpWhitelist, ok := env["TCP_WHITELIST_IPS"]; ok && tcpWhitelist != "" {
		for _, ip := range strings.Split(tcpWhitelist, ",") {
			ip = strings.TrimSpace(ip)
			if ip != "" {
				clusterArray.tcpWhitelist = append(clusterArray.tcpWhitelist, ip)
			}
		}
	}

	// Load admin credentials for authentication
	if adminUsername, ok := env["ADMIN_USERNAME"]; ok && adminUsername != "" {
		clusterArray.adminUsername = adminUsername
	} else {
		clusterArray.adminUsername = "admin"
	}
	if adminPassword, ok := env["ADMIN_PASSWORD"]; ok && adminPassword != "" {
		clusterArray.adminPassword = adminPassword
	} else {
		clusterArray.adminPassword = "changeme"
	}

	// Load TCP timeout configuration (default: 30 seconds)
	clusterArray.tcpTimeout = 30 * time.Second
	if tcpTimeoutStr, ok := env["TCP_TIMEOUT"]; ok && tcpTimeoutStr != "" {
		if seconds, err := strconv.ParseInt(tcpTimeoutStr, 10, 64); err == nil && seconds > 0 {
			clusterArray.tcpTimeout = time.Duration(seconds) * time.Second
		}
	}

	// Parse WebSocket configuration
	if wsEnabled, ok := env["WS_ENABLED"]; ok && (wsEnabled == "true" || wsEnabled == "1") {
		clusterArray.WsEnabled = true
	}

	// Parse HTTP server configuration (default: enabled)
	clusterArray.HttpEnabled = true
	if httpEnabled, ok := env["HTTP_ENABLED"]; ok && (httpEnabled == "false" || httpEnabled == "0") {
		clusterArray.HttpEnabled = false
	}

	// Parse cluster hide IP configuration
	if hideIP, ok := env["CLUSTER_HIDE_IP"]; ok && (hideIP == "true" || hideIP == "1") {
		clusterArray.ClusterHideIP = true
	}

	// Start TCP server if configured and enabled
	if clusterArray.TcpEnabled && clusterArray.tcpKey != "" {
		go clusterArray.StartTCPServer(clusterArray.tcpPort, clusterArray.tcpListenAddr, clusterArray.tcpKey)
	} else if clusterArray.TcpEnabled && clusterArray.tcpKey == "" {
		log.Printf("[TCP] TCP enabled but TCP_KEY is empty; TCP server not started")
	}

	// Load from last config if requested
	if loadLastConfig {
		if err := clusterArray.loadFromLastConfig(); err != nil {
			log.Printf("[CLUSTER:%s] Error loading last config: %v\n", clusterArray.getNodeID(), err)
			IncrementErrorCount()
		}
	}

	// Start peer sync server
	go clusterArray.startPeerSyncServer(clusterArray.port)

	// Sync with peers on startup
	go clusterArray.syncFromPeersOnStartup()
}

// loadFromLastConfig finds and loads from the most recent runtime directory
func (c *Cluster) loadFromLastConfig() error {
	// Find all runtime directories
	entries, err := os.ReadDir("runtime")
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("no runtime directory found")
		}
		return fmt.Errorf("failed to read runtime directory: %v", err)
	}

	// Find the most recent directory (alphabetically last due to timestamp format)
	var latestDir string
	for i := len(entries) - 1; i >= 0; i-- {
		if entries[i].IsDir() {
			latestDir = entries[i].Name()
			break
		}
	}

	if latestDir == "" {
		return fmt.Errorf("no runtime directories found")
	}

	latestPath := filepath.Join("runtime", latestDir)
	c.runtimePath = latestPath
	// log.Printf("[CLUSTER:%s] Loading from last config: %s\n", c.getNodeID(), latestPath)

	// Read all JSON files from the directory
	files, err := os.ReadDir(latestPath)
	if err != nil {
		return fmt.Errorf("failed to read runtime directory: %v", err)
	}

	loadedCount := 0
	loadedPrivateCount := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".json") {
			continue
		}

		fileName := file.Name()
		isPrivate := strings.HasPrefix(fileName, "private_")
		arrayName := strings.TrimSuffix(fileName, ".json")
		if isPrivate {
			arrayName = strings.TrimPrefix(arrayName, "private_")
		}
		filePath := filepath.Join(latestPath, fileName)

		// Read and parse JSON file
		data, err := os.ReadFile(filePath)
		if err != nil {
			log.Printf("[CLUSTER:%s] Error reading file %s: %v\n", c.getNodeID(), filePath, err)
			continue
		}

		var payload struct {
			Data      map[string]string `json:"data"`
			Timestamp time.Time         `json:"timestamp"`
		}

		if err := json.Unmarshal(data, &payload); err != nil {
			log.Printf("[CLUSTER:%s] Error unmarshaling file %s: %v\n", c.getNodeID(), filePath, err)
			continue
		}

		// Load into cluster array
		c.mu.Lock()
		if isPrivate {
			c.privateData[arrayName] = payload.Data
			loadedPrivateCount++
		} else {
			c.data[arrayName] = payload.Data
			loadedCount++
		}
		c.timestamps[arrayName] = payload.Timestamp
		c.mu.Unlock()

		// log.Printf("[CLUSTER:%s] Loaded array: %s (%d keys)\n", c.getNodeID(), arrayName, len(payload.Data))
	}

	log.Printf("[CLUSTER:%s] Loaded %d public arrays and %d private arrays from %s\n", c.getNodeID(), loadedCount, loadedPrivateCount, latestPath)
	return nil
}

func (c *Cluster) queueSave(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: false, isPrivate: false}:
	default:
		log.Printf("[CLUSTER:%s] Persistence queue full, saving synchronously for array: %s\n", c.getNodeID(), arrayName)
		if err := c.saveArrayToDisk(arrayName); err != nil {
			log.Printf("[CLUSTER:%s] Error saving array to disk: %v\n", c.getNodeID(), err)
		}
	}
}

func (c *Cluster) queueSavePrivate(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: false, isPrivate: true}:
	default:
		log.Printf("[CLUSTER:%s] Persistence queue full, saving synchronously for private array: %s\n", c.getNodeID(), arrayName)
		if err := c.savePrivateArrayToDisk(arrayName); err != nil {
			log.Printf("[CLUSTER:%s] Error saving private array to disk: %v\n", c.getNodeID(), err)
		}
	}
}

func (c *Cluster) queueDelete(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: true, isPrivate: false}:
	default:
		log.Printf("[CLUSTER:%s] Persistence queue full, deleting synchronously for array: %s\n", c.getNodeID(), arrayName)
		if err := c.deleteArrayFromDisk(arrayName); err != nil {
			log.Printf("[CLUSTER:%s] Error deleting array file: %v\n", c.getNodeID(), err)
		}
	}
}

func (c *Cluster) queueDeletePrivate(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: true, isPrivate: true}:
	default:
		log.Printf("[CLUSTER:%s] Persistence queue full, deleting synchronously for private array: %s\n", c.getNodeID(), arrayName)
		if err := c.deletePrivateArrayFromDisk(arrayName); err != nil {
			log.Printf("[CLUSTER:%s] Error deleting private array file: %v\n", c.getNodeID(), err)
		}
	}
}

func (c *Cluster) persistenceWorker() {
	ticker := time.NewTicker(150 * time.Millisecond)
	defer ticker.Stop()

	pending := make(map[string]persistenceTask)

	flush := func() {
		for arrayName, task := range pending {
			if task.delete {
				if task.isPrivate {
					if err := c.deletePrivateArrayFromDisk(arrayName); err != nil {
						log.Printf("[CLUSTER:%s] Error deleting private array file: %v\n", c.getNodeID(), err)
						IncrementErrorCount()
					}
				} else {
					if err := c.deleteArrayFromDisk(arrayName); err != nil {
						log.Printf("[CLUSTER:%s] Error deleting array file: %v\n", c.getNodeID(), err)
						IncrementErrorCount()
					}
				}
			} else {
				if task.isPrivate {
					if err := c.savePrivateArrayToDisk(arrayName); err != nil {
						log.Printf("[CLUSTER:%s] Error saving private array to disk: %v\n", c.getNodeID(), err)
						IncrementErrorCount()
					}
				} else {
					if err := c.saveArrayToDisk(arrayName); err != nil {
						log.Printf("[CLUSTER:%s] Error saving array to disk: %v\n", c.getNodeID(), err)
						IncrementErrorCount()
					}
				}
			}
			delete(pending, arrayName)
		}
	}

	for {
		select {
		case task := <-c.persistenceChan:
			pending[task.arrayName] = task // coalesce by array, keep latest op
		case <-ticker.C:
			if len(pending) > 0 {
				flush()
			}
		}
	}
}

// saveArrayToDisk saves an array to a JSON file
func (c *Cluster) saveArrayToDisk(arrayName string) error {
	if c.runtimePath == "" {
		return fmt.Errorf("runtime path is not initialized")
	}

	c.mu.RLock()
	arrayRef, ok := c.data[arrayName]
	if !ok {
		c.mu.RUnlock()
		return nil
	}

	// Deep copy under lock to avoid concurrent map read/write panics.
	data := make(map[string]string, len(arrayRef))
	for k, v := range arrayRef {
		data[k] = v
	}
	timestamp := c.timestamps[arrayName]
	c.mu.RUnlock()

	payload := map[string]interface{}{
		"data":      data,
		"timestamp": timestamp,
	}

	filePath := filepath.Join(c.runtimePath, arrayName+".json")
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal array: %v", err)
	}

	if err := os.WriteFile(filePath, jsonData, 0644); err != nil {
		return fmt.Errorf("failed to write file: %v", err)
	}

	return nil
}

// deleteArrayFromDisk deletes an array's JSON file
func (c *Cluster) deleteArrayFromDisk(arrayName string) error {
	filePath := filepath.Join(c.runtimePath, arrayName+".json")
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete file: %v", err)
	}
	return nil
}

// savePrivateArrayToDisk saves a private array to a JSON file
func (c *Cluster) savePrivateArrayToDisk(arrayName string) error {
	if c.runtimePath == "" {
		return fmt.Errorf("runtime path is not initialized")
	}

	c.mu.RLock()
	arrayRef, ok := c.privateData[arrayName]
	if !ok {
		c.mu.RUnlock()
		return nil
	}

	// Deep copy under lock to avoid concurrent map read/write panics.
	data := make(map[string]string, len(arrayRef))
	for k, v := range arrayRef {
		data[k] = v
	}
	timestamp := c.timestamps[arrayName]
	c.mu.RUnlock()

	payload := map[string]interface{}{
		"data":      data,
		"timestamp": timestamp,
	}

	filePath := filepath.Join(c.runtimePath, "private_"+arrayName+".json")
	jsonData, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal private array: %v", err)
	}

	if err := os.WriteFile(filePath, jsonData, 0600); err != nil {
		return fmt.Errorf("failed to write private file: %v", err)
	}

	return nil
}

// deletePrivateArrayFromDisk deletes a private array's JSON file
func (c *Cluster) deletePrivateArrayFromDisk(arrayName string) error {
	filePath := filepath.Join(c.runtimePath, "private_"+arrayName+".json")
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete private file: %v", err)
	}
	return nil
}

// encryptMessage encrypts plaintext using AES-256-GCM
func (c *Cluster) encryptMessage(plaintext string) (string, error) {
	if len(c.aesKey) != 32 {
		return "", fmt.Errorf("invalid AES key length: %d", len(c.aesKey))
	}

	block, err := aes.NewCipher(c.aesKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", err
	}

	ciphertext := gcm.Seal(nonce, nonce, []byte(plaintext), nil)
	return base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptMessage decrypts base64-encoded ciphertext using AES-256-GCM
func (c *Cluster) decryptMessage(encryptedText string) (string, error) {
	if len(c.aesKey) != 32 {
		return "", fmt.Errorf("invalid AES key length: %d", len(c.aesKey))
	}

	ciphertext, err := base64.StdEncoding.DecodeString(encryptedText)
	if err != nil {
		return "", err
	}

	block, err := aes.NewCipher(c.aesKey)
	if err != nil {
		return "", err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return "", err
	}

	return string(plaintext), nil
}

// isIPAllowed checks if the given IP is allowed by the whitelist
// Supports exact IP matches and CIDR subnets (IPv4 and IPv6)
// If whitelist is empty, all IPs are allowed
func (c *Cluster) isIPAllowed(remoteAddr string, whitelist []string) bool {
	// If whitelist is empty, allow all
	if len(whitelist) == 0 {
		return true
	}

	// Extract IP from remoteAddr (format: "IP:PORT")
	ip := remoteAddr
	if idx := strings.LastIndex(remoteAddr, ":"); idx != -1 {
		ip = remoteAddr[:idx]
	}

	// Remove brackets from IPv6 addresses (e.g., "[::1]" -> "::1")
	ip = strings.Trim(ip, "[]")

	parsedIP := net.ParseIP(ip)
	if parsedIP == nil {
		return false // Invalid IP format
	}

	// Check against whitelist
	for _, whitelistedEntry := range whitelist {
		whitelistedEntry = strings.TrimSpace(whitelistedEntry)
		if whitelistedEntry == "" {
			continue
		}

		// Try to parse as CIDR notation first
		if strings.Contains(whitelistedEntry, "/") {
			_, network, err := net.ParseCIDR(whitelistedEntry)
			if err == nil && network.Contains(parsedIP) {
				return true
			}
			continue
		}

		// Handle special cases (any IPv4 or IPv6)
		if whitelistedEntry == "::" && strings.Contains(ip, ":") {
			return true
		}
		if whitelistedEntry == "0.0.0.0" && !strings.Contains(ip, ":") {
			return true
		}

		// Exact IP match
		if ip == whitelistedEntry {
			return true
		}
	}

	return false
}

// Write adds or updates a key-value pair in an array
func (c *Cluster) Write(arrayName, value string) error {
	c.mu.Lock()

	if c.data[arrayName] == nil {
		c.data[arrayName] = make(map[string]string)
	}

	// Parse value as "key: value" format
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		c.mu.Unlock()
		return fmt.Errorf("invalid format, expected 'key: value'")
	}

	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])

	c.data[arrayName][key] = val

	// Update timestamp only if array is not empty
	if len(c.data[arrayName]) > 0 {
		c.timestamps[arrayName] = time.Now()
	}

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Queue persistence operation
	c.queueSave(arrayName)

	// Broadcast to peers asynchronously
	c.broadcastToPeers()

	// Notify WebSocket subscribers
	c.BroadcastArrayUpdate(arrayName)

	return nil
}

// Read retrieves a value from an array
func (c *Cluster) Read(arrayName, key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if array, ok := c.data[arrayName]; ok {
		if val, ok := array[key]; ok {
			return val, nil
		}
		return "", fmt.Errorf("key '%s' not found in array '%s'", key, arrayName)
	}
	return "", fmt.Errorf("array '%s' not found", arrayName)
}

// Update modifies an existing key-value pair
func (c *Cluster) Update(arrayName, value string) error {
	c.mu.Lock()

	if c.data[arrayName] == nil {
		c.mu.Unlock()
		return fmt.Errorf("array '%s' not found", arrayName)
	}

	// Parse value as "key: value" format
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		c.mu.Unlock()
		return fmt.Errorf("invalid format, expected 'key: value'")
	}

	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])

	if _, ok := c.data[arrayName][key]; !ok {
		c.mu.Unlock()
		return fmt.Errorf("key '%s' not found in array '%s'", key, arrayName)
	}

	c.data[arrayName][key] = val

	// Update timestamp only if array is not empty
	if len(c.data[arrayName]) > 0 {
		c.timestamps[arrayName] = time.Now()
	}

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Queue persistence operation
	c.queueSave(arrayName)

	// Broadcast to peers asynchronously
	c.broadcastToPeers()

	// Notify WebSocket subscribers
	c.BroadcastArrayUpdate(arrayName)

	return nil
}

// Delete removes a key from an array
func (c *Cluster) Delete(arrayName, key string) error {
	c.mu.Lock()

	if array, ok := c.data[arrayName]; ok {
		if _, ok := array[key]; ok {
			delete(c.data[arrayName], key)

			// Track what to do after lock is released
			isEmpty := len(c.data[arrayName]) == 0
			c.timestamps[arrayName] = time.Now()

			// Release lock BEFORE I/O operations
			c.mu.Unlock()

			// Queue file operation
			if isEmpty {
				c.queueDelete(arrayName)
			} else {
				c.queueSave(arrayName)
			}

			// Broadcast to peers asynchronously
			c.broadcastToPeers()

			// Notify WebSocket subscribers
			c.BroadcastArrayUpdate(arrayName)

			return nil
		}
		c.mu.Unlock()
		return fmt.Errorf("key '%s' not found in array '%s'", key, arrayName)
	}
	c.mu.Unlock()
	return fmt.Errorf("array '%s' not found", arrayName)
}

// DeleteArray removes an entire array
func (c *Cluster) DeleteArray(arrayName string) error {
	c.mu.Lock()

	if _, ok := c.data[arrayName]; !ok {
		c.mu.Unlock()
		return fmt.Errorf("array '%s' not found", arrayName)
	}

	delete(c.data, arrayName)
	c.timestamps[arrayName] = time.Now()

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Queue persistence operation
	c.queueDelete(arrayName)

	// Broadcast to peers asynchronously
	c.broadcastToPeers()

	return nil
}

// WritePrivate adds or updates a key-value pair in a private array
func (c *Cluster) WritePrivate(arrayName, value string) error {
	c.mu.Lock()

	if c.privateData[arrayName] == nil {
		c.privateData[arrayName] = make(map[string]string)
	}

	// Parse value as "key: value" format
	parts := strings.SplitN(value, ":", 2)
	if len(parts) != 2 {
		c.mu.Unlock()
		return fmt.Errorf("invalid format, expected 'key: value'")
	}

	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])

	c.privateData[arrayName][key] = val

	// Update timestamp only if array is not empty
	if len(c.privateData[arrayName]) > 0 {
		c.timestamps[arrayName] = time.Now()
	}

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Queue persistence operation
	c.queueSavePrivate(arrayName)

	// Broadcast to peers asynchronously
	c.broadcastToPeers()

	// Notify WebSocket subscribers (private arrays use same broadcast)
	c.BroadcastArrayUpdate(arrayName)

	return nil
}

// ReadPrivate retrieves a value from a private array
func (c *Cluster) ReadPrivate(arrayName, key string) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if array, ok := c.privateData[arrayName]; ok {
		if val, ok := array[key]; ok {
			return val, nil
		}
		return "", fmt.Errorf("key '%s' not found in private array '%s'", key, arrayName)
	}
	return "", fmt.Errorf("private array '%s' not found", arrayName)
}

// DeletePrivate removes a key from a private array
func (c *Cluster) DeletePrivate(arrayName, key string) error {
	c.mu.Lock()

	if array, ok := c.privateData[arrayName]; ok {
		if _, ok := array[key]; ok {
			delete(c.privateData[arrayName], key)

			// Track what to do after lock is released
			isEmpty := len(c.privateData[arrayName]) == 0
			c.timestamps[arrayName] = time.Now()

			// Release lock BEFORE I/O operations
			c.mu.Unlock()

			// Queue file operation
			if isEmpty {
				c.queueDeletePrivate(arrayName)
			} else {
				c.queueSavePrivate(arrayName)
			}

			// Broadcast to peers asynchronously
			c.broadcastToPeers()
			return nil
		}
		c.mu.Unlock()
		return fmt.Errorf("key '%s' not found in private array '%s'", key, arrayName)
	}
	c.mu.Unlock()
	return fmt.Errorf("private array '%s' not found", arrayName)
}

// DeletePrivateArray removes an entire private array
func (c *Cluster) DeletePrivateArray(arrayName string) error {
	c.mu.Lock()

	if _, ok := c.privateData[arrayName]; !ok {
		c.mu.Unlock()
		return fmt.Errorf("private array '%s' not found", arrayName)
	}

	delete(c.privateData, arrayName)
	c.timestamps[arrayName] = time.Now()

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Queue persistence operation
	c.queueDeletePrivate(arrayName)

	// Broadcast to peers asynchronously
	c.broadcastToPeers()

	return nil
}

// GetMetrics returns cluster metrics
func (c *Cluster) GetMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Convert peer bootup times to a readable format
	peerDetails := make(map[string]map[string]string, len(c.peers))
	currentTime := time.Now().Format(time.RFC3339)

	for i, peer := range c.peers {
		displayName := peer
		if c.ClusterHideIP {
			displayName = fmt.Sprintf("node%d", i)
		}
		normalizedPeer := normalizeNodeAddr(peer)

		bootup := ""
		if bootTime, ok := c.peerBootupTimes[peer]; ok {
			bootup = bootTime.Format(time.RFC3339)
		} else if bootTime, ok := c.peerBootupTimes[normalizedPeer]; ok {
			bootup = bootTime.Format(time.RFC3339)
		}

		lastSeen := ""
		status := "offline"
		if seenAt, ok := c.peerLastSeen[peer]; ok {
			lastSeen = seenAt.Format(time.RFC3339)
			if time.Since(seenAt) <= peerAliveTTL {
				status = "online"
			}
		} else if seenAt, ok := c.peerLastSeen[normalizedPeer]; ok {
			lastSeen = seenAt.Format(time.RFC3339)
			if time.Since(seenAt) <= peerAliveTTL {
				status = "online"
			}
		}

		peerDetails[displayName] = map[string]string{
			"node_id":        strconv.Itoa(i + 1),
			"bootup_time":    bootup,
			"current_status": status,
			"last_seen":      lastSeen,
			"current_time":   currentTime,
		}
	}

	// Determine cluster health status
	var health string
	peerCount := len(c.peers)
	if peerCount == 0 {
		health = "disabled"
	} else {
		responsivePeers := 0
		for _, peer := range c.peers {
			normalizedPeer := normalizeNodeAddr(peer)
			seenAt, ok := c.peerLastSeen[peer]
			if !ok {
				seenAt, ok = c.peerLastSeen[normalizedPeer]
			}
			if ok && time.Since(seenAt) <= peerAliveTTL {
				responsivePeers++
			}
		}
		if responsivePeers == peerCount {
			health = "healthy"
		} else if responsivePeers > 0 {
			health = "degraded"
		} else {
			health = "degraded"
		}
	}

	return map[string]interface{}{
		"bootup_time":      c.bootupTime.Format(time.RFC3339),
		"current_node_id":  c.getNodeID(),
		"uptime_seconds":   int64(time.Since(c.bootupTime).Seconds()),
		"last_sync":        c.lastSyncTime.Format(time.RFC3339),
		"total_syncs":      c.syncCount,
		"failed_syncs":     c.failedSyncs,
		"total_sync_bytes": c.totalSyncBytes,
		"error_count":      c.errorCount,
		"peer_count":       len(c.peers),
		"peers":            peerDetails,
		"health":           health,
	}
}

// GetAllWithMetrics returns arrays and metrics together
func (c *Cluster) GetAllWithMetrics() map[string]interface{} {
	return map[string]interface{}{
		"arrays":  c.GetAll(),
		"metrics": c.GetMetrics(),
	}
}

// GetAllWithMetricsAndPrivate returns both public and private arrays with metrics
func (c *Cluster) GetAllWithMetricsAndPrivate() map[string]interface{} {
	return map[string]interface{}{
		"arrays":         c.GetAll(),
		"private_arrays": c.GetAllPrivate(),
		"metrics":        c.GetMetrics(),
	}
}

// GetAll returns all public arrays
func (c *Cluster) GetAll() map[string]map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Deep copy
	result := make(map[string]map[string]string)
	for arrayName, array := range c.data {
		result[arrayName] = make(map[string]string)
		for k, v := range array {
			result[arrayName][k] = v
		}
	}
	return result
}

// GetAllPrivate returns all private arrays (requires authentication)
func (c *Cluster) GetAllPrivate() map[string]map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// Deep copy
	result := make(map[string]map[string]string)
	for arrayName, array := range c.privateData {
		result[arrayName] = make(map[string]string)
		for k, v := range array {
			result[arrayName][k] = v
		}
	}
	return result
}

// GetArray returns a single array by name.
// If the array does not exist, it returns an empty map.
func (c *Cluster) GetArray(arrayName string) map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	array, ok := c.data[arrayName]
	if !ok {
		return map[string]string{}
	}

	// Deep copy
	result := make(map[string]string)
	for k, v := range array {
		result[k] = v
	}

	return result
}

// UpdateFromPeer merges data from a peer based on timestamps
func (c *Cluster) UpdateFromPeer(data map[string]map[string]string, timestamps map[string]time.Time) {
	c.mu.Lock()

	// Track which arrays need disk updates
	arraysToBeSaved := []string{}
	arraysToBeDeleted := []string{}

	for arrayName, peerTimestamp := range timestamps {
		if peerTimestamp.IsZero() {
			continue
		}
		localTimestamp := c.timestamps[arrayName]

		// Apply only newer peer state to avoid resurrecting stale values.
		if localTimestamp.IsZero() || peerTimestamp.After(localTimestamp) {
			array, exists := data[arrayName]
			if !exists {
				// Tombstone/full-array delete propagated by timestamp without data.
				delete(c.data, arrayName)
				c.timestamps[arrayName] = peerTimestamp
				arraysToBeDeleted = append(arraysToBeDeleted, arrayName)
				continue
			}

			if c.data[arrayName] == nil {
				c.data[arrayName] = make(map[string]string)
			}
			// Replace current contents with peer contents (including empty maps).
			for k := range c.data[arrayName] {
				delete(c.data[arrayName], k)
			}
			for k, v := range array {
				c.data[arrayName][k] = v
			}

			c.timestamps[arrayName] = peerTimestamp
			if len(array) == 0 {
				arraysToBeDeleted = append(arraysToBeDeleted, arrayName)
			} else {
				arraysToBeSaved = append(arraysToBeSaved, arrayName)
			}
		}
	}

	log.Printf("[CLUSTER:%s] Merged data from peer\n", c.getNodeID())

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Save to disk asynchronously (without lock)
	for _, arrayName := range arraysToBeSaved {
		c.queueSave(arrayName)
	}
	for _, arrayName := range arraysToBeDeleted {
		c.queueDelete(arrayName)
	}
}

// UpdateFromPeerPrivate merges private data from a peer based on timestamps
func (c *Cluster) UpdateFromPeerPrivate(data map[string]map[string]string, timestamps map[string]time.Time) {
	c.mu.Lock()

	// Track which arrays need disk updates
	arraysToBeSaved := []string{}
	arraysToBeDeleted := []string{}

	for arrayName, peerTimestamp := range timestamps {
		if peerTimestamp.IsZero() {
			continue
		}
		localTimestamp := c.timestamps[arrayName]

		// Apply only newer peer state to avoid resurrecting stale values.
		if localTimestamp.IsZero() || peerTimestamp.After(localTimestamp) {
			array, exists := data[arrayName]
			if !exists {
				// Tombstone/full-array delete propagated by timestamp without data.
				delete(c.privateData, arrayName)
				c.timestamps[arrayName] = peerTimestamp
				arraysToBeDeleted = append(arraysToBeDeleted, arrayName)
				continue
			}

			if c.privateData[arrayName] == nil {
				c.privateData[arrayName] = make(map[string]string)
			}
			// Replace current contents with peer contents (including empty maps).
			for k := range c.privateData[arrayName] {
				delete(c.privateData[arrayName], k)
			}
			for k, v := range array {
				c.privateData[arrayName][k] = v
			}

			c.timestamps[arrayName] = peerTimestamp
			if len(array) == 0 {
				arraysToBeDeleted = append(arraysToBeDeleted, arrayName)
			} else {
				arraysToBeSaved = append(arraysToBeSaved, arrayName)
			}
		}
	}

	log.Printf("[CLUSTER:%s] Merged private data from peer\n", c.getNodeID())

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Save to disk asynchronously (without lock)
	for _, arrayName := range arraysToBeSaved {
		c.queueSavePrivate(arrayName)
	}
	for _, arrayName := range arraysToBeDeleted {
		c.queueDeletePrivate(arrayName)
	}
}

// syncFromPeersOnStartup tries to fetch data from peers when starting up with retries
func (c *Cluster) syncFromPeersOnStartup() {
	time.Sleep(1 * time.Second) // Wait a bit for server to be ready

	for _, peer := range clusterArray.peers {
		go func(peerAddr string) {
			// Retry with exponential backoff
			maxRetries := 5
			for attempt := 0; attempt < maxRetries; attempt++ {
				if attempt > 0 {
					delay := time.Duration(attempt*2) * time.Second
					log.Printf("[CLUSTER:%s] Retrying sync from peer %s in %v (attempt %d/%d)\n", clusterArray.getNodeID(), peerAddr, delay, attempt+1, maxRetries)
					time.Sleep(delay)
				}

				conn, err := net.DialTimeout("tcp", formatPeerAddr(peerAddr)+":9000", 5*time.Second)
				if err != nil {
					if attempt == maxRetries-1 {
						log.Printf("[CLUSTER:%s] Failed to sync from peer %s after %d attempts: %v\n", clusterArray.getNodeID(), peerAddr, maxRetries, err)
						IncrementErrorCount()
					}
					continue
				}
				defer conn.Close()

				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)

				// Send peer secret for authentication
				fmt.Fprintf(writer, "%s\n", clusterArray.peerSecret)
				writer.Flush()

				// Wait for auth response
				authResp, err := reader.ReadString('\n')
				if err != nil || strings.TrimSpace(authResp) != "AUTH_OK" {
					log.Printf("[CLUSTER:%s] Authentication failed with peer %s\n", clusterArray.getNodeID(), peerAddr)
					continue
				}

				fmt.Fprintf(writer, "GET_ARRAYS\n")
				writer.Flush()

				line, err := reader.ReadString('\n')
				if err != nil {
					log.Printf("[CLUSTER:%s] Error reading from peer %s: %v\n", clusterArray.getNodeID(), peerAddr, err)
					continue
				}

				// Decrypt the response
				jsonStr, err := c.decryptMessage(strings.TrimSpace(line))
				if err != nil {
					log.Printf("[CLUSTER:%s] Decryption failed: %v\n", clusterArray.getNodeID(), err)
					IncrementErrorCount()
					continue
				}

				// Parse JSON with timestamps and bootup time
				var payload struct {
					Data        map[string]map[string]string `json:"data"`
					PrivateData map[string]map[string]string `json:"private_data"`
					Timestamps  map[string]time.Time         `json:"timestamps"`
					BootupTime  time.Time                    `json:"bootup_time"`
				}

				if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
					log.Printf("[CLUSTER:%s] Error unmarshaling peer data: %v\n", clusterArray.getNodeID(), err)
					continue
				}

				// Store peer's bootup time
				if !payload.BootupTime.IsZero() {
					c.mu.Lock()
					c.peerBootupTimes[peerAddr] = payload.BootupTime
					c.peerBootupTimes[normalizeNodeAddr(peerAddr)] = payload.BootupTime
					c.peerLastSeen[peerAddr] = time.Now()
					c.peerLastSeen[normalizeNodeAddr(peerAddr)] = time.Now()
					c.mu.Unlock()
				}

				c.UpdateFromPeer(payload.Data, payload.Timestamps)
				c.UpdateFromPeerPrivate(payload.PrivateData, payload.Timestamps)
				log.Printf("[CLUSTER:%s] Successfully synced from peer %s on startup\n", clusterArray.getNodeID(), peerAddr)
				return // Success, exit retry loop
			}
		}(peer)
	}

	// Start periodic background sync to keep in sync with peers
	go c.periodicPeerSync()

	// Start fast heartbeat loop for near-immediate peer liveness detection.
	go c.peerHeartbeatLoop()
}

// formatPeerAddr wraps IPv6 addresses in brackets for net.Dial
func formatPeerAddr(addr string) string {
	// If address contains colons (IPv6) and not already bracketed, add brackets
	if strings.Contains(addr, ":") && !strings.HasPrefix(addr, "[") {
		return "[" + addr + "]"
	}
	return addr
}

// periodicPeerSync runs a background task to periodically sync with peers
func (c *Cluster) periodicPeerSync() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, peer := range clusterArray.peers {
			go func(peerAddr string) {
				conn, err := net.DialTimeout("tcp", formatPeerAddr(peerAddr)+":9000", 5*time.Second)
				if err != nil {
					// Silently fail, we'll retry next time
					return
				}
				defer conn.Close()

				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)

				// Send peer secret for authentication
				fmt.Fprintf(writer, "%s\n", clusterArray.peerSecret)
				writer.Flush()

				// Wait for auth response
				authResp, err := reader.ReadString('\n')
				if err != nil || strings.TrimSpace(authResp) != "AUTH_OK" {
					return
				}

				fmt.Fprintf(writer, "GET_ARRAYS\n")
				writer.Flush()

				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}

				// Decrypt the response
				jsonStr, err := c.decryptMessage(strings.TrimSpace(line))
				if err != nil {
					return
				}

				var payload struct {
					Data        map[string]map[string]string `json:"data"`
					PrivateData map[string]map[string]string `json:"private_data"`
					Timestamps  map[string]time.Time         `json:"timestamps"`
					BootupTime  time.Time                    `json:"bootup_time"`
				}

				if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
					return
				}

				// Store peer's bootup time
				if !payload.BootupTime.IsZero() {
					c.mu.Lock()
					c.peerBootupTimes[peerAddr] = payload.BootupTime
					c.peerBootupTimes[normalizeNodeAddr(peerAddr)] = payload.BootupTime
					c.peerLastSeen[peerAddr] = time.Now()
					c.peerLastSeen[normalizeNodeAddr(peerAddr)] = time.Now()
					c.mu.Unlock()
				}

				c.UpdateFromPeer(payload.Data, payload.Timestamps)
				c.UpdateFromPeerPrivate(payload.PrivateData, payload.Timestamps)
			}(peer)
		}
	}
}

// peerHeartbeatLoop performs lightweight authenticated pings to peers every second.
func (c *Cluster) peerHeartbeatLoop() {
	ticker := time.NewTicker(peerHeartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		for _, peer := range c.peers {
			c.pingPeer(peer)
		}
	}
}

func (c *Cluster) pingPeer(peerAddr string) {
	conn, err := net.DialTimeout("tcp", formatPeerAddr(peerAddr)+":9000", peerHeartbeatTimeout)
	if err != nil {
		return
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(peerHeartbeatTimeout))

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Send peer secret for authentication
	fmt.Fprintf(writer, "%s\n", c.peerSecret)
	if err := writer.Flush(); err != nil {
		return
	}

	// Wait for auth response
	authResp, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(authResp) != "AUTH_OK" {
		return
	}

	// Lightweight ping command
	fmt.Fprintf(writer, "PING\n")
	if err := writer.Flush(); err != nil {
		return
	}

	pong, err := reader.ReadString('\n')
	if err != nil || strings.TrimSpace(pong) != "PONG" {
		return
	}

	now := time.Now()
	normalized := normalizeNodeAddr(peerAddr)
	c.mu.Lock()
	c.peerLastSeen[peerAddr] = now
	c.peerLastSeen[normalized] = now
	c.mu.Unlock()
}

// startPeerSyncServer starts a TCP server for peer synchronization
func (c *Cluster) startPeerSyncServer(port string) {
	addr := c.listenAddr + ":" + port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[CLUSTER:%s] Error starting peer sync server: %v\n", c.getNodeID(), err)
		return
	}
	defer listener.Close()

	log.Printf("[CLUSTER:%s] Peer sync server started on %s\n", c.getNodeID(), addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[CLUSTER:%s] Error accepting connection: %v\n", c.getNodeID(), err)
			continue
		}

		go c.handlePeerConnection(conn)
	}
}

// handlePeerConnection handles incoming peer connections with authentication
func (c *Cluster) handlePeerConnection(conn net.Conn) {
	defer conn.Close()

	// Check IP whitelist first
	if !c.isIPAllowed(conn.RemoteAddr().String(), c.clusterWhitelist) {
		log.Printf("[CLUSTER:%s] Rejected connection from %s: IP not whitelisted\n", c.getNodeID(), conn.RemoteAddr())
		return
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Step 1: Receive and verify peer secret
	secretLine, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[CLUSTER:%s] Error reading peer secret: %v\n", c.getNodeID(), err)
		return
	}

	secretLine = strings.TrimSpace(secretLine)
	if secretLine != c.peerSecret {
		log.Printf("[CLUSTER:%s] Invalid peer secret from %s\n", c.getNodeID(), conn.RemoteAddr())
		fmt.Fprintf(writer, "AUTH_FAILED\n")
		writer.Flush()
		return
	}

	// Authentication successful
	fmt.Fprintf(writer, "AUTH_OK\n")
	writer.Flush()
	log.Printf("[CLUSTER:%s] Peer authenticated from %s\n", c.getNodeID(), conn.RemoteAddr())

	// Mark peer as recently seen once authenticated.
	remoteAddr := conn.RemoteAddr().String()
	if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
		remoteAddr = host
	}
	remoteAddr = strings.TrimPrefix(strings.TrimSuffix(remoteAddr, "]"), "[")
	c.mu.Lock()
	c.peerLastSeen[remoteAddr] = time.Now()
	c.peerLastSeen[normalizeNodeAddr(remoteAddr)] = time.Now()
	c.mu.Unlock()

	// Step 2: Read command
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[CLUSTER:%s] Error reading command from peer: %v\n", c.getNodeID(), err)
		return
	}

	line = strings.TrimSpace(line)

	if line == "GET_ARRAYS" {
		c.mu.RLock()
		payload := map[string]interface{}{
			"data":         c.data,
			"private_data": c.privateData,
			"timestamps":   c.timestamps,
			"bootup_time":  c.bootupTime,
		}
		c.mu.RUnlock()

		jsonData, _ := json.Marshal(payload)

		// Encrypt the response
		encrypted, err := c.encryptMessage(string(jsonData))
		if err != nil {
			log.Printf("[CLUSTER:%s] Encryption failed: %v\n", c.getNodeID(), err)
			return
		}

		// Store peer's bootup time from incoming connection
		remoteAddr := conn.RemoteAddr().String()
		if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
			remoteAddr = host
		}
		// Strip brackets from IPv6 addresses for consistent key format
		remoteAddr = strings.TrimPrefix(strings.TrimSuffix(remoteAddr, "]"), "[")

		c.mu.Lock()
		c.peerBootupTimes[remoteAddr] = c.bootupTime
		c.peerBootupTimes[normalizeNodeAddr(remoteAddr)] = c.bootupTime
		c.peerLastSeen[remoteAddr] = time.Now()
		c.peerLastSeen[normalizeNodeAddr(remoteAddr)] = time.Now()
		c.mu.Unlock()

		fmt.Fprintf(writer, "%s\n", encrypted)
		writer.Flush()
		log.Printf("[CLUSTER:%s] Sent encrypted arrays to peer\n", c.getNodeID())
	} else if line == "PING" {
		fmt.Fprintf(writer, "PONG\n")
		writer.Flush()
		return
	} else if strings.HasPrefix(line, "UPDATE ") {
		encryptedData := strings.TrimPrefix(line, "UPDATE ")

		// Decrypt the update
		jsonStr, err := c.decryptMessage(encryptedData)
		if err != nil {
			log.Printf("[CLUSTER:%s] Decryption failed: %v\n", clusterArray.getNodeID(), err)
			return
		}

		var payload struct {
			Data        map[string]map[string]string `json:"data"`
			PrivateData map[string]map[string]string `json:"private_data"`
			Timestamps  map[string]time.Time         `json:"timestamps"`
			BootupTime  time.Time                    `json:"bootup_time"`
		}

		if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
			log.Printf("[CLUSTER:%s] Error unmarshaling update data: %v\n", c.getNodeID(), err)
			return
		}

		// Store peer's bootup time
		if !payload.BootupTime.IsZero() {
			c.mu.Lock()
			// Extract host/IP from remote address to normalize peer identification (remove ephemeral port)
			remoteAddr := conn.RemoteAddr().String()
			if host, _, err := net.SplitHostPort(remoteAddr); err == nil {
				remoteAddr = host
			}
			// Strip brackets from IPv6 addresses for consistent key format
			remoteAddr = strings.TrimPrefix(strings.TrimSuffix(remoteAddr, "]"), "[")
			c.peerBootupTimes[remoteAddr] = payload.BootupTime
			c.peerBootupTimes[normalizeNodeAddr(remoteAddr)] = payload.BootupTime
			c.peerLastSeen[remoteAddr] = time.Now()
			c.peerLastSeen[normalizeNodeAddr(remoteAddr)] = time.Now()
			c.mu.Unlock()
		}

		c.UpdateFromPeer(payload.Data, payload.Timestamps)
		c.UpdateFromPeerPrivate(payload.PrivateData, payload.Timestamps)
	}
}

// triggerBroadcast signals the broadcast worker to send updates
func (c *Cluster) triggerBroadcast() {
	select {
	case clusterArray.broadcastChan <- true:
		// Signal sent
	default:
		// Channel full, skip (broadcast already triggered)
	}
}

// broadcastWorker processes broadcast requests in the background
func (c *Cluster) broadcastWorker() {
	for range c.broadcastChan {
		// Debounce - wait a bit to batch updates
		time.Sleep(100 * time.Millisecond)

		c.mu.RLock()
		payload := map[string]interface{}{
			"data":         c.data,
			"private_data": c.privateData,
			"timestamps":   c.timestamps,
			"bootup_time":  c.bootupTime,
		}
		c.mu.RUnlock()

		jsonData, _ := json.Marshal(payload)

		c.mu.Lock()
		c.syncCount++
		c.totalSyncBytes += int64(len(jsonData))
		c.lastSyncTime = time.Now()
		c.mu.Unlock()

		for _, peer := range c.peers {
			go func(peerAddr string) {
				conn, err := net.DialTimeout("tcp", formatPeerAddr(peerAddr)+":9000", 5*time.Second)
				if err != nil {
					c.mu.Lock()
					c.failedSyncs++
					c.mu.Unlock()
					return
				}
				defer conn.Close()

				reader := bufio.NewReader(conn)
				writer := bufio.NewWriter(conn)

				// Send peer secret for authentication
				fmt.Fprintf(writer, "%s\n", c.peerSecret)
				writer.Flush()

				// Wait for auth response
				authResp, err := reader.ReadString('\n')
				if err != nil || strings.TrimSpace(authResp) != "AUTH_OK" {
					log.Printf("[CLUSTER:%s] Authentication failed with peer %s\n", clusterArray.getNodeID(), peerAddr)
					c.mu.Lock()
					c.failedSyncs++
					c.mu.Unlock()
					return
				}

				// Encrypt the update
				encrypted, err := c.encryptMessage(string(jsonData))
				if err != nil {
					log.Printf("[CLUSTER:%s] Encryption failed: %v\n", c.getNodeID(), err)
					return
				}

				fmt.Fprintf(writer, "UPDATE %s\n", encrypted)
				writer.Flush()
			}(peer)
		}
	}
}

// broadcastToPeers signals to broadcast (non-blocking)
func (c *Cluster) broadcastToPeers() {
	c.triggerBroadcast()
}

// Export exports all arrays (public + private) with metrics to stdout as JSON
func (c *Cluster) Export() error {
	exportData := c.GetAllWithMetricsAndPrivate()

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(exportData); err != nil {
		return fmt.Errorf("failed to export: %v", err)
	}

	return nil
}

// ExportArray exports a single array to stdout as JSON.
// If the array does not exist, it exports {}.
func (c *Cluster) ExportArray(arrayName string) error {
	exportData := c.GetArray(arrayName)

	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(exportData); err != nil {
		return fmt.Errorf("failed to export array: %v", err)
	}

	return nil
}

// Import imports arrays from stdin (JSON format - supports public and private arrays)
func (c *Cluster) Import(clearFirst bool) error {
	// Read JSON from stdin - use generic map to support multiple formats
	var importData map[string]interface{}

	decoder := json.NewDecoder(os.Stdin)
	if err := decoder.Decode(&importData); err != nil {
		return fmt.Errorf("failed to decode import data: %v", err)
	}

	c.mu.Lock()

	arraysToPersist := make([]string, 0)
	privateArraysToPersist := make([]string, 0)
	arraysToDelete := make([]string, 0)
	privateArraysToDelete := make([]string, 0)

	// Clear existing data if requested
	if clearFirst {
		// Track existing arrays for disk cleanup
		for arrayName := range c.data {
			arraysToDelete = append(arraysToDelete, arrayName)
		}
		for arrayName := range c.privateData {
			privateArraysToDelete = append(privateArraysToDelete, arrayName)
		}

		c.data = make(map[string]map[string]string)
		c.privateData = make(map[string]map[string]string)
		c.timestamps = make(map[string]time.Time)
		log.Printf("[CLUSTER:%s] Cleared all existing arrays before import\n", clusterArray.getNodeID())
	}

	// Determine format and extract data/timestamps
	var dataMap map[string]map[string]string
	var privateDataMap map[string]map[string]string
	var timestampsMap map[string]time.Time

	// Check if new format (with "arrays" and "metrics" keys)
	if arrays, ok := importData["arrays"]; ok {
		// New format: {"arrays": {...}, "private_arrays": {...}, "metrics": {...}}
		arraysMap, _ := arrays.(map[string]interface{})
		dataMap = c.convertInterfaceToStringMap(arraysMap)

		// Import private arrays if present
		if privateArrays, ok := importData["private_arrays"]; ok {
			privateArraysMap, _ := privateArrays.(map[string]interface{})
			privateDataMap = c.convertInterfaceToStringMap(privateArraysMap)
		}
	} else if data, ok := importData["data"]; ok {
		// Old format: {"data": {...}, "timestamps": {...}}
		if dataObj, ok := data.(map[string]interface{}); ok {
			dataMap = c.convertInterfaceToStringMap(dataObj)
		}
		if ts, ok := importData["timestamps"]; ok {
			timestampsMap = make(map[string]time.Time)
			if tsObj, ok := ts.(map[string]interface{}); ok {
				for name, raw := range tsObj {
					if s, ok := raw.(string); ok {
						if parsed, err := time.Parse(time.RFC3339, s); err == nil {
							timestampsMap[name] = parsed
						}
					}
				}
			}
		}
	}

	if dataMap == nil {
		c.mu.Unlock()
		return fmt.Errorf("invalid import format: missing 'arrays' or 'data' key")
	}

	// Import data
	for arrayName, array := range dataMap {
		if c.data[arrayName] == nil {
			c.data[arrayName] = make(map[string]string)
		}

		for k, v := range array {
			c.data[arrayName][k] = v
		}

		// Set timestamp if available
		if ts, ok := timestampsMap[arrayName]; ok {
			c.timestamps[arrayName] = ts
		} else {
			c.timestamps[arrayName] = time.Now()
		}

		arraysToPersist = append(arraysToPersist, arrayName)
	}

	// Import private data
	for arrayName, array := range privateDataMap {
		if c.privateData[arrayName] == nil {
			c.privateData[arrayName] = make(map[string]string)
		}

		for k, v := range array {
			c.privateData[arrayName][k] = v
		}

		// Set timestamp if available
		if ts, ok := timestampsMap[arrayName]; ok {
			c.timestamps[arrayName] = ts
		} else {
			c.timestamps[arrayName] = time.Now()
		}

		privateArraysToPersist = append(privateArraysToPersist, arrayName)
	}

	c.mu.Unlock()

	for _, arrayName := range arraysToDelete {
		c.queueDelete(arrayName)
	}
	for _, arrayName := range privateArraysToDelete {
		c.queueDeletePrivate(arrayName)
	}

	for _, arrayName := range arraysToPersist {
		c.queueSave(arrayName)
	}
	for _, arrayName := range privateArraysToPersist {
		c.queueSavePrivate(arrayName)
	}

	log.Printf("[CLUSTER:%s] Imported %d public arrays and %d private arrays\n", clusterArray.getNodeID(), len(dataMap), len(privateDataMap))
	return nil
}

// convertInterfaceToStringMap converts nested interface{} to map[string]map[string]string
func (c *Cluster) convertInterfaceToStringMap(data map[string]interface{}) map[string]map[string]string {
	result := make(map[string]map[string]string)
	for key, val := range data {
		if innerMap, ok := val.(map[string]interface{}); ok {
			result[key] = make(map[string]string)
			for k, v := range innerMap {
				if str, ok := v.(string); ok {
					result[key][k] = str
				}
			}
		}
	}
	return result
}

// SubscribeToArray subscribes a channel to receive updates for a specific array
// Returns the update channel and an unsubscribe function
func (c *Cluster) SubscribeToArray(arrayName string) (<-chan WebSocketUpdate, func()) {
	c.wsSubMu.Lock()
	defer c.wsSubMu.Unlock()

	// Create a buffered channel for this subscriber
	updateChan := make(chan WebSocketUpdate, 10)

	// Add subscriber to the list for this array
	if c.wsSubscriptions[arrayName] == nil {
		c.wsSubscriptions[arrayName] = make([]chan WebSocketUpdate, 0)
	}
	c.wsSubscriptions[arrayName] = append(c.wsSubscriptions[arrayName], updateChan)

	// Return the read-only channel and an unsubscribe function
	unsubscribeFn := func() {
		c.wsSubMu.Lock()
		defer c.wsSubMu.Unlock()

		if subscribers, ok := c.wsSubscriptions[arrayName]; ok {
			// Find and remove the channel
			for i, ch := range subscribers {
				if ch == updateChan {
					// Remove from slice
					c.wsSubscriptions[arrayName] = append(subscribers[:i], subscribers[i+1:]...)
					close(ch)
					break
				}
			}

			// Clean up empty arrays
			if len(c.wsSubscriptions[arrayName]) == 0 {
				delete(c.wsSubscriptions, arrayName)
			}
		}
	}

	return updateChan, unsubscribeFn
}

// UnsubscribeFromArray is deprecated - use the unsubscribe function returned by SubscribeToArray instead
func (c *Cluster) UnsubscribeFromArray(arrayName string, updateChan chan WebSocketUpdate) {
	c.wsSubMu.Lock()
	defer c.wsSubMu.Unlock()

	if subscribers, ok := c.wsSubscriptions[arrayName]; ok {
		// Find and remove the channel
		for i, ch := range subscribers {
			if ch == updateChan {
				// Remove from slice
				c.wsSubscriptions[arrayName] = append(subscribers[:i], subscribers[i+1:]...)
				close(ch)
				break
			}
		}

		// Clean up empty arrays
		if len(c.wsSubscriptions[arrayName]) == 0 {
			delete(c.wsSubscriptions, arrayName)
		}
	}
}

// BroadcastArrayUpdate sends an update to all subscribers of an array
func (c *Cluster) BroadcastArrayUpdate(arrayName string) {
	c.mu.RLock()
	data := make(map[string]string)
	if arr, exists := c.data[arrayName]; exists {
		for k, v := range arr {
			data[k] = v
		}
	}
	ts := c.timestamps[arrayName]
	c.mu.RUnlock()

	update := WebSocketUpdate{
		ArrayName: arrayName,
		Data:      data,
		Timestamp: ts,
	}

	c.wsSubMu.RLock()
	arraySubscribers := c.wsSubscriptions[arrayName]
	allSubscribers := c.wsSubscriptions[""] // subscribers watching all arrays

	if len(arraySubscribers) == 0 && len(allSubscribers) == 0 {
		c.wsSubMu.RUnlock()
		return
	}

	// Send update to all subscribers (non-blocking)
	for _, ch := range arraySubscribers {
		select {
		case ch <- update:
			// Sent successfully
		default:
			// Channel full, skip to avoid blocking
			log.Printf("[WS] Dropped update for subscriber (channel full)\n")
		}
	}

	if arrayName != "" {
		for _, ch := range allSubscribers {
			select {
			case ch <- update:
				// Sent successfully
			default:
				// Channel full, skip to avoid blocking
				log.Printf("[WS] Dropped update for subscriber (channel full)\n")
			}
		}
	}

	c.wsSubMu.RUnlock()
}
