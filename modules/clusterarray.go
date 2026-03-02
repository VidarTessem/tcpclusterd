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

// WebSocketUpdate represents an update pushed to WebSocket clients
type WebSocketUpdate struct {
	ArrayName string            `json:"array_name"`
	Data      map[string]string `json:"data"`
	Timestamp time.Time         `json:"timestamp"`
}

// Cluster represents a distributed cluster array instance
type Cluster struct {
	data             map[string]map[string]string
	privateData      map[string]map[string]string // Private arrays requiring authentication
	timestamps       map[string]time.Time
	mu               sync.RWMutex
	port             string
	listenAddr       string
	peers            []string
	lastSyncTime     time.Time
	syncCount        int64
	failedSyncs      int64
	totalSyncBytes   int64
	errorCount       int64 // Total error count across all operations
	runtimePath      string
	broadcastChan    chan bool
	aesKey           []byte
	peerSecret       string
	httpWhitelist    []string // IP addresses allowed to access HTTP
	clusterWhitelist []string // IP addresses allowed to access peer sync port
	tcpWhitelist     []string // IP addresses allowed to access TCP port
	persistenceChan  chan persistenceTask
	tcpPort          string
	tcpListenAddr    string
	tcpKey           string
	WsEnabled        bool
	HttpEnabled      bool
	bootupTime       time.Time                         // Time when this cluster node started
	peerBootupTimes  map[string]time.Time              // Bootup times of peer nodes
	wsSubscriptions  map[string][]chan WebSocketUpdate // Array name -> list of subscriber channels
	wsSubMu          sync.RWMutex                      // Lock for wsSubscriptions
	adminUsername    string                            // Admin username for authentication
	adminPassword    string                            // Admin password for authentication
	tcpTimeout       time.Duration                     // TCP connection timeout
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
		wsSubscriptions: make(map[string][]chan WebSocketUpdate),
		tcpTimeout:      30 * time.Second, // Default 30 second timeout
	}

	// Start background broadcast worker
	go clusterArray.broadcastWorker()
	go clusterArray.persistenceWorker()
}

// CreateRuntimeDirectory creates a new runtime directory with current timestamp
func (c *Cluster) CreateRuntimeDirectory() error {
	timestamp := time.Now().Format("2006-01-02_15-04-05")
	runtimePath := filepath.Join("runtime", timestamp)

	if err := os.MkdirAll(runtimePath, 0755); err != nil {
		return fmt.Errorf("failed to create runtime directory: %v", err)
	}

	c.runtimePath = runtimePath
	log.Printf("[CLUSTER] Runtime directory: %s\n", runtimePath)
	return nil
}

// InitClusterArray initializes the cluster from environment variables
func InitClusterArray(env map[string]string, loadLastConfig bool) {
	// Initialize logging system
	if err := InitLogger(env); err != nil {
		log.Printf("[CLUSTER] Failed to initialize logger: %v", err)
	}

	// Reset slice-based configuration to avoid duplicates when re-initialized.
	clusterArray.peers = clusterArray.peers[:0]
	clusterArray.httpWhitelist = clusterArray.httpWhitelist[:0]
	clusterArray.clusterWhitelist = clusterArray.clusterWhitelist[:0]

	if peersStr, ok := env["CLUSTER_PEERS"]; ok {
		peers := strings.Split(peersStr, ",")
		for _, peer := range peers {
			peer = strings.TrimSpace(peer)
			if peer != "" {
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
			log.Printf("[CLUSTER] Warning: CLUSTER_CIPHER_KEY must be 32 bytes (or base64-encoded 32 bytes), got %d\n", len(cipherKey))
			key = []byte(cipherKey) // Use as-is (will fail at encryption time with clear error)
		}

		clusterArray.aesKey = key
	}

	if peerSecret, ok := env["CLUSTER_PEER_SECRET"]; ok && peerSecret != "" {
		clusterArray.peerSecret = peerSecret
	}

	// Parse HTTP whitelist IPs
	if httpWhitelist, ok := env["HTTP_WHITELIST_IPS"]; ok && httpWhitelist != "" {
		for _, ip := range strings.Split(httpWhitelist, ",") {
			ip = strings.TrimSpace(ip)
			if ip != "" {
				clusterArray.httpWhitelist = append(clusterArray.httpWhitelist, ip)
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

	// Start TCP server if configured
	if clusterArray.tcpKey != "" {
		go clusterArray.StartTCPServer(clusterArray.tcpPort, clusterArray.tcpListenAddr, clusterArray.tcpKey)
	}

	// Load from last config if requested
	if loadLastConfig {
		if err := clusterArray.loadFromLastConfig(); err != nil {
			log.Printf("[CLUSTER] Error loading last config: %v\n", err)
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
	// log.Printf("[CLUSTER] Loading from last config: %s\n", latestPath)

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
			log.Printf("[CLUSTER] Error reading file %s: %v\n", filePath, err)
			continue
		}

		var payload struct {
			Data      map[string]string `json:"data"`
			Timestamp time.Time         `json:"timestamp"`
		}

		if err := json.Unmarshal(data, &payload); err != nil {
			log.Printf("[CLUSTER] Error unmarshaling file %s: %v\n", filePath, err)
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

		// log.Printf("[CLUSTER] Loaded array: %s (%d keys)\n", arrayName, len(payload.Data))
	}

	log.Printf("[CLUSTER] Loaded %d public arrays and %d private arrays from %s\n", loadedCount, loadedPrivateCount, latestPath)
	return nil
}

func (c *Cluster) queueSave(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: false, isPrivate: false}:
	default:
		log.Printf("[CLUSTER] Persistence queue full, saving synchronously for array: %s\n", arrayName)
		if err := c.saveArrayToDisk(arrayName); err != nil {
			log.Printf("[CLUSTER] Error saving array to disk: %v\n", err)
		}
	}
}

func (c *Cluster) queueSavePrivate(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: false, isPrivate: true}:
	default:
		log.Printf("[CLUSTER] Persistence queue full, saving synchronously for private array: %s\n", arrayName)
		if err := c.savePrivateArrayToDisk(arrayName); err != nil {
			log.Printf("[CLUSTER] Error saving private array to disk: %v\n", err)
		}
	}
}

func (c *Cluster) queueDelete(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: true, isPrivate: false}:
	default:
		log.Printf("[CLUSTER] Persistence queue full, deleting synchronously for array: %s\n", arrayName)
		if err := c.deleteArrayFromDisk(arrayName); err != nil {
			log.Printf("[CLUSTER] Error deleting array file: %v\n", err)
		}
	}
}

func (c *Cluster) queueDeletePrivate(arrayName string) {
	select {
	case c.persistenceChan <- persistenceTask{arrayName: arrayName, delete: true, isPrivate: true}:
	default:
		log.Printf("[CLUSTER] Persistence queue full, deleting synchronously for private array: %s\n", arrayName)
		if err := c.deletePrivateArrayFromDisk(arrayName); err != nil {
			log.Printf("[CLUSTER] Error deleting private array file: %v\n", err)
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
						log.Printf("[CLUSTER] Error deleting private array file: %v\n", err)
						IncrementErrorCount()
					}
				} else {
					if err := c.deleteArrayFromDisk(arrayName); err != nil {
						log.Printf("[CLUSTER] Error deleting array file: %v\n", err)
						IncrementErrorCount()
					}
				}
			} else {
				if task.isPrivate {
					if err := c.savePrivateArrayToDisk(arrayName); err != nil {
						log.Printf("[CLUSTER] Error saving private array to disk: %v\n", err)
						IncrementErrorCount()
					}
				} else {
					if err := c.saveArrayToDisk(arrayName); err != nil {
						log.Printf("[CLUSTER] Error saving array to disk: %v\n", err)
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

			if isEmpty {
				delete(c.timestamps, arrayName)
			} else {
				c.timestamps[arrayName] = time.Now()
			}

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
	delete(c.timestamps, arrayName)

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

			if isEmpty {
				delete(c.timestamps, arrayName)
			} else {
				c.timestamps[arrayName] = time.Now()
			}

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
	delete(c.timestamps, arrayName)

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
	peerBootups := make(map[string]string)
	for peer, bootTime := range c.peerBootupTimes {
		peerBootups[peer] = bootTime.Format(time.RFC3339)
	}

	// Determine cluster health status
	var health string
	peerCount := len(c.peers)
	if peerCount == 0 {
		health = "disabled"
	} else {
		responsivePeers := len(c.peerBootupTimes)
		if responsivePeers == peerCount {
			health = "healthy"
		} else if responsivePeers > 0 {
			health = "degraded"
		} else {
			health = "degraded"
		}
	}

	return map[string]interface{}{
		"bootup_time":       c.bootupTime.Format(time.RFC3339),
		"uptime_seconds":    int64(time.Since(c.bootupTime).Seconds()),
		"last_sync":         c.lastSyncTime.Format(time.RFC3339),
		"total_syncs":       c.syncCount,
		"failed_syncs":      c.failedSyncs,
		"total_sync_bytes":  c.totalSyncBytes,
		"error_count":       c.errorCount,
		"peer_count":        len(c.peers),
		"peers":             c.peers,
		"peer_bootup_times": peerBootups,
		"health":            health,
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

	// Track which arrays need to be saved
	arraysToBeSaved := []string{}

	for arrayName, array := range data {
		peerTimestamp := timestamps[arrayName]
		localTimestamp := c.timestamps[arrayName]

		// If array is empty locally or peer data is newer, update it
		if len(c.data[arrayName]) == 0 || peerTimestamp.After(localTimestamp) {
			if c.data[arrayName] == nil {
				c.data[arrayName] = make(map[string]string)
			}
			for k, v := range array {
				c.data[arrayName][k] = v
			}

			// Only set timestamp if array is not empty
			if len(array) > 0 {
				c.timestamps[arrayName] = peerTimestamp
				arraysToBeSaved = append(arraysToBeSaved, arrayName)
			}
		}
	}

	log.Printf("[CLUSTER] Merged data from peer\n")

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Save to disk asynchronously (without lock)
	for _, arrayName := range arraysToBeSaved {
		c.queueSave(arrayName)
	}
}

// UpdateFromPeerPrivate merges private data from a peer based on timestamps
func (c *Cluster) UpdateFromPeerPrivate(data map[string]map[string]string, timestamps map[string]time.Time) {
	c.mu.Lock()

	// Track which arrays need to be saved
	arraysToBeSaved := []string{}

	for arrayName, array := range data {
		peerTimestamp := timestamps[arrayName]
		localTimestamp := c.timestamps[arrayName]

		// If array is empty locally or peer data is newer, update it
		if len(c.privateData[arrayName]) == 0 || peerTimestamp.After(localTimestamp) {
			if c.privateData[arrayName] == nil {
				c.privateData[arrayName] = make(map[string]string)
			}
			for k, v := range array {
				c.privateData[arrayName][k] = v
			}

			// Only set timestamp if array is not empty
			if len(array) > 0 {
				c.timestamps[arrayName] = peerTimestamp
				arraysToBeSaved = append(arraysToBeSaved, arrayName)
			}
		}
	}

	log.Printf("[CLUSTER] Merged private data from peer\n")

	// Release lock BEFORE I/O operations
	c.mu.Unlock()

	// Save to disk asynchronously (without lock)
	for _, arrayName := range arraysToBeSaved {
		c.queueSavePrivate(arrayName)
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
					log.Printf("[CLUSTER] Retrying sync from peer %s in %v (attempt %d/%d)\n", peerAddr, delay, attempt+1, maxRetries)
					time.Sleep(delay)
				}

				conn, err := net.DialTimeout("tcp", peerAddr+":9000", 5*time.Second)
				if err != nil {
					if attempt == maxRetries-1 {
						log.Printf("[CLUSTER] Failed to sync from peer %s after %d attempts: %v\n", peerAddr, maxRetries, err)
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
					log.Printf("[CLUSTER] Authentication failed with peer %s\n", peerAddr)
					continue
				}

				fmt.Fprintf(writer, "GET_ARRAYS\n")
				writer.Flush()

				line, err := reader.ReadString('\n')
				if err != nil {
					log.Printf("[CLUSTER] Error reading from peer %s: %v\n", peerAddr, err)
					continue
				}

				// Decrypt the response
				jsonStr, err := c.decryptMessage(strings.TrimSpace(line))
				if err != nil {
					log.Printf("[CLUSTER] Decryption failed: %v\n", err)
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
					log.Printf("[CLUSTER] Error unmarshaling peer data: %v\n", err)
					continue
				}

				// Store peer's bootup time
				if !payload.BootupTime.IsZero() {
					c.mu.Lock()
					c.peerBootupTimes[peerAddr] = payload.BootupTime
					c.mu.Unlock()
				}

				c.UpdateFromPeer(payload.Data, payload.Timestamps)
				c.UpdateFromPeerPrivate(payload.PrivateData, payload.Timestamps)
				log.Printf("[CLUSTER] Successfully synced from peer %s on startup\n", peerAddr)
				return // Success, exit retry loop
			}
		}(peer)
	}

	// Start periodic background sync to keep in sync with peers
	go c.periodicPeerSync()
}

// periodicPeerSync runs a background task to periodically sync with peers
func (c *Cluster) periodicPeerSync() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		for _, peer := range clusterArray.peers {
			go func(peerAddr string) {
				conn, err := net.DialTimeout("tcp", peerAddr+":9000", 5*time.Second)
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
					c.mu.Unlock()
				}

				c.UpdateFromPeer(payload.Data, payload.Timestamps)
				c.UpdateFromPeerPrivate(payload.PrivateData, payload.Timestamps)
			}(peer)
		}
	}
}

// startPeerSyncServer starts a TCP server for peer synchronization
func (c *Cluster) startPeerSyncServer(port string) {
	addr := c.listenAddr + ":" + port
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[CLUSTER] Error starting peer sync server: %v\n", err)
		return
	}
	defer listener.Close()

	log.Printf("[CLUSTER] Peer sync server started on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[CLUSTER] Error accepting connection: %v\n", err)
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
		log.Printf("[CLUSTER] Rejected connection from %s: IP not whitelisted\n", conn.RemoteAddr())
		return
	}

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Step 1: Receive and verify peer secret
	secretLine, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[CLUSTER] Error reading peer secret: %v\n", err)
		return
	}

	secretLine = strings.TrimSpace(secretLine)
	if secretLine != c.peerSecret {
		log.Printf("[CLUSTER] Invalid peer secret from %s\n", conn.RemoteAddr())
		fmt.Fprintf(writer, "AUTH_FAILED\n")
		writer.Flush()
		return
	}

	// Authentication successful
	fmt.Fprintf(writer, "AUTH_OK\n")
	writer.Flush()
	log.Printf("[CLUSTER] Peer authenticated from %s\n", conn.RemoteAddr())

	// Step 2: Read command
	line, err := reader.ReadString('\n')
	if err != nil {
		log.Printf("[CLUSTER] Error reading command from peer: %v\n", err)
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
			log.Printf("[CLUSTER] Encryption failed: %v\n", err)
			return
		}

		fmt.Fprintf(writer, "%s\n", encrypted)
		writer.Flush()
		log.Printf("[CLUSTER] Sent encrypted arrays to peer\n")
	} else if strings.HasPrefix(line, "UPDATE ") {
		encryptedData := strings.TrimPrefix(line, "UPDATE ")

		// Decrypt the update
		jsonStr, err := c.decryptMessage(encryptedData)
		if err != nil {
			log.Printf("[CLUSTER] Decryption failed: %v\n", err)
			return
		}

		var payload struct {
			Data        map[string]map[string]string `json:"data"`
			PrivateData map[string]map[string]string `json:"private_data"`
			Timestamps  map[string]time.Time         `json:"timestamps"`
			BootupTime  time.Time                    `json:"bootup_time"`
		}

		if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
			log.Printf("[CLUSTER] Error unmarshaling update data: %v\n", err)
			return
		}

		// Store peer's bootup time
		if !payload.BootupTime.IsZero() {
			c.mu.Lock()
			c.peerBootupTimes[conn.RemoteAddr().String()] = payload.BootupTime
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
				conn, err := net.DialTimeout("tcp", peerAddr+":9000", 5*time.Second)
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
					log.Printf("[CLUSTER] Authentication failed with peer %s\n", peerAddr)
					c.mu.Lock()
					c.failedSyncs++
					c.mu.Unlock()
					return
				}

				// Encrypt the update
				encrypted, err := c.encryptMessage(string(jsonData))
				if err != nil {
					log.Printf("[CLUSTER] Encryption failed: %v\n", err)
					c.mu.Lock()
					c.failedSyncs++
					c.mu.Unlock()
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
		log.Printf("[CLUSTER] Cleared all existing arrays before import\n")
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

	log.Printf("[CLUSTER] Imported %d public arrays and %d private arrays\n", len(dataMap), len(privateDataMap))
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
	c.wsSubMu.RLock()
	subscribers, ok := c.wsSubscriptions[arrayName]
	c.wsSubMu.RUnlock()

	if !ok || len(subscribers) == 0 {
		return // No subscribers for this array
	}

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

	// Send update to all subscribers (non-blocking)
	for _, ch := range subscribers {
		select {
		case ch <- update:
			// Sent successfully
		default:
			// Channel full, skip to avoid blocking
			log.Printf("[WS] Dropped update for subscriber (channel full)\n")
		}
	}
}
