package main

import (
	"bufio"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/joho/godotenv"

	"cluster/modules"
)

// Config represents the configuration from .env
type Config struct {
	RuntimePath          string
	PersistentBackupPath string
	AutoClearRuntimes    bool
	AutoBackupEnabled    bool
	AdminPassword        string
	LogLevel             string
	LogFile              string
	AuditLogFile         string
	ClusterPeers         string
	ClusterDefaultPort   int
}

// Global variables
var (
	db             *modules.Database
	replManager    *modules.ReplicationManager
	tokenManager   *modules.TokenManager
	httpServer     *modules.HTTPServer
	clusterManager *modules.ClusterManager
	peerServer     *modules.PeerServer
	logger         *modules.Logger
	config         *Config
	socketListener net.Listener
)

const socketPath = "/tmp/tcpclusterd.sock"

var (
	ErrSocketUnavailable         = errors.New("server socket unavailable")
	ErrCommandRequiresLocalScope = errors.New("command requires local handling")
)

func loadConfig() *Config {
	cfg := &Config{
		RuntimePath:          generateRuntimePath(),
		PersistentBackupPath: "backups",
		AutoClearRuntimes:    parseEnvBool(os.Getenv("AUTO_CLEAR_RUNTIMES"), false),
		AutoBackupEnabled:    parseEnvBool(os.Getenv("AUTO_BACKUP"), true),
		LogLevel:             getEnvOrDefault("LOG_LEVEL", "info"),
		LogFile:              getEnvOrDefault("LOG_FILE", "logs/app.log"),
		AuditLogFile:         getEnvOrDefault("AUDIT_LOG_FILE", "logs/audit.log"),
		ClusterPeers:         getEnvOrDefault("CLUSTER_PEERS", ""),
		AdminPassword:        getEnvOrDefault("ADMIN_PASSWORD", "admin"),
	}

	// Parse cluster default port
	if portStr := os.Getenv("CLUSTER_DEFAULT_PORT"); portStr != "" {
		if port, err := strconv.Atoi(portStr); err == nil {
			cfg.ClusterDefaultPort = port
		} else {
			cfg.ClusterDefaultPort = 5000
		}
	} else {
		cfg.ClusterDefaultPort = 5000
	}

	// Parse paths
	if rp := os.Getenv("RUNTIME_PATH"); rp != "" {
		cfg.RuntimePath = rp
	}

	if bp := os.Getenv("PERSISTENT_BACKUP_PATH"); bp != "" {
		cfg.PersistentBackupPath = bp
	}

	return cfg
}

func generateRuntimePath() string {
	return filepath.Join("runtime", time.Now().Format("2006-01-02_15-04-05"))
}

// generateRandomPassword generates a random password of specified length
func generateRandomPassword(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@#$%^&*"
	password := make([]byte, length)
	for i := range password {
		num, _ := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		password[i] = charset[num.Int64()]
	}
	return string(password)
}

// findLatestRuntimePath finds the most recent runtime folder
func findLatestRuntimePath() string {
	runtimeDir := "runtime"
	entries, err := os.ReadDir(runtimeDir)
	if err != nil {
		return "" // No runtime directory exists
	}

	var latestPath string
	var latestTime time.Time

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Parse timestamp from folder name (format: 2006-01-02_15-04-05)
		t, err := time.Parse("2006-01-02_15-04-05", entry.Name())
		if err != nil {
			continue // Skip folders that don't match the format
		}
		if t.After(latestTime) {
			latestTime = t
			latestPath = filepath.Join(runtimeDir, entry.Name())
		}
	}

	return latestPath
}

func parseEnvBool(value string, defaultValue bool) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return defaultValue
	}
	return v == "true" || v == "1" || v == "yes" || v == "on"
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

func main() {
	// Load .env file FIRST, before any config parsing
	if err := godotenv.Load(); err != nil {
		log.Printf("Note: .env file not found or error loading: %v", err)
	}

	// NOW parse configuration from environment
	config = loadConfig()

	// Initialize logger (must be before any logging)
	modules.InitLogger(config.LogLevel, config.LogFile, config.AuditLogFile)
	logger = modules.GlobalLogger

	// Parse command line arguments FIRST to check if this is a CLI command
	addUser := flag.String("add", "", "Add a new user and create their personal database (usage: --add username)")
	password := flag.String("password", "", "Password for user (if not provided, a random one will be generated)")
	clearRuntimes := flag.Bool("clearruntimes", false, "Clear old runtime folders")
	export := flag.Bool("export", false, "Export database and exit (usage: --export [dbname])")
	importFile := flag.String("import", "", "Import database from file")
	importClear := flag.Bool("clear", false, "Use with --import to clear current runtime data before import")
	lastRuntime := flag.Bool("lastruntime", false, "Load database from last runtime folder and start server")
	exportLastRuntime := flag.Bool("exportlastruntime", false, "Export database from last runtime folder and exit")
	importPersistent := flag.Bool("importpersistent", false, "Load database from persistent backup and start server")
	listUsers := flag.Bool("listusers", false, "List all users and exit")
	listServices := flag.Bool("list-services", false, "List service configurations and exit")
	listCluster := flag.Bool("list-cluster", false, "List cluster peers from CLUSTER_PEERS and exit")
	peerMetrics := flag.Bool("peer-metrics", false, "Show peer metrics (usage: --peer-metrics [peer_address])")
	removeUser := flag.String("remove", "", "Remove a user and delete their personal database (usage: --remove username)")
	flushTokens := flag.Bool("flushtokens", false, "Flush all authentication tokens and exit")
	configHTTP := flag.String("config-http", "", "Configure HTTP service (usage: --config-http enabled=true port=9090 host=0.0.0.0 tls=false)")
	configTCP := flag.String("config-tcp", "", "Configure TCP service (usage: --config-tcp enabled=true port=5000 host=0.0.0.0)")
	configWS := flag.String("config-ws", "", "Configure WebSocket service (usage: --config-ws enabled=true port=8080 host=0.0.0.0). Client controls polling interval.")
	addCluster := flag.String("add-cluster", "", "Deprecated: cluster peers are configured via CLUSTER_PEERS in .env")
	removeCluster := flag.String("remove-cluster", "", "Deprecated: cluster peers are configured via CLUSTER_PEERS in .env")
	flag.Parse()

	// Get optional arguments for export and peer-metrics
	var exportDbName string
	if *export && flag.NArg() > 0 {
		exportDbName = flag.Arg(0)
	}

	var peerMetricsAddr string
	if *peerMetrics && flag.NArg() > 0 {
		peerMetricsAddr = flag.Arg(0)
	}

	// Check if this is a CLI command (not starting server)
	isCLICommand := *export || *exportLastRuntime || *importFile != "" || *lastRuntime || *importPersistent || *listUsers || *listServices || *listCluster || *addUser != "" || *removeUser != "" || *flushTokens || *configHTTP != "" || *configTCP != "" || *configWS != "" || *peerMetrics || *addCluster != "" || *removeCluster != ""

	// Print version only when starting server (no arguments)
	if !isCLICommand {
		fmt.Println("tcpclusterd version 1.0.4")
	}

	// If CLI command, try to use socket connection first (server is running)
	// This MUST be done BEFORE initializing cluster manager
	if isCLICommand {
		// Try socket connection first
		err := handleCLIViaSocket(*export, exportDbName, *exportLastRuntime, *importFile, *importClear, *lastRuntime, *importPersistent, *listUsers, *listServices, *listCluster, *addUser, *removeUser, *flushTokens, *configHTTP, *configTCP, *configWS, *peerMetrics, peerMetricsAddr, *addCluster, *removeCluster, *password)
		if err == nil {
			os.Exit(0)
		}

		// Only fall back to local operations when socket is unavailable
		// or command explicitly requires local handling.
		if errors.Is(err, ErrSocketUnavailable) || errors.Is(err, ErrCommandRequiresLocalScope) {
			logger.Info("Socket unavailable or command is local-only, falling back to local operations: %v", err)
		} else {
			logger.Error("CLI command failed via running server: %v", err)
			os.Exit(1)
		}
	}

	// NOW initialize cluster manager (only needed for server mode)
	var err error
	if config.ClusterPeers != "" {
		clusterManager, err = modules.NewClusterManager(config.ClusterPeers, logger)
		if err != nil {
			logger.Error("Failed to initialize cluster manager: %v", err)
			os.Exit(1)
		}
	} else {
		logger.Warn("No CLUSTER_PEERS configured - running in single-server mode")
	}

	// Handle CLI commands that don't need socket

	// Handle clear runtimes
	if *clearRuntimes || config.AutoClearRuntimes {
		clearOldRuntimes()
		if *clearRuntimes {
			os.Exit(0)
		}
	}

	// For server startup, create new timestamped runtime path
	runtimePath := config.RuntimePath

	// Initialize database with appropriate runtime path
	db = modules.NewDatabase(runtimePath, config.PersistentBackupPath, config.AutoClearRuntimes, config.AutoBackupEnabled)

	// Try to load existing runtime
	dbErr := db.LoadRuntimeSnapshot()
	if dbErr != nil {
		log.Printf("Warning: Failed to load runtime: %v", dbErr)
		// Initialize with default config if no runtime exists
		if initErr := db.InitializeDatabase(config.AdminPassword); initErr != nil {
			log.Fatalf("Failed to initialize database: %v", initErr)
		}
	}

	// Initialize database schema for services
	if schemaErr := modules.InitializeDatabaseSchema(db); schemaErr != nil {
		log.Printf("Warning: Failed to initialize schema: %v", schemaErr)
	}

	// If no data was loaded, initialize with admin user
	if len(db.GetAllDatabases()) == 0 {
		log.Println("Initializing new database with admin user...")
		if err := db.InitializeDatabase(config.AdminPassword); err != nil {
			log.Fatalf("Failed to initialize database: %v", err)
		}
	}

	// Handle WebSocket config
	if *configWS != "" {
		configureServiceCLI("websocket", *configWS)
		os.Exit(0)
	}

	// Handle add cluster server
	if *addCluster != "" {
		log.Println("add-cluster is deprecated. Edit CLUSTER_PEERS in .env on all nodes.")
		os.Exit(0)
	}

	// Handle remove cluster server
	if *removeCluster != "" {
		log.Println("remove-cluster is deprecated. Edit CLUSTER_PEERS in .env on all nodes.")
		os.Exit(0)
	}

	// Start server
	startServer()
}

func clearOldRuntimes() {
	runtimeBase := "runtime"
	entries, err := os.ReadDir(runtimeBase)
	if err != nil {
		log.Printf("Failed to read runtime directory: %v", err)
		return
	}

	retention := 30 * 24 * time.Hour
	now := time.Now()
	currentRuntimeAbs, _ := filepath.Abs(filepath.Clean(config.RuntimePath))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		folderName := strings.TrimSpace(entry.Name())
		parsedTime, parseErr := time.Parse("2006-01-02_15-04-05", folderName)
		if parseErr != nil {
			continue
		}
		if now.Sub(parsedTime) <= retention {
			continue
		}

		path := filepath.Join(runtimeBase, folderName)
		pathAbs, _ := filepath.Abs(filepath.Clean(path))
		if currentRuntimeAbs != "" && pathAbs == currentRuntimeAbs {
			continue
		}

		if err := os.RemoveAll(path); err != nil {
			log.Printf("Failed to remove runtime folder %s: %v", path, err)
		} else {
			log.Printf("Removed old runtime folder (>30d): %s", path)
		}
	}
}

func importDatabase(filename string) error {
	data, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("failed to read file: %w", err)
	}

	return db.ImportFromJSON(data)
}

func exportDatabase(dbname string) error {
	var data []byte
	var err error

	if dbname == "" {
		// Export all databases
		data, err = db.ExportToJSON()
	} else {
		// Export specific database
		data, err = db.ExportToJSONDatabase(dbname)
	}

	if err != nil {
		return err
	}

	// Output to stdout so user can redirect with >
	fmt.Println(string(data))
	return nil
}

func listAllUsers() {
	fmt.Println("=== System Users ===")

	// Query system.users table
	users, err := db.SelectRows("system", "users", make(map[string]string), true)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if userList, ok := users.([]interface{}); ok {
		if len(userList) == 0 {
			fmt.Println("No users found")
			return
		}

		for _, u := range userList {
			if userMap, ok := u.(map[string]interface{}); ok {
				fmt.Printf("Username: %v\n", userMap["username"])
				fmt.Printf("  Is Admin: %v\n", userMap["is_admin"])
				fmt.Println()
			}
		}
	}
}

func addNewUser(username string) {
	// Prompt for password
	fmt.Print("Enter password for new user: ")
	var password string
	fmt.Scanln(&password)

	if password == "" {
		fmt.Println("Password cannot be empty")
		os.Exit(1)
	}

	if err := db.AddUser(username, password); err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("User '%s' created successfully with personal database\n", username)

	// Save changes
	if err := db.SaveRuntimeSnapshot(); err != nil {
		log.Printf("Warning: Failed to save runtime: %v", err)
	}
}

func removeUserCmd(username string) {
	if username == "admin" {
		fmt.Println("Cannot remove admin user")
		os.Exit(1)
	}

	// This would need to be implemented in the Database module
	fmt.Println("User removal not yet implemented")
	os.Exit(1)
}

func startServer() {
	// Get local peer address if cluster is configured
	var localPeer string
	if clusterManager != nil {
		peer := clusterManager.GetLocalPeer()
		if peer != nil {
			localPeer = peer.Address
			logger.Info("Starting cluster node at %s", localPeer)
		}
	}

	// Initialize replication manager
	replToken := strings.TrimSpace(getEnvOrDefault("REPLICATION_TOKEN", ""))
	replManager = modules.NewReplicationManager(db, nil, replToken)

	// Initialize token manager with AES key from environment or use default
	aesKey := getEnvOrDefault("AES_KEY", "default-secret-key-for-jwt-tokens")
	var err error
	tokenManager, err = modules.NewTokenManager(db, aesKey, replManager)
	if err != nil {
		log.Fatalf("Failed to initialize token manager: %v", err)
	}

	// Initialize peer server for cluster communication (if cluster configured)
	if clusterManager != nil && localPeer != "" {
		var peerErr error
		peerServer, peerErr = modules.NewPeerServer(localPeer, clusterManager, db, logger, executeReplicatedWrite)
		if peerErr != nil {
			logger.Error("Failed to initialize peer server: %v", peerErr)
		} else {
			logger.Info("Peer server started for cluster communication")

			// Initialize peer metrics for all configured peers
			for _, peer := range clusterManager.GetPeers() {
				if err := db.UpdatePeerMetrics(peer.Address, false, time.Now(), 0, 0, 0); err != nil {
					logger.Warn("Failed to initialize metrics for peer %s: %v", peer.Address, err)
				}
			}
		}
	}

	// Initialize HTTP server (before peer server, just handles local queries)
	wsServer := modules.NewWebSocketServer(db, tokenManager, 30)
	httpServer = modules.NewHTTPServer(db, replManager, tokenManager, wsServer, nil, nil, replToken)
	httpServer.RegisterHandlers()

	// Initialize service manager for dynamic service configuration
	serviceManager := modules.NewServiceManager(db, localPeer)
	if err := serviceManager.LoadConfigFromDatabase(); err != nil {
		logger.Warn("Failed to load service config: %v", err)
	}

	// Start Unix socket server for CLI commands (always on)
	startSocketServer()
	log.Println("Socket server started (always-on)")

	// Start periodic database saving
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for range ticker.C {
			if err := db.SaveRuntimeSnapshot(); err != nil {
				log.Printf("Failed to save database snapshot: %v", err)
			}
		}
	}()

	// Start periodic heartbeat updater
	// Cluster heartbeat is now handled by peer server

	// Start periodic service config watcher
	go watchServiceConfig(serviceManager)

	// Start periodic retry worker for failed cluster writes
	if clusterManager != nil && peerServer != nil {
		go retryFailedClusterWrites()
	}

	// Setup graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("Runtime path: %s", config.RuntimePath)
	log.Printf("Databases: %v", db.GetAllDatabases())
	log.Println("Server started with socket-only mode. Services will be dynamically loaded from database.")

	// Wait for shutdown signal
	<-sigChan
	log.Println("Shutting down...")

	// Final save before exit
	if err := db.SaveRuntimeSnapshot(); err != nil {
		log.Printf("Failed to save database on shutdown: %v", err)
	}

	// Close socket listener
	if socketListener != nil {
		socketListener.Close()
		os.Remove(socketPath)
	}

	// Clean up replication manager
	replManager.Close()
	if peerServer != nil {
		peerServer.Close()
	}
	if logger != nil {
		logger.Close()
	}

	log.Println("Shutdown complete")
}

func retryFailedClusterWrites() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		if clusterManager == nil || peerServer == nil {
			continue
		}

		failed := clusterManager.GetFailedEntries()
		if len(failed) == 0 {
			continue
		}

		logger.Warn("Retrying %d failed cluster writes", len(failed))
		for _, entry := range failed {
			op := &modules.WriteOperation{
				ID:         entry.ID,
				Operation:  entry.Operation,
				Data:       entry.Data,
				Timestamp:  entry.Timestamp.Unix(),
				SourcePeer: "retry",
				IsPrivate:  true,
			}
			if err := peerServer.ReplicateWriteAndWait(op, 10*time.Second); err != nil {
				logger.Warn("Retry failed for %s: %v", entry.ID, err)
			} else {
				logger.Info("Retry succeeded for %s", entry.ID)
			}
		}
	}
}

// flushAuthTokens clears all authentication tokens from the database
func flushAuthTokens() {
	// Initialize database to flush tokens
	db = modules.NewDatabase(config.RuntimePath, config.PersistentBackupPath, config.AutoClearRuntimes, config.AutoBackupEnabled)

	// Load existing runtime
	err := db.LoadRuntimeSnapshot()
	if err != nil {
		log.Printf("Warning: Failed to load runtime: %v", err)
	}

	// Initialize token manager
	aesKey := getEnvOrDefault("AES_KEY", "default-secret-key-for-jwt-tokens")
	var err2 error
	tokenManager, err2 = modules.NewTokenManager(db, aesKey, nil)
	if err2 != nil {
		log.Fatalf("Failed to initialize token manager: %v", err2)
	}

	// Flush all tokens
	tokenManager.FlushTokens()

	// Save the database
	if err := db.SaveRuntimeSnapshot(); err != nil {
		log.Fatalf("Failed to save database after flushing tokens: %v", err)
	}

	log.Println("All authentication tokens have been flushed")
}

// Example API usage demonstrating the new SQL interface
func demonstrateAPI() {
	fmt.Println()
	fmt.Println("=== SQL API Examples ===")
	fmt.Println()

	examples := []string{
		"INSERT PRIVATE INTO testdb.users (username, email) VALUES ('john', 'john@example.com')",
		"SELECT * FROM testdb.users WHERE username='john'",
		"INSERT PUBLIC INTO testdb.posts (title, content) VALUES ('Hello', 'This is a post')",
		"DELETE FROM testdb.posts WHERE title='Hello'",
	}

	for _, example := range examples {
		fmt.Printf("Query: %s\n", example)
		fmt.Println()
	}
}

// Socket communication structures
type SocketRequest struct {
	Command string                 `json:"command"`
	Data    map[string]interface{} `json:"data,omitempty"`
}

type SocketResponse struct {
	Success bool                   `json:"success"`
	Message string                 `json:"message,omitempty"`
	Data    map[string]interface{} `json:"data,omitempty"`
}

// isServerRunning checks if server is already running by trying to connect to socket
func isServerRunning() bool {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

// handleCLIViaSocket handles all CLI commands by connecting to the running server socket
func handleCLIViaSocket(export bool, exportDbName string, exportLastRuntime bool, importFile string, importClear bool, lastRuntime bool, importPersistent bool, listUsers bool, listServices bool, listCluster bool, addUser string, removeUser string, flushTokens bool, configHTTP string, configTCP string, configWS string, peerMetrics bool, peerMetricsAddr string, addCluster string, removeCluster string, password string) error {
	// Handle export
	if export {
		req := SocketRequest{
			Command: "export",
			Data:    map[string]interface{}{"dbname": exportDbName},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("export failed: %s", resp.Message)
		}
		if jsonStr, ok := resp.Data["json"].(string); ok {
			fmt.Println(jsonStr)
		}
		return nil
	}

	// Handle list users
	if listUsers {
		req := SocketRequest{Command: "listusers"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("listusers failed: %s", resp.Message)
		}
		if users, ok := resp.Data["users"]; ok {
			data, _ := json.MarshalIndent(users, "", "  ")
			fmt.Println(string(data))
		}
		return nil
	}

	// Handle list services
	if listServices {
		req := SocketRequest{Command: "listservices"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("listservices failed: %s", resp.Message)
		}
		if services, ok := resp.Data["services"]; ok {
			data, _ := json.MarshalIndent(services, "", "  ")
			fmt.Println(string(data))
		}
		return nil
	}

	// Handle list cluster
	if listCluster {
		req := SocketRequest{Command: "listcluster"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("listcluster failed: %s", resp.Message)
		}
		if servers, ok := resp.Data["servers"]; ok {
			data, _ := json.MarshalIndent(servers, "", "  ")
			fmt.Println(string(data))
		}
		return nil
	}

	// Handle add user
	if addUser != "" {
		// Generate password if not provided
		userPassword := password
		if userPassword == "" {
			userPassword = generateRandomPassword(16)
		}
		req := SocketRequest{
			Command: "adduser",
			Data: map[string]interface{}{
				"username": addUser,
				"password": userPassword,
			},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("adduser failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		if password == "" {
			fmt.Printf("Generated password: %s\n", userPassword)
		}
		return nil
	}

	// Handle remove user
	if removeUser != "" {
		req := SocketRequest{
			Command: "removeuser",
			Data:    map[string]interface{}{"username": removeUser},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("removeuser failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle flush tokens
	if flushTokens {
		req := SocketRequest{Command: "flushtokens"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("flushtokens failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle HTTP config
	if configHTTP != "" {
		req := SocketRequest{
			Command: "configure",
			Data: map[string]interface{}{
				"service": "http",
				"config":  configHTTP,
			},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("HTTP config failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle TCP config
	if configTCP != "" {
		req := SocketRequest{
			Command: "configure",
			Data: map[string]interface{}{
				"service": "tcp",
				"config":  configTCP,
			},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("TCP config failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle WebSocket config
	if configWS != "" {
		req := SocketRequest{
			Command: "configure",
			Data: map[string]interface{}{
				"service": "ws",
				"config":  configWS,
			},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("WebSocket config failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle peer metrics
	if peerMetrics {
		req := SocketRequest{
			Command: "peermetrics",
		}
		if peerMetricsAddr != "" {
			req.Data = map[string]interface{}{"peer_address": peerMetricsAddr}
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("peermetrics failed: %s", resp.Message)
		}
		if metric, ok := resp.Data["metric"]; ok {
			data, _ := json.MarshalIndent(metric, "", "  ")
			fmt.Println(string(data))
		} else if metrics, ok := resp.Data["metrics"]; ok {
			data, _ := json.MarshalIndent(metrics, "", "  ")
			fmt.Println(string(data))
		}
		return nil
	}

	// Handle exportlastruntime via socket (exports previous runtime database)
	if exportLastRuntime {
		req := SocketRequest{Command: "exportlastruntime"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("exportlastruntime failed: %s", resp.Message)
		}
		if data, ok := resp.Data["export"]; ok {
			jsonData, _ := json.MarshalIndent(data, "", "  ")
			fmt.Println(string(jsonData))
		}
		return nil
	}

	// Handle lastruntime via socket
	if lastRuntime {
		req := SocketRequest{Command: "lastruntime"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("lastruntime failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle import via socket
	if importFile != "" {
		data, err := os.ReadFile(importFile)
		if err != nil {
			return fmt.Errorf("failed to read import file: %v", err)
		}
		req := SocketRequest{
			Command: "import",
			Data: map[string]interface{}{
				"json":  string(data),
				"clear": importClear,
			},
		}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("import failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// Handle importpersistent via socket
	if importPersistent {
		req := SocketRequest{Command: "importpersistent"}
		resp, err := sendSocketCommand(req)
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf("importpersistent failed: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return nil
	}

	// add-cluster and remove-cluster are deprecated
	if addCluster != "" || removeCluster != "" {
		fmt.Println("add-cluster and remove-cluster are deprecated. Edit CLUSTER_PEERS in .env on all nodes.")
		return nil
	}

	return fmt.Errorf("no valid CLI command")
}

// sendSocketCommand sends a command to the running server via Unix socket
func sendSocketCommand(req SocketRequest) (*SocketResponse, error) {
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSocketUnavailable, err)
	}
	defer conn.Close()

	// Send request
	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(req); err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Receive response
	var resp SocketResponse
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&resp); err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	return &resp, nil
}

// startSocketServer starts the Unix domain socket server for CLI commands
func startSocketServer() {
	// Remove existing socket file if it exists
	os.Remove(socketPath)

	var err error
	socketListener, err = net.Listen("unix", socketPath)
	if err != nil {
		log.Fatalf("Failed to create Unix socket: %v", err)
	}

	log.Printf("Socket server listening on %s", socketPath)

	go func() {
		for {
			conn, err := socketListener.Accept()
			if err != nil {
				// Check if listener was closed
				select {
				case <-time.After(10 * time.Millisecond):
					log.Printf("Socket accept error: %v", err)
					continue
				default:
					return
				}
			}
			go handleSocketConnection(conn)
		}
	}()
}

// handleSocketConnection handles incoming socket connections for CLI commands
func handleSocketConnection(conn net.Conn) {
	defer conn.Close()

	var req SocketRequest
	decoder := json.NewDecoder(conn)
	if err := decoder.Decode(&req); err != nil {
		if err.Error() != "EOF" {
			log.Printf("Failed to decode socket request: %v", err)
		}
		return
	}

	resp := executeSocketCommand(req)

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(resp); err != nil {
		log.Printf("Failed to encode socket response: %v", err)
	}
}

// executeSocketCommand executes commands received via socket
func executeSocketCommand(req SocketRequest) SocketResponse {
	switch req.Command {
	case "export":
		dbname, _ := req.Data["dbname"].(string)
		exported, err := exportDatabaseJSON(dbname, true)
		if err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"json": exported},
		}

	case "listusers":
		// Query system.users table
		users, err := db.SelectRows("system", "users", make(map[string]string), true)
		if err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"users": users},
		}

	case "listservices":
		// Query system.services_config table
		services, err := db.SelectRows("system", "services_config", make(map[string]string), true)
		if err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"services": services},
		}

	case "listcluster":
		servers := getClusterPeersView()
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"servers": servers},
		}

	case "peermetrics":
		// Get metrics for specific peer or all peers
		peerAddr, _ := req.Data["peer_address"].(string)
		if peerAddr != "" {
			metric, err := db.GetPeerMetric(peerAddr)
			if err != nil {
				return SocketResponse{Success: false, Message: err.Error()}
			}
			return SocketResponse{
				Success: true,
				Data:    map[string]interface{}{"metric": metric},
			}
		}
		// Get all metrics and filter to only configured peers
		metrics, err := db.GetPeerMetrics()
		if err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		// Filter to only show configured cluster peers (those with standard replication port 5000)
		filtered := filterConfiguredPeers(metrics, config)
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"metrics": filtered},
		}

	case "addcluster":
		return SocketResponse{Success: false, Message: "addcluster is deprecated: edit CLUSTER_PEERS in .env on all nodes"}

	case "removecluster":
		return SocketResponse{Success: false, Message: "removecluster is deprecated: edit CLUSTER_PEERS in .env on all nodes"}

	case "adduser":
		username, _ := req.Data["username"].(string)
		password, _ := req.Data["password"].(string)
		if username == "" || password == "" {
			return SocketResponse{Success: false, Message: "username and password required"}
		}
		if err := applyClusterWrite("add_user", map[string]interface{}{
			"username": username,
			"password": password,
		}); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{Success: true, Message: "✓ User '" + username + "' and database created successfully"}

	case "removeuser":
		username, _ := req.Data["username"].(string)
		if username == "" || username == "admin" {
			return SocketResponse{Success: false, Message: "invalid username"}
		}
		if err := applyClusterWrite("remove_user", map[string]interface{}{
			"username": username,
		}); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{Success: true, Message: "✓ User '" + username + "' and database removed successfully"}

	case "flushtokens":
		if err := applyClusterWrite("flush_tokens", map[string]interface{}{}); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{Success: true, Message: "All tokens flushed"}

	case "configure":
		service, _ := req.Data["service"].(string)
		configStr, _ := req.Data["config"].(string)
		if service == "" || configStr == "" {
			return SocketResponse{Success: false, Message: "service and config required"}
		}

		// Parse config string using the same logic as parseConfigParams
		params := parseConfigParams(configStr)

		// Add service name and timestamp to params
		params["service"] = service
		params["created_at"] = time.Now().Unix()

		if err := applyClusterWrite("configure_service", params); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}

		return SocketResponse{Success: true, Message: fmt.Sprintf("Service '%s' configured successfully", service)}

	case "import":
		jsonStr, _ := req.Data["json"].(string)
		if jsonStr == "" {
			return SocketResponse{Success: false, Message: "json data required"}
		}
		clearMode, _ := req.Data["clear"].(bool)
		if err := applyClusterWrite("import", map[string]interface{}{
			"json":  jsonStr,
			"clear": clearMode,
		}); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		if clearMode {
			return SocketResponse{Success: true, Message: "✓ Data imported successfully (clear mode)"}
		}
		return SocketResponse{Success: true, Message: "✓ Data imported successfully"}

	case "import_persistent":
		backupPath, err := findLatestPersistentBackup(config.PersistentBackupPath)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("import_persistent failed: %v", err)}
		}
		data, err := os.ReadFile(backupPath)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("import_persistent failed to read backup: %v", err)}
		}
		if err := applyClusterWrite("import_persistent_data", map[string]interface{}{
			"json": string(data),
		}); err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("import_persistent failed: %v", err)}
		}
		return SocketResponse{Success: true, Message: "✓ Persistent backup imported and replicated successfully"}

	case "importpersistent":
		backupPath, err := findLatestPersistentBackup(config.PersistentBackupPath)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("importpersistent failed: %v", err)}
		}
		data, err := os.ReadFile(backupPath)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("importpersistent failed to read backup: %v", err)}
		}
		if err := applyClusterWrite("import_persistent_data", map[string]interface{}{
			"json": string(data),
		}); err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("importpersistent failed: %v", err)}
		}
		return SocketResponse{Success: true, Message: "✓ Persistent backup imported and replicated successfully"}

	case "exportlastruntime":
		lastPath := findLatestRuntimePath()
		if lastPath == "" {
			return SocketResponse{Success: false, Message: "No previous runtime found"}
		}
		// Load and export from that runtime
		data, err := os.ReadFile(lastPath + "/database.json")
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("failed to read runtime database: %v", err)}
		}
		var exportData interface{}
		if err := json.Unmarshal(data, &exportData); err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("failed to parse runtime database: %v", err)}
		}
		return SocketResponse{
			Success: true,
			Message: "✓ Last runtime database exported",
			Data: map[string]interface{}{
				"export": exportData,
			},
		}

	case "lastruntime":
		lastPath := findLatestRuntimePath()
		if lastPath == "" {
			return SocketResponse{Success: false, Message: "No previous runtime found"}
		}
		return SocketResponse{Success: true, Message: "Last runtime directory: " + lastPath}

	default:
		return SocketResponse{Success: false, Message: "unknown command"}
	}
}

func exportDatabaseJSON(dbname string, includeTokens bool) (string, error) {
	var data []byte
	var err error

	if dbname == "" {
		data, err = db.ExportToJSON()
	} else {
		data, err = db.ExportToJSONDatabase(dbname)
	}
	if err != nil {
		return "", err
	}

	return sanitizeExportJSON(string(data), includeTokens)
}

func authenticateTCPToken(token string) (string, bool, string, error) {
	if token == "" {
		return "", false, "", fmt.Errorf("authentication required: call auth first, then include token")
	}

	username, isAdmin, err := tokenManager.ValidateToken(token)
	if err != nil {
		return "", false, "", fmt.Errorf("invalid token: %v", err)
	}

	if err := tokenManager.ConsumeToken(token); err != nil {
		return "", false, "", fmt.Errorf("failed to consume token: %v", err)
	}

	nextToken, err := tokenManager.IssueToken(username, isAdmin)
	if err != nil {
		return "", false, "", fmt.Errorf("failed to issue next token: %v", err)
	}

	return username, isAdmin, nextToken, nil
}

func handleTCPSubscription(conn net.Conn, req SocketRequest) {
	if req.Data == nil {
		req.Data = map[string]interface{}{}
	}

	token, _ := req.Data["token"].(string)
	_, _, nextToken, err := authenticateTCPToken(token)
	if err != nil {
		_ = json.NewEncoder(conn).Encode(SocketResponse{Success: false, Message: err.Error()})
		return
	}

	intervalSeconds := 5
	switch value := req.Data["interval_seconds"].(type) {
	case float64:
		intervalSeconds = int(value)
	case int:
		intervalSeconds = value
	case string:
		if parsed, parseErr := strconv.Atoi(strings.TrimSpace(value)); parseErr == nil {
			intervalSeconds = parsed
		}
	}
	if intervalSeconds <= 0 {
		intervalSeconds = 5
	}

	dbname, _ := req.Data["dbname"].(string)
	scope, _ := req.Data["scope"].(string)
	includeTokens := strings.EqualFold(strings.TrimSpace(scope), "full")

	encoder := json.NewEncoder(conn)
	sendSnapshot := func(message string) error {
		payload, exportErr := exportDatabaseJSON(dbname, includeTokens)
		if exportErr != nil {
			return encoder.Encode(SocketResponse{Success: false, Message: fmt.Sprintf("failed to export database: %v", exportErr)})
		}

		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		return encoder.Encode(SocketResponse{
			Success: true,
			Message: message,
			Data: map[string]interface{}{
				"json":             payload,
				"token":            nextToken,
				"interval_seconds": intervalSeconds,
			},
		})
	}

	if err := sendSnapshot("subscription started"); err != nil {
		log.Printf("Failed to write TCP subscription response: %v", err)
		return
	}

	ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if err := sendSnapshot("subscription update"); err != nil {
			log.Printf("TCP subscription ended for %s: %v", conn.RemoteAddr(), err)
			return
		}
	}
}

// CLI command functions using socket communication
func exportDatabaseCLI(dbname string) error {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{
			Command: "export",
			Data:    map[string]interface{}{"dbname": dbname},
		})
		if err != nil {
			return err
		}
		if !resp.Success {
			return fmt.Errorf(resp.Message)
		}
		fmt.Print(resp.Data["json"])
		return nil
	}

	// No server running - start temporary one
	return runWithTempServer(func() error {
		return exportDatabase(dbname)
	})
}

func listUsersCLI() {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{Command: "listusers"})
		if err != nil {
			log.Fatalf("Failed to list users: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		users, _ := resp.Data["users"].([]interface{})
		fmt.Println("=== System Users ===")
		for _, u := range users {
			if user, ok := u.(map[string]interface{}); ok {
				fmt.Printf("- %s\n", user["username"])
			}
		}
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		listAllUsers()
		return nil
	})
}

// listServicesCLI displays the current service configurations
func listServicesCLI() {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{Command: "listservices"})
		if err != nil {
			log.Fatalf("Failed to list services: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		services, _ := resp.Data["services"].([]interface{})
		fmt.Println("=== Service Configurations ===")
		for _, svc := range services {
			if service, ok := svc.(map[string]interface{}); ok {
				displayServiceConfig(service)
			}
		}
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		listAllServices()
		return nil
	})
}

// listAllServices displays all service configurations from database
func listAllServices() {
	configs, err := db.SelectRows("system", "services_config", make(map[string]string), true)
	if err != nil {
		fmt.Println("=== Service Configurations ===")
		fmt.Println("(No services configured)")
		return
	}

	fmt.Println("=== Service Configurations ===")
	if configList, ok := configs.([]interface{}); ok {
		for _, cfg := range configList {
			if configMap, ok := cfg.(map[string]interface{}); ok {
				displayServiceConfig(configMap)
			}
		}
	}
}

// displayServiceConfig formats and displays a single service configuration
func displayServiceConfig(cfg map[string]interface{}) {
	service := cfg["service"]
	enabled := cfg["enabled"]
	port := cfg["port"]
	host := cfg["host"]

	status := "❌ disabled"
	if enabled == true {
		status = "✓ enabled"
	}

	fmt.Printf("\n%s (%s)\n", service, status)
	if host != nil && host != "" {
		fmt.Printf("  Host: %v\n", host)
	}
	if port != nil {
		fmt.Printf("  Port: %v\n", port)
	}

	// Display service-specific fields
	for k, v := range cfg {
		if k != "service" && k != "enabled" && k != "host" && k != "port" && k != "created_at" && v != "" && v != false && v != 0 {
			if k == "tls" && v == false {
				continue
			}
			if k == "tls" && v == true {
				fmt.Printf("  TLS: %v\n", v)
			} else if k != "whitelist" {
				fmt.Printf("  %s: %v\n", strings.ToTitle(strings.ReplaceAll(k, "_", " ")), v)
			} else {
				fmt.Printf("  %s: %v\n", strings.ToTitle(k), v)
			}
		}
	}
}

// listClusterCLI displays the current cluster servers
func listClusterCLI() {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{Command: "listcluster"})
		if err != nil {
			log.Fatalf("Failed to list cluster: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		servers, _ := resp.Data["servers"].([]interface{})
		fmt.Println("=== Cluster Servers ===")
		for _, srv := range servers {
			if server, ok := srv.(map[string]interface{}); ok {
				displayClusterServer(server)
			}
		}
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		listAllClusterServers()
		return nil
	})
}

// listAllClusterServers displays all cluster servers from database
func listAllClusterServers() {
	fmt.Println("=== Cluster Servers ===")
	serverList := getClusterPeersView()
	if len(serverList) == 0 {
		fmt.Println("(No cluster peers configured)")
		return
	}
	for _, srv := range serverList {
		if serverMap, ok := srv.(map[string]interface{}); ok {
			displayClusterServer(serverMap)
		}
	}
}

// displayClusterServer formats and displays a single cluster server
func displayClusterServer(srv map[string]interface{}) {
	serverID := srv["server_id"]
	host := srv["host"]
	port := srv["port"]
	status := srv["status"]

	statusIcon := "❌"
	if status == "alive" {
		statusIcon = "✓"
	}

	fmt.Printf("\n%s (%s %s)\n", serverID, statusIcon, status)
	if host != nil && host != "" {
		fmt.Printf("  Host: %v\n", host)
	}
	if port != nil && port != 0 {
		fmt.Printf("  Port: %v\n", port)
	}
	if local, ok := srv["is_local"].(bool); ok && local {
		fmt.Printf("  Local: true\n")
	}
	if connected, ok := srv["connected"]; ok {
		fmt.Printf("  Connected: %v\n", connected)
	}
	if lastHB, ok := srv["last_heartbeat"]; ok && lastHB != nil {
		fmt.Printf("  Last Heartbeat: %v\n", lastHB)
	}
}

// addClusterServerCLI adds a new cluster server
func addClusterServerCLI(configStr string) {
	_ = configStr
	fmt.Println("add-cluster is deprecated. Edit CLUSTER_PEERS in .env on all nodes.")
}

// addClusterServer adds a cluster server to the database
func addClusterServer(configStr string) error {
	_ = configStr
	return fmt.Errorf("add-cluster is deprecated: edit CLUSTER_PEERS in .env")
}

// removeClusterServerCLI removes a cluster server
func removeClusterServerCLI(serverID string) {
	_ = serverID
	fmt.Println("remove-cluster is deprecated. Edit CLUSTER_PEERS in .env on all nodes.")
}

// removeClusterServer removes a cluster server from the database
func removeClusterServer(serverID string) error {
	_ = serverID
	return fmt.Errorf("remove-cluster is deprecated: edit CLUSTER_PEERS in .env")
}

func getClusterPeersView() []interface{} {
	result := make([]interface{}, 0)

	if clusterManager == nil {
		return result
	}

	connected := map[string]bool{}
	if peerServer != nil {
		for _, p := range peerServer.GetConnectedPeers() {
			connected[p] = true
		}
	}

	for _, p := range clusterManager.GetAllPeers() {
		host := p.Address
		port := interface{}(nil)
		if h, prt, err := net.SplitHostPort(p.Address); err == nil {
			host = h
			if pInt, convErr := strconv.Atoi(prt); convErr == nil {
				port = pInt
			}
		}

		status := "unknown"
		if p.IsLocal {
			status = "alive"
		} else if connected[p.Address] {
			status = "alive"
		}

		result = append(result, map[string]interface{}{
			"server_id": p.Address,
			"host":      host,
			"port":      port,
			"status":    status,
			"is_local":  p.IsLocal,
			"connected": connected[p.Address],
		})
	}

	return result
}

func addUserCLI(username string) {
	fmt.Print("Enter password for new user: ")
	var password string
	fmt.Scanln(&password)

	if password == "" {
		log.Fatal("Password cannot be empty")
	}

	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{
			Command: "adduser",
			Data: map[string]interface{}{
				"username": username,
				"password": password,
			},
		})
		if err != nil {
			log.Fatalf("Failed to add user: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		if err := db.AddUser(username, password); err != nil {
			return err
		}
		fmt.Printf("✓ User '%s' created successfully\n", username)
		fmt.Printf("✓ Database '%s' created successfully\n", username)
		return db.SaveRuntimeSnapshot()
	})
}

func removeUserCLI(username string) {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{
			Command: "removeuser",
			Data:    map[string]interface{}{"username": username},
		})
		if err != nil {
			log.Fatalf("Failed to remove user: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		if username == "admin" {
			return fmt.Errorf("cannot remove admin user")
		}
		// Delete user from system.users table
		where := map[string]string{"username": username}
		_, err := db.DeleteRows("system", "users", where, true)
		if err != nil {
			return err
		}
		fmt.Printf("✓ User '%s' removed successfully\n", username)
		fmt.Printf("✓ Database '%s' deleted successfully\n", username)
		return db.SaveRuntimeSnapshot()
	})
}

func flushTokensCLI() {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{Command: "flushtokens"})
		if err != nil {
			log.Fatalf("Failed to flush tokens: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		flushAuthTokens()
		return nil
	})
}

// configureServiceCLI configures a service via CLI
func configureServiceCLI(service, configStr string) {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{
			Command: "configure",
			Data: map[string]interface{}{
				"service": service,
				"config":  configStr,
			},
		})
		if err != nil {
			log.Fatalf("Failed to configure service: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return
	}

	// No server running - start temporary one
	runWithTempServer(func() error {
		return configureService(service, configStr)
	})
}

// configureService parses configuration string and updates the service config
func configureService(service, configStr string) error {
	params := parseConfigParams(configStr)

	// Build the service config object
	serviceRow := map[string]interface{}{
		"service": service,
	}

	// Copy all parameters
	for k, v := range params {
		serviceRow[k] = v
	}

	// Ensure created_at is set
	if _, ok := serviceRow["created_at"]; !ok {
		serviceRow["created_at"] = time.Now().Unix()
	}

	// Use cluster write for atomic DELETE+INSERT
	if err := applyClusterWrite("configure_service", serviceRow); err != nil {
		return fmt.Errorf("failed to configure service: %w", err)
	}

	fmt.Printf("✓ Service '%s' configured successfully\n", service)
	fmt.Printf("  Configuration: %+v\n", params)
	return nil
}

// filterConfiguredPeers filters peer metrics to only show peers that are configured in CLUSTER_PEERS
// This hides ephemeral inbound connections with dynamic ports
func filterConfiguredPeers(metrics []map[string]interface{}, cfg *Config) []map[string]interface{} {
	if cfg == nil || cfg.ClusterPeers == "" {
		return metrics
	}

	// Parse configured peers
	configuredAddrs := make(map[string]bool)
	for _, peer := range strings.Split(cfg.ClusterPeers, ",") {
		peer = strings.TrimSpace(peer)
		if peer == "" {
			continue
		}
		// Normalize the address to extract just the host/address part
		// For configured peers, we match the full address with default port
		configuredAddrs[peer] = true
		// Also check if it's an IPv6 address without port, add default port
		if strings.HasPrefix(peer, "[") && !strings.Contains(peer, "]:") {
			configuredAddrs[peer+":"+fmt.Sprintf("%d", cfg.ClusterDefaultPort)] = true
		} else if !strings.Contains(peer, ":") && !strings.HasPrefix(peer, "[") {
			// IPv4 address without port
			configuredAddrs[peer+":"+fmt.Sprintf("%d", cfg.ClusterDefaultPort)] = true
		}
	}

	// Filter: only include peers that match configured addresses
	// This filters out ephemeral ports like :37518, :56146
	result := make([]map[string]interface{}, 0)
	for _, entry := range metrics {
		if addr, ok := entry["peer_address"].(string); ok {
			// Check if this address is configured (exact match is primary, but also accept if host matches with default port)
			if configuredAddrs[addr] {
				result = append(result, entry)
				continue
			}

			// Extract just the host part (before the colon for IPv4, or before last :port for IPv6)
			host := extractPeerHost(addr)
			// Check if this host with default port is configured
			defaultAddr := host + ":" + fmt.Sprintf("%d", cfg.ClusterDefaultPort)
			if configuredAddrs[defaultAddr] {
				result = append(result, entry)
				continue
			}

			// Check if host exactly matches a configured address
			for configured := range configuredAddrs {
				if strings.Contains(configured, host) {
					result = append(result, entry)
					break
				}
			}
		}
	}

	return result
}

// extractPeerHost extracts the host part from a peer address
// Examples: "127.0.0.1:5000" -> "127.0.0.1", "[::1]:5000" -> "[::1]", "[::1]:37518" -> "[::1]"
func extractPeerHost(addr string) string {
	if strings.HasPrefix(addr, "[") {
		// IPv6 format: [host]:port
		if idx := strings.LastIndex(addr, "]:"); idx != -1 {
			return addr[:idx+1] // Include the closing bracket
		}
	} else {
		// IPv4 format: host:port
		if idx := strings.LastIndex(addr, ":"); idx != -1 {
			return addr[:idx]
		}
	}
	return addr
}

// performNetworkDiagnostics tests network connectivity to IPv4 and IPv6
func performNetworkDiagnostics() string {
	diagnostics := " I did a ping test to "

	// Test IPv4 (Google DNS)
	ipv4Addr := "8.8.8.8"
	ipv4Status := "TIMEOUT"
	conn, err := net.DialTimeout("tcp", ipv4Addr+":53", 2*time.Second)
	if err == nil {
		conn.Close()
		ipv4Status = "OK"
	}
	diagnostics += ipv4Addr + " (" + ipv4Status + ") "

	// Test IPv6 (Google DNS)
	ipv6Addr := "[2001:4860:4860::8888]"
	ipv6Status := "TIMEOUT"
	conn, err = net.DialTimeout("tcp", ipv6Addr+":53", 2*time.Second)
	if err == nil {
		conn.Close()
		ipv6Status = "OK"
	}
	diagnostics += "and " + ipv6Addr + " (" + ipv6Status + ")"

	return diagnostics
}

// applyClusterWrite applies a write with strong consistency:
// 1) replicate to all peers and wait for ACK, 2) apply locally, 3) persist snapshot.
func applyClusterWrite(operation string, data map[string]interface{}) error {
	writeID := fmt.Sprintf("%s-%d", operation, time.Now().UnixNano())
	localPeer := "local"
	if clusterManager != nil && clusterManager.GetLocalPeer() != nil {
		localPeer = clusterManager.GetLocalPeer().Address
	}

	writeOp := &modules.WriteOperation{
		ID:         writeID,
		Operation:  operation,
		Data:       data,
		Timestamp:  time.Now().Unix(),
		SourcePeer: localPeer,
		IsPrivate:  true,
	}

	// Apply locally first so the current node always becomes the durable source of truth.
	if err := executeReplicatedWrite(writeOp); err != nil {
		return err
	}

	// Track for later replication before attempting immediate cluster commit.
	db.LogWriteWithID(writeID, writeOp.Operation, writeOp.Database, writeOp.Table, writeOp.Data, writeOp.Where, writeOp.IsPrivate)

	if clusterManager != nil {
		clusterManager.TrackWrite(writeID, "system", "cluster_op", operation, data)
	}

	// Try immediate replication, but do not roll back the local write if peers are unavailable.
	if peerServer != nil && clusterManager != nil {
		timeout := 10 * time.Second
		if operation == "import" {
			timeout = 60 * time.Second
		}
		if err := peerServer.ReplicateWriteAndWait(writeOp, timeout); err != nil {
			if strings.Contains(err.Error(), "timed out") {
				log.Printf("[WARN] Cluster write %s queued for retry after timeout: %v%s", writeID, err, performNetworkDiagnostics())
			} else {
				log.Printf("[WARN] Cluster write %s queued for retry: %v", writeID, err)
			}
		} else {
			db.MarkWriteReplicated(writeID)
		}
	}

	if err := db.SaveRuntimeSnapshot(); err != nil {
		return fmt.Errorf("failed to save snapshot: %w", err)
	}

	return nil
}

// executeReplicatedWrite applies a replicated write operation locally.
func executeReplicatedWrite(writeOp *modules.WriteOperation) error {
	switch writeOp.Operation {
	case "add_user":
		username, _ := writeOp.Data["username"].(string)
		password, _ := writeOp.Data["password"].(string)
		if username == "" || password == "" {
			return fmt.Errorf("add_user requires username and password")
		}
		if db.UserExists(username) {
			if db.UserPasswordMatches(username, password) {
				return nil
			}
			return fmt.Errorf("user already exists")
		}
		return db.AddUser(username, password)

	case "remove_user":
		username, _ := writeOp.Data["username"].(string)
		if username == "" || username == "admin" {
			return fmt.Errorf("invalid username")
		}
		return db.RemoveUser(username)

	case "flush_tokens":
		_, err := db.DeleteRows("system", "tokens", map[string]string{}, true)
		return err

	case "insert":
		// Use direct method to avoid recursive journal logging
		return db.InsertRowDirect(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.IsPrivate)

	case "delete":
		// Use direct method to avoid recursive journal logging
		_, err := db.DeleteRowsDirect(writeOp.Database, writeOp.Table, writeOp.Where, writeOp.IsPrivate)
		return err

	case "update":
		// Use direct method to avoid recursive journal logging
		_, err := db.UpdateRowsDirect(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.Where, writeOp.IsPrivate)
		return err

	case "upsert":
		// Use direct method to avoid recursive journal logging
		return db.UpsertRowDirect(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.IsPrivate)

	case "import":
		jsonStr, _ := writeOp.Data["json"].(string)
		if jsonStr == "" {
			return fmt.Errorf("import requires json data")
		}
		clearMode, _ := writeOp.Data["clear"].(bool)
		if clearMode {
			if err := db.ImportFromJSON([]byte(`{"databases":{}}`)); err != nil {
				return fmt.Errorf("failed to clear data before import: %w", err)
			}
		}
		return db.ImportFromJSON([]byte(jsonStr))

	case "import_persistent_data":
		jsonStr, _ := writeOp.Data["json"].(string)
		if jsonStr == "" {
			return fmt.Errorf("import_persistent_data requires json data")
		}
		return db.ImportFromJSON([]byte(jsonStr))

	case "configure_service":
		service, _ := writeOp.Data["service"].(string)
		if service == "" {
			return fmt.Errorf("service name required")
		}

		// Use direct method to avoid recursive journal logging
		if err := db.ConfigureServiceDirect(writeOp.Data); err != nil {
			return fmt.Errorf("failed to configure service: %w", err)
		}
		fmt.Printf("[CLUSTER] Service '%s' configured\n", service)
		return nil

	default:
		return fmt.Errorf("unsupported replicated operation: %s", writeOp.Operation)
	}
}

// parseConfigParams parses key=value pairs from config string
func parseConfigParams(configStr string) map[string]interface{} {
	params := make(map[string]interface{})
	pairs := strings.Fields(configStr)

	for _, pair := range pairs {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		// Convert value types
		if val == "true" {
			params[key] = true
		} else if val == "false" {
			params[key] = false
		} else if intVal, err := strconv.Atoi(val); err == nil {
			params[key] = intVal
		} else {
			params[key] = val
		}
	}

	return params
}

// runWithTempServer starts a temporary server, runs the function, then shuts down
func runWithTempServer(fn func() error) error {
	log.Println("No server running - starting temporary server...")

	// Initialize database
	db = modules.NewDatabase(findLatestRuntimePath(), config.PersistentBackupPath, config.AutoClearRuntimes, config.AutoBackupEnabled)
	if err := db.LoadRuntimeSnapshot(); err != nil {
		log.Printf("Warning: Failed to load runtime: %v", err)
	}

	// Run the function
	err := fn()

	// Save and exit
	if saveErr := db.SaveRuntimeSnapshot(); saveErr != nil {
		log.Printf("Warning: Failed to save runtime: %v", saveErr)
	}

	if err != nil {
		return err
	}

	log.Println("Operation completed")
	return nil
}

// watchServiceConfig monitors the database for service configuration changes
func watchServiceConfig(serviceManager *modules.ServiceManager) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// Track running servers so we can close them
	var httpServer *http.Server
	var tcpListener net.Listener
	var wsListener net.Listener

	// Store previous configs to detect changes
	var prevHTTPCfg, prevTCPCfg *modules.ServiceConfig
	var prevWSCfg *modules.WebSocketConfig
	var lastHTTPState string

	// Check config immediately on startup (don't wait 2 seconds)
	checkConfig := func() {
		if err := serviceManager.LoadConfigFromDatabase(); err != nil {
			log.Printf("Error reloading service config: %v", err)
			return
		}

		// Check HTTP service
		httpCfg := serviceManager.GetHTTPConfig()
		configChanged := (prevHTTPCfg == nil && httpCfg != nil) ||
			(prevHTTPCfg != nil && httpCfg == nil) ||
			(prevHTTPCfg != nil && httpCfg != nil &&
				(prevHTTPCfg.Host != httpCfg.Host ||
					prevHTTPCfg.Port != httpCfg.Port ||
					prevHTTPCfg.Enabled != httpCfg.Enabled))

		if httpCfg != nil {
			state := fmt.Sprintf("present:%t:%s:%d", httpCfg.Enabled, httpCfg.Host, httpCfg.Port)
			if state != lastHTTPState {
				log.Printf("[DEBUG] HTTP config found: enabled=%v, host=%s, port=%d (changed=%v)",
					httpCfg.Enabled, httpCfg.Host, httpCfg.Port, configChanged)
				lastHTTPState = state
			}
			if httpCfg.Enabled {
				if httpServer == nil || configChanged {
					// Stop old server if it exists
					if httpServer != nil {
						log.Printf("[DEBUG] Stopping old HTTP server...")
						httpServer.Close()
						httpServer = nil
						time.Sleep(100 * time.Millisecond) // Give port time to release
					}
					log.Printf("[DEBUG] Starting HTTP service on %s:%d...", httpCfg.Host, httpCfg.Port)
					// Start new server
					httpServer = startHTTPService(httpCfg)
				}
			} else {
				if httpServer != nil {
					log.Printf("[DEBUG] Disabling HTTP service...")
					httpServer.Close()
					httpServer = nil
				}
			}
		} else {
			if lastHTTPState != "missing" {
				log.Printf("[DEBUG] No HTTP config found in database")
				lastHTTPState = "missing"
			}
			if httpServer != nil {
				httpServer.Close()
				httpServer = nil
			}
		}
		prevHTTPCfg = httpCfg

		// Check TCP service
		tcpCfg := serviceManager.GetTCPConfig()
		tcpConfigChanged := (prevTCPCfg == nil && tcpCfg != nil) ||
			(prevTCPCfg != nil && tcpCfg == nil) ||
			(prevTCPCfg != nil && tcpCfg != nil &&
				(prevTCPCfg.Host != tcpCfg.Host ||
					prevTCPCfg.Port != tcpCfg.Port ||
					prevTCPCfg.Enabled != tcpCfg.Enabled))

		if tcpCfg != nil && tcpCfg.Enabled {
			if tcpListener == nil || tcpConfigChanged {
				// Stop old listener if it exists
				if tcpListener != nil {
					log.Printf("[DEBUG] Stopping old TCP listener...")
					tcpListener.Close()
					tcpListener = nil
					time.Sleep(100 * time.Millisecond) // Give port time to release
				}
				log.Printf("[DEBUG] Starting TCP service on %s:%d...", tcpCfg.Host, tcpCfg.Port)
				// Start new listener
				tcpListener = startTCPService(tcpCfg)
			}
		} else {
			if tcpListener != nil {
				log.Printf("[DEBUG] Disabling TCP service...")
				tcpListener.Close()
				tcpListener = nil
			}
		}
		prevTCPCfg = tcpCfg

		// Check WebSocket service
		wsCfg := serviceManager.GetWebSocketConfig()
		wsConfigChanged := (prevWSCfg == nil && wsCfg != nil) ||
			(prevWSCfg != nil && wsCfg == nil) ||
			(prevWSCfg != nil && wsCfg != nil &&
				(prevWSCfg.Host != wsCfg.Host ||
					prevWSCfg.Port != wsCfg.Port ||
					prevWSCfg.Enabled != wsCfg.Enabled))

		if wsCfg != nil && wsCfg.Enabled {
			if wsListener == nil || wsConfigChanged {
				// Stop old listener if it exists
				if wsListener != nil {
					log.Printf("[DEBUG] Stopping old WebSocket listener...")
					wsListener.Close()
					wsListener = nil
					time.Sleep(100 * time.Millisecond) // Give port time to release
				}
				log.Printf("[DEBUG] Starting WebSocket service on %s:%d...", wsCfg.Host, wsCfg.Port)
				// Start new listener
				wsListener = startWebSocketService(wsCfg)
			}
		} else {
			if wsListener != nil {
				log.Printf("[DEBUG] Disabling WebSocket service...")
				wsListener.Close()
				wsListener = nil
			}
		}
		prevWSCfg = wsCfg
	}

	// Run config check immediately on startup, then every 2 seconds
	checkConfig()
	for range ticker.C {
		checkConfig()
	}
}

// startHTTPService starts the HTTP/HTTPS service and returns the server
func startHTTPService(cfg *modules.ServiceConfig) *http.Server {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	log.Printf("Starting HTTP service on %s (TLS: %v)", addr, cfg.TLS)

	server := &http.Server{
		Addr:    addr,
		Handler: http.DefaultServeMux,
	}

	go func() {
		var err error
		if cfg.TLS && cfg.CertPath != "" && cfg.KeyPath != "" {
			err = server.ListenAndServeTLS(cfg.CertPath, cfg.KeyPath)
		} else {
			err = server.ListenAndServe()
		}
		if err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP service error: %v", err)
		}
	}()

	return server
}

// startTCPService starts the TCP service and returns the listener
func startTCPService(cfg *modules.ServiceConfig) net.Listener {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Failed to start TCP service: %v", err)
		return nil
	}

	go func() {
		log.Printf("TCP service listening on %s", addr)

		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("TCP accept error: %v", err)
				return
			}
			go handleTCPConnection(conn)
		}
	}()

	return listener
}

// handleTCPConnection handles incoming TCP connections
func handleTCPConnection(conn net.Conn) {
	defer conn.Close()
	if parseEnvBool(os.Getenv("TCP_LOG_CONNECTIONS"), false) {
		log.Printf("TCP connection from %s", conn.RemoteAddr())
	}

	reader := bufio.NewReader(conn)
	encoder := json.NewEncoder(conn)

	for {
		_ = conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				input := strings.TrimSpace(line)
				if input == "" {
					return
				}
				// Process last line below and then close.
				line = input
			} else {
				_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				_ = encoder.Encode(SocketResponse{Success: false, Message: fmt.Sprintf("failed to read request: %v", err)})
				return
			}
		}

		input := strings.TrimSpace(line)
		if input == "" {
			if errors.Is(err, io.EOF) {
				return
			}
			continue
		}

		var req SocketRequest
		if strings.HasPrefix(input, "{") {
			if unmarshalErr := json.Unmarshal([]byte(input), &req); unmarshalErr != nil {
				_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				_ = encoder.Encode(SocketResponse{Success: false, Message: fmt.Sprintf("invalid json request: %v", unmarshalErr)})
				if errors.Is(err, io.EOF) {
					return
				}
				continue
			}
		} else {
			cmdReq, parseErr := parseTCPTextCommand(input)
			if parseErr != nil {
				_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				_ = encoder.Encode(SocketResponse{Success: false, Message: parseErr.Error()})
				if errors.Is(err, io.EOF) {
					return
				}
				continue
			}
			req = cmdReq
		}

		if req.Command == "subscribe" {
			handleTCPSubscription(conn, req)
			return
		}

		resp := executeTCPCommand(req)
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if encodeErr := encoder.Encode(resp); encodeErr != nil {
			log.Printf("Failed to write TCP response: %v", encodeErr)
			return
		}

		if errors.Is(err, io.EOF) {
			return
		}
	}
}

func executeTCPCommand(req SocketRequest) SocketResponse {
	if req.Data == nil {
		req.Data = map[string]interface{}{}
	}

	// Step 1: username/password authentication to get JWT token
	if req.Command == "auth" {
		username, _ := req.Data["username"].(string)
		password, _ := req.Data["password"].(string)
		if username == "" || password == "" {
			return SocketResponse{Success: false, Message: "authentication required: provide username and password"}
		}

		user, err := db.AuthenticateUser(username, password)
		if err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}

		token, err := tokenManager.IssueToken(user.Username, user.IsAdmin)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("failed to issue token: %v", err)}
		}

		return SocketResponse{
			Success: true,
			Message: "authenticated",
			Data:    map[string]interface{}{"token": token, "username": user.Username, "is_admin": user.IsAdmin},
		}
	}

	// Step 2: token-based access for all TCP commands
	token, _ := req.Data["token"].(string)
	username, _, nextToken, err := authenticateTCPToken(token)
	if err != nil {
		return SocketResponse{Success: false, Message: err.Error()}
	}

	if req.Command == "export" {
		dbname, _ := req.Data["dbname"].(string)
		scope, _ := req.Data["scope"].(string)
		includeTokens := strings.EqualFold(strings.TrimSpace(scope), "full")
		exported, exportErr := exportDatabaseJSON(dbname, includeTokens)
		if exportErr != nil {
			return SocketResponse{Success: false, Message: exportErr.Error(), Data: map[string]interface{}{"token": nextToken}}
		}
		return SocketResponse{Success: true, Data: map[string]interface{}{"json": exported, "token": nextToken}}
	}

	// Handle SQL query command
	if req.Command == "query" {
		sqlQuery, _ := req.Data["query"].(string)
		if sqlQuery == "" {
			return SocketResponse{Success: false, Message: "query field required"}
		}

		// Parse SQL command
		parser := modules.NewSQLParser(db)
		cmd, err := parser.Parse(sqlQuery)
		if err != nil {
			return SocketResponse{
				Success: false,
				Message: fmt.Sprintf("SQL parse error: %v", err),
				Data:    map[string]interface{}{"token": nextToken},
			}
		}

		// Execute SQL command
		result, err := parser.Execute(cmd, username)
		if err != nil {
			return SocketResponse{
				Success: false,
				Message: fmt.Sprintf("SQL execution error: %v", err),
				Data:    map[string]interface{}{"token": nextToken},
			}
		}

		return SocketResponse{
			Success: true,
			Message: "query executed successfully",
			Data:    map[string]interface{}{"result": result, "token": nextToken},
		}
	}

	resp := executeSocketCommand(req)

	if resp.Data == nil {
		resp.Data = map[string]interface{}{}
	}
	resp.Data["token"] = nextToken
	return resp
}

func sanitizeExportJSON(jsonStr string, includeTokens bool) (string, error) {
	if includeTokens {
		return jsonStr, nil
	}

	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &payload); err != nil {
		return "", err
	}

	databases, ok := payload["databases"].(map[string]interface{})
	if !ok {
		out, err := json.MarshalIndent(payload, "", "  ")
		if err != nil {
			return "", err
		}
		return string(out), nil
	}

	for _, dbAny := range databases {
		dbMap, ok := dbAny.(map[string]interface{})
		if !ok {
			continue
		}
		if privateTables, ok := dbMap["private_tables"].(map[string]interface{}); ok {
			delete(privateTables, "tokens")
		}
	}

	out, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return "", err
	}

	return string(out), nil
}

func parseTCPTextCommand(input string) (SocketRequest, error) {
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return SocketRequest{}, fmt.Errorf("empty command")
	}

	cmd := strings.ToLower(parts[0])
	switch cmd {
	case "auth":
		if len(parts) < 3 {
			return SocketRequest{}, fmt.Errorf("usage: auth <username> <password>")
		}
		return SocketRequest{Command: "auth", Data: map[string]interface{}{"username": parts[1], "password": parts[2]}}, nil
	case "export":
		dbname := ""
		scope := ""
		if len(parts) > 1 {
			dbname = parts[1]
		}
		if len(parts) > 2 {
			scope = parts[2]
		}
		return SocketRequest{Command: "export", Data: map[string]interface{}{"dbname": dbname, "scope": scope}}, nil
	case "subscribe":
		if len(parts) < 2 {
			return SocketRequest{}, fmt.Errorf("usage: subscribe <token> [interval_seconds] [scope] [dbname]")
		}
		intervalSeconds := "5"
		scope := ""
		dbname := ""
		if len(parts) > 2 {
			intervalSeconds = parts[2]
		}
		if len(parts) > 3 {
			scope = parts[3]
		}
		if len(parts) > 4 {
			dbname = parts[4]
		}
		return SocketRequest{Command: "subscribe", Data: map[string]interface{}{"token": parts[1], "interval_seconds": intervalSeconds, "scope": scope, "dbname": dbname}}, nil
	case "listusers":
		return SocketRequest{Command: "listusers"}, nil
	case "listservices":
		return SocketRequest{Command: "listservices"}, nil
	case "listcluster":
		return SocketRequest{Command: "listcluster"}, nil
	case "peermetrics":
		if len(parts) > 1 {
			return SocketRequest{Command: "peermetrics", Data: map[string]interface{}{"peer_address": parts[1]}}, nil
		}
		return SocketRequest{Command: "peermetrics"}, nil
	case "insert", "select", "update", "delete", "upsert":
		// SQL command - return as query command with full input as query
		return SocketRequest{Command: "query", Data: map[string]interface{}{"query": input}}, nil
	default:
		return SocketRequest{}, fmt.Errorf("unknown tcp command '%s'", cmd)
	}
}

// startWebSocketService starts the WebSocket service and returns the listener
func startWebSocketService(cfg *modules.WebSocketConfig) net.Listener {
	addr := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Failed to start WebSocket service: %v", err)
		return nil
	}

	go func() {
		log.Printf("WebSocket service listening on %s (Mode: %d, Interval: %ds, Ping: %ds)",
			addr, cfg.Mode, cfg.IntervalSeconds, cfg.PingIntervalSec)

		// TODO: Implement WebSocket service
		// This will handle both mode 1 (onChange) and mode 2 (interval)
		// with proper token rotation and keep-alive

		// For now, just accept connections
		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("WebSocket accept error: %v", err)
				return
			}
			conn.Close()
		}
	}()

	return listener
}

// loadLastRuntimeCLI loads database from the last runtime folder
func loadLastRuntimeCLI() error {
	latestPath := findLatestRuntimePath()
	if latestPath == "" {
		return fmt.Errorf("no previous runtime found")
	}

	logger.Info("Loading database from last runtime: %s", latestPath)

	data, err := os.ReadFile(filepath.Join(latestPath, "database.json"))
	if err != nil {
		return fmt.Errorf("failed to read last runtime database: %w", err)
	}

	if err := db.ImportFromJSON(data); err != nil {
		return fmt.Errorf("failed to import last runtime database: %w", err)
	}

	config.RuntimePath = latestPath
	if err := db.SaveRuntimeSnapshot(); err != nil {
		return fmt.Errorf("failed to save runtime snapshot: %w", err)
	}

	logger.Audit("LOAD_LAST_RUNTIME", fmt.Sprintf("path=%s", latestPath))

	return nil
}

// loadPersistentBackupCLI loads database from persistent backup
func loadPersistentBackupCLI() error {
	backupPath, err := findLatestPersistentBackup(config.PersistentBackupPath)
	if err != nil {
		return err
	}

	logger.Info("Loading database from persistent backup: %s", backupPath)
	logger.Audit("LOAD_PERSISTENT_BACKUP", fmt.Sprintf("path=%s", backupPath))

	data, err := os.ReadFile(backupPath)
	if err != nil {
		return fmt.Errorf("failed to read backup file: %w", err)
	}

	if err := db.ImportFromJSON(data); err != nil {
		return fmt.Errorf("failed to import backup file: %w", err)
	}

	if err := db.SaveRuntimeSnapshot(); err != nil {
		return fmt.Errorf("failed to save runtime snapshot: %w", err)
	}

	return nil
}

func findLatestPersistentBackup(backupDir string) (string, error) {
	entries, err := os.ReadDir(backupDir)
	if err != nil {
		return "", fmt.Errorf("failed to read backup directory: %w", err)
	}

	var latestPath string
	var latestMod time.Time

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		if latestPath == "" || info.ModTime().After(latestMod) {
			latestPath = filepath.Join(backupDir, entry.Name())
			latestMod = info.ModTime()
		}
	}

	if latestPath == "" {
		return "", fmt.Errorf("no persistent backup found in %s", backupDir)
	}

	return latestPath, nil
}
