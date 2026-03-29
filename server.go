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
	"sort"
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
	db                      *modules.Database
	replManager             *modules.ReplicationManager
	tokenManager            *modules.TokenManager
	httpServer              *modules.HTTPServer
	clusterManager          *modules.ClusterManager
	peerServer              *modules.PeerServer
	logger                  *modules.Logger
	config                  *Config
	socketListener          net.Listener
	serverStopChan          chan string
	autoRecoveryEnabled     bool
	autoRecoveryRuntimePath string
)

const (
	socketPath       = "/tmp/tcpclusterd.sock"
	localSocketToken = "__local_socket__"
	localSocketUser  = "admin"
)

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
		LogFile:              normalizeLogFilePath(os.Getenv("LOG_FILE")),
		AuditLogFile:         normalizeJournalLogPath(os.Getenv("AUDIT_LOG_FILE")),
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

// findPreviousRuntimePath finds the second-to-last (previous) runtime folder
func findPreviousRuntimePath() string {
	runtimeDir := "runtime"
	entries, err := os.ReadDir(runtimeDir)
	if err != nil {
		return "" // No runtime directory exists
	}

	var times []time.Time
	var paths []string

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Parse timestamp from folder name (format: 2006-01-02_15-04-05)
		t, err := time.Parse("2006-01-02_15-04-05", entry.Name())
		if err != nil {
			continue // Skip folders that don't match the format
		}
		times = append(times, t)
		paths = append(paths, filepath.Join(runtimeDir, entry.Name()))
	}

	if len(times) < 2 {
		return "" // Need at least 2 runtimes to get the previous one
	}

	// Sort times to find second-to-last
	type timePathPair struct {
		t    time.Time
		path string
	}
	var pairs []timePathPair
	for i := range times {
		pairs = append(pairs, timePathPair{times[i], paths[i]})
	}

	// Sort by time descending
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].t.After(pairs[j].t)
	})

	// Return second item (index 1) which is the previous runtime
	return pairs[1].path
}

func listRuntimeSnapshots() []map[string]interface{} {
	runtimeDir := "runtime"
	entries, err := os.ReadDir(runtimeDir)
	if err != nil {
		return []map[string]interface{}{}
	}

	currentRuntimeAbs, _ := filepath.Abs(filepath.Clean(config.RuntimePath))
	type runtimeEntry struct {
		parsed time.Time
		data   map[string]interface{}
	}
	runtimes := make([]runtimeEntry, 0, len(entries))

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		name := strings.TrimSpace(entry.Name())
		path := filepath.Join(runtimeDir, name)
		absPath, _ := filepath.Abs(filepath.Clean(path))
		info, statErr := entry.Info()
		modifiedUnix := int64(0)
		if statErr == nil {
			modifiedUnix = info.ModTime().Unix()
		}

		parsed, parseErr := time.Parse("2006-01-02_15-04-05", name)
		kind := "named"
		timestampUnix := int64(0)
		if parseErr == nil {
			kind = "timestamped"
			timestampUnix = parsed.Unix()
		}

		runtimes = append(runtimes, runtimeEntry{
			parsed: parsed,
			data: map[string]interface{}{
				"name":            name,
				"path":            path,
				"kind":            kind,
				"is_current":      absPath == currentRuntimeAbs,
				"has_system":      fileExists(path + "/system.json"),
				"has_runtime":     fileExists(path+"/runtime.json") || fileExists(path+"/runtime.json.gz"),
				"has_database":    fileExists(path+"/runtime.json") || fileExists(path+"/runtime.json.gz") || fileExists(path+"/database.json"),
				"has_wal":         fileExists(path + "/wal.jsonl"),
				"has_serial_meta": fileExists(path + "/serial.meta.json"),
				"modified_unix":   modifiedUnix,
				"timestamp_unix":  timestampUnix,
			},
		})
	}

	sort.Slice(runtimes, func(i, j int) bool {
		leftKind, _ := runtimes[i].data["kind"].(string)
		rightKind, _ := runtimes[j].data["kind"].(string)
		if leftKind != rightKind {
			return leftKind == "timestamped"
		}
		if leftKind == "timestamped" {
			return runtimes[i].parsed.After(runtimes[j].parsed)
		}
		leftName, _ := runtimes[i].data["name"].(string)
		rightName, _ := runtimes[j].data["name"].(string)
		return leftName < rightName
	})

	result := make([]map[string]interface{}, 0, len(runtimes))
	for _, runtime := range runtimes {
		result = append(result, runtime.data)
	}
	return result
}

func fileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
}

// importDatabaseFromJSON imports a database from JSON data via cluster replication
func importDatabaseFromJSON(importData map[string]interface{}) error {
	jsonBytes, err := json.Marshal(importData)
	if err != nil {
		return fmt.Errorf("failed to marshal import data: %v", err)
	}

	return applyClusterWrite("import", map[string]interface{}{
		"json":  string(jsonBytes),
		"clear": true, // Always clear when loading previous runtime
	})
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

func normalizeLogFilePath(value string) string {
	trimmed := strings.TrimSpace(value)
	switch trimmed {
	case "", "app.log", "logs/app.log":
		return "system.log"
	default:
		return trimmed
	}
}

func normalizeJournalLogPath(value string) string {
	trimmed := strings.TrimSpace(value)
	switch trimmed {
	case "", "audit.log", "logs/audit.log":
		return "journal.log"
	default:
		return trimmed
	}
}

func setupSystemLog(logFilePath string) error {
	path := normalizeLogFilePath(logFilePath)
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return fmt.Errorf("failed to create log directory: %w", err)
		}
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("failed to open system log: %w", err)
	}

	log.SetOutput(file)
	log.SetFlags(log.LstdFlags)
	return nil
}

func main() {
	// Load .env file FIRST, before any config parsing
	_ = godotenv.Load()

	// NOW parse configuration from environment
	config = loadConfig()
	if err := setupSystemLog(config.LogFile); err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize system log: %v\n", err)
		os.Exit(1)
	}

	// Initialize logger (must be before any logging)
	modules.InitLogger(config.LogLevel, config.LogFile, config.AuditLogFile)
	logger = modules.GlobalLogger

	// Parse command line arguments FIRST to check if this is a CLI command
	addUser := flag.String("add", "", "Add a new user and create their personal database (usage: --add username)")
	force := flag.Bool("force", false, "Use with --add to create user even if personal database already exists")
	password := flag.String("password", "", "Password for user (if not provided, a random one will be generated)")
	clearRuntimes := flag.Bool("clearruntimes", false, "Clear old runtime folders")
	export := flag.Bool("export", false, "Export database and exit (usage: --export [dbname])")
	listTarget := flag.String("list", "", "List target: runtimes, users, services, cors, cluster")
	loadTarget := flag.String("load", "", "Load target: runtime or cluster")
	configService := flag.String("config", "", "Configure service: http, tcp, ws, cors")
	invalidateUser := flag.String("invalidate", "", "Invalidate all tokens for a user")
	auto := flag.Bool("auto", false, "Automatically choose startup data source")
	importFile := flag.String("import", "", "Import database from file")
	importClear := flag.Bool("clear", false, "Use with --import to clear current runtime data before import")
	lastRuntime := flag.Bool("lastruntime", false, "Load database from last runtime folder and start server")
	loadLastRuntime := flag.Bool("loadlastruntime", false, "Load database from the previous (not current) runtime folder and start server")
	exportLastRuntime := flag.Bool("exportlastruntime", false, "Export database from last runtime folder and exit")
	importPersistent := flag.Bool("importpersistent", false, "Load database from persistent backup and start server")
	listUsers := flag.Bool("listusers", false, "List all users and exit")
	listServices := flag.Bool("list-services", false, "List service configurations and exit")
	listCORS := flag.Bool("list-cors", false, "List API CORS configuration and exit")
	listCluster := flag.Bool("list-cluster", false, "List cluster peers from CLUSTER_PEERS and exit")
	listRuntimes := flag.Bool("listruntimes", false, "List runtime directories and exit")
	peerMetrics := flag.Bool("peer-metrics", false, "Show peer metrics (usage: --peer-metrics [peer_address])")
	removeUser := flag.String("remove", "", "Remove a user and delete their personal database (usage: --remove username)")
	start := flag.Bool("start", false, "Start the server unless it is already running")
	stop := flag.Bool("stop", false, "Stop the running server via socket")
	flushTokens := flag.Bool("flushtokens", false, "Flush all authentication tokens and exit")
	flushWAL := flag.Bool("flushwal", false, "Flush runtime WAL and force later peers to full-sync")
	configHTTP := flag.String("config-http", "", "Configure HTTP service (usage: --config-http enabled=true port=9090 host=0.0.0.0 tls=false)")
	configTCP := flag.String("config-tcp", "", "Configure TCP service (usage: --config-tcp enabled=true port=5000 host=0.0.0.0)")
	configWS := flag.String("config-ws", "", "Configure WebSocket service (usage: --config-ws enabled=true port=8080 host=0.0.0.0). Client controls polling interval.")
	configCORS := flag.String("config-cors", "", "Configure API CORS (usage: --config-cors allow_all_origins=false allowed_origins=https://app.example.com allow_credentials=true)")
	addCluster := flag.String("add-cluster", "", "Deprecated: cluster peers are configured via CLUSTER_PEERS in .env")
	removeCluster := flag.String("remove-cluster", "", "Deprecated: cluster peers are configured via CLUSTER_PEERS in .env")
	flag.Parse()

	loadRuntimeCmd := false
	loadClusterCmd := false
	configVerbUsed := false
	if target := strings.ToLower(strings.TrimSpace(*listTarget)); target != "" {
		switch target {
		case "runtimes", "runtime":
			*listRuntimes = true
		case "users", "user":
			*listUsers = true
		case "services", "service":
			*listServices = true
		case "cors":
			*listCORS = true
		case "cluster", "peers":
			*listCluster = true
		default:
			_ = writeJSONOutput(normalizeSocketResponse(SocketRequest{Command: "list"}, SocketResponse{Success: false, Code: "invalid_list_target", Message: fmt.Sprintf("unsupported list target: %s", target)}))
			os.Exit(1)
		}
	}
	if target := strings.ToLower(strings.TrimSpace(*loadTarget)); target != "" {
		switch target {
		case "runtime":
			loadRuntimeCmd = true
		case "cluster":
			loadClusterCmd = true
		default:
			_ = writeJSONOutput(normalizeSocketResponse(SocketRequest{Command: "load"}, SocketResponse{Success: false, Code: "invalid_load_target", Message: fmt.Sprintf("unsupported load target: %s", target)}))
			os.Exit(1)
		}
	}

	// Support unquoted config flags, e.g.:
	//   --config-tcp enabled=true port=8081 host=0.0.0.0
	// Standard Go flag parsing only captures the first token after the flag value.
	// If there are trailing key=value tokens and one config flag is active, append them.
	if *configService != "" && flag.NArg() > 0 {
		params := strings.TrimSpace(strings.Join(flag.Args(), " "))
		configVerbUsed = true
		switch strings.ToLower(strings.TrimSpace(*configService)) {
		case "http", "api":
			*configHTTP = params
		case "tcp":
			*configTCP = params
		case "ws", "websocket":
			*configWS = params
		case "cors":
			*configCORS = params
		default:
			_ = writeJSONOutput(normalizeSocketResponse(SocketRequest{Command: "config"}, SocketResponse{Success: false, Code: "invalid_config_target", Message: fmt.Sprintf("unsupported config target: %s", *configService)}))
			os.Exit(1)
		}
	}
	if *configHTTP != "" && flag.NArg() > 0 && !configVerbUsed {
		parts := append([]string{*configHTTP}, flag.Args()...)
		*configHTTP = strings.TrimSpace(strings.Join(parts, " "))
	}
	if *configTCP != "" && flag.NArg() > 0 && !configVerbUsed {
		parts := append([]string{*configTCP}, flag.Args()...)
		*configTCP = strings.TrimSpace(strings.Join(parts, " "))
	}
	if *configWS != "" && flag.NArg() > 0 && !configVerbUsed {
		parts := append([]string{*configWS}, flag.Args()...)
		*configWS = strings.TrimSpace(strings.Join(parts, " "))
	}
	if *configCORS != "" && flag.NArg() > 0 && !configVerbUsed {
		parts := append([]string{*configCORS}, flag.Args()...)
		*configCORS = strings.TrimSpace(strings.Join(parts, " "))
	}

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
	isCLICommand := *export || *exportLastRuntime || *importFile != "" || *lastRuntime || *loadLastRuntime || *importPersistent || *listUsers || *listServices || *listCORS || *listCluster || *listRuntimes || loadRuntimeCmd || loadClusterCmd || *invalidateUser != "" || *addUser != "" || *removeUser != "" || *stop || *flushTokens || *flushWAL || *configHTTP != "" || *configTCP != "" || *configWS != "" || *configCORS != "" || *peerMetrics || *addCluster != "" || *removeCluster != ""

	if *start && isServerRunning() {
		if err := writeJSONOutput(normalizeSocketResponse(SocketRequest{Command: "start"}, SocketResponse{
			Success: true,
			Message: "server already running",
			Data: map[string]interface{}{
				"socket_path": socketPath,
			},
		})); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write json output: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Print version only when starting server (no arguments)
	if !isCLICommand && !*start {
		fmt.Println("tcpclusterd version 1.0.4")
	}

	// If CLI command, try to use socket connection first (server is running)
	// This MUST be done BEFORE initializing cluster manager
	if isCLICommand {
		// Try socket connection first
		err := handleCLIViaSocket(*export, exportDbName, *exportLastRuntime, *importFile, *importClear, *lastRuntime, *loadLastRuntime, *importPersistent, *listUsers, *listServices, *listCORS, *listCluster, *listRuntimes, loadRuntimeCmd, loadClusterCmd, *invalidateUser, *addUser, *force, *removeUser, *stop, *flushTokens, *flushWAL, *configHTTP, *configTCP, *configWS, *configCORS, *peerMetrics, peerMetricsAddr, *addCluster, *removeCluster, *password)
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

	bootstrapSystemPath := ""
	if *auto {
		bootstrapSystemPath = findLatestRuntimePath()
		if bootstrapSystemPath != "" && strings.TrimSpace(config.ClusterPeers) == "" {
			bootstrapDB := modules.NewDatabase(bootstrapSystemPath, config.PersistentBackupPath, config.AutoClearRuntimes, config.AutoBackupEnabled)
			if err := bootstrapDB.LoadSystemSnapshotFromDir(bootstrapSystemPath); err == nil {
				if peers := bootstrapDB.ReplicationPeers(); len(peers) > 0 {
					config.ClusterPeers = strings.Join(peers, ",")
				}
			}
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
	if clusterManager != nil {
		remotePeers := clusterManager.GetRemotePeers()
		addresses := make([]string, 0, len(remotePeers))
		for _, peer := range remotePeers {
			addresses = append(addresses, peer.Address)
		}
		db.SetReplicationPeers(addresses)
	}

	if *auto {
		if bootstrapSystemPath != "" {
			if err := db.LoadSystemSnapshotFromDir(bootstrapSystemPath); err != nil {
				log.Printf("Warning: Failed to load system snapshot for auto bootstrap: %v", err)
			}
		}
	} else {
		// Try to load existing runtime
		dbErr := db.LoadRuntimeSnapshot()
		if dbErr != nil {
			log.Printf("Warning: Failed to load runtime: %v", dbErr)
			// Initialize with default config if no runtime exists
			if initErr := db.InitializeDatabase(config.AdminPassword); initErr != nil {
				log.Fatalf("Failed to initialize database: %v", initErr)
			}
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

	if *configCORS != "" {
		configureCORSCLI(*configCORS)
		os.Exit(0)
	}

	if *listCORS {
		listCORSCLI()
		os.Exit(0)
	}

	if *flushWAL {
		if err := db.FlushRuntimeWAL(); err != nil {
			logger.Error("Failed to flush WAL: %v", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	if loadRuntimeCmd {
		resp := normalizeSocketResponse(SocketRequest{Command: "loadruntime"}, SocketResponse{})
		runtimePath, reason := selectRuntimeLoadCandidate()
		if runtimePath == "" {
			resp.Success = false
			resp.OK = false
			resp.Status = "error"
			resp.Code = "no_runtime_candidate"
			resp.Message = "No previous runtime candidate found"
		} else if err := loadRuntimeFromPath(runtimePath); err != nil {
			resp.Success = false
			resp.OK = false
			resp.Status = "error"
			resp.Code = "error"
			resp.Message = err.Error()
		} else {
			resp.Success = true
			resp.OK = true
			resp.Status = "ok"
			resp.Code = "ok"
			resp.Message = fmt.Sprintf("Runtime loaded from: %s", runtimePath)
			resp.Data = map[string]interface{}{"runtime_path": runtimePath, "reason": reason}
		}
		_ = writeJSONOutput(resp)
		if !resp.Success {
			os.Exit(1)
		}
		os.Exit(0)
	}

	if loadClusterCmd {
		resp := normalizeSocketResponse(SocketRequest{Command: "loadcluster", Data: map[string]interface{}{"force": *force}}, SocketResponse{})
		result, err := loadClusterFromPeers(*force)
		if err != nil {
			resp.Success = false
			resp.OK = false
			resp.Status = "error"
			resp.Code = "cluster_load_failed"
			resp.Message = err.Error()
		} else {
			resp.Success = true
			resp.OK = true
			resp.Status = "ok"
			resp.Code = "ok"
			resp.Message = "Cluster runtime loaded"
			resp.Data = result
		}
		_ = writeJSONOutput(resp)
		if !resp.Success {
			os.Exit(1)
		}
		os.Exit(0)
	}

	if *invalidateUser != "" {
		invalidBefore := time.Now().Unix()
		err := applyClusterWrite("invalidate_user_tokens", map[string]interface{}{"username": *invalidateUser, "tokens_invalid_before": invalidBefore})
		resp := normalizeSocketResponse(SocketRequest{Command: "invalidate"}, SocketResponse{})
		if err != nil {
			resp.Success = false
			resp.OK = false
			resp.Status = "error"
			resp.Code = "error"
			resp.Message = err.Error()
		} else {
			cacheRemoved := 0
			if tokenManager != nil {
				cacheRemoved = tokenManager.InvalidateUserTokens(strings.TrimSpace(*invalidateUser))
			}
			resp.Success = true
			resp.OK = true
			resp.Status = "ok"
			resp.Code = "ok"
			resp.Message = fmt.Sprintf("Tokens invalidated for user '%s'", *invalidateUser)
			resp.Data = map[string]interface{}{"username": *invalidateUser, "tokens_invalid_before": invalidBefore, "cache_tokens_removed": cacheRemoved}
		}
		_ = writeJSONOutput(resp)
		if !resp.Success {
			os.Exit(1)
		}
		os.Exit(0)
	}

	if *auto {
		decision, err := executeAutoDecision()
		resp := normalizeSocketResponse(SocketRequest{Command: "auto"}, SocketResponse{})
		if err != nil {
			resp.Success = false
			resp.OK = false
			resp.Status = "error"
			resp.Code = "auto_failed"
			resp.Message = err.Error()
			_ = writeJSONOutput(resp)
			os.Exit(1)
		}
		resp.Success = true
		resp.OK = true
		resp.Status = "ok"
		resp.Code = "ok"
		if action, ok := decision["action"].(string); ok {
			resp.Message = action
		}
		resp.Data = decision
		if err := writeJSONOutput(resp); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write json output: %v\n", err)
			os.Exit(1)
		}
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
	serverStopChan = make(chan string, 1)

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
	if tokenManager.TokenExpiration() > 0 {
		log.Printf("[auth:token] mode=%s expiration=%s", tokenManager.TokenType(), tokenManager.TokenExpiration())
	} else {
		log.Printf("[auth:token] mode=%s expiration=disabled", tokenManager.TokenType())
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
	httpServer = modules.NewHTTPServer(db, replManager, tokenManager, wsServer, peerServer, clusterManager, replToken)
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
	if autoRecoveryEnabled {
		go runAutoRecovery(serviceManager)
	}

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
	shutdownReason := "signal"
	select {
	case sig := <-sigChan:
		shutdownReason = sig.String()
	case shutdownReason = <-serverStopChan:
	}
	log.Printf("Shutting down (%s)...", shutdownReason)

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
			timeout := 10 * time.Second
			if strings.EqualFold(strings.TrimSpace(entry.Operation), "import") {
				timeout = 60 * time.Second
			}
			op := &modules.WriteOperation{
				ID:         entry.ID,
				Operation:  entry.Operation,
				Data:       entry.Data,
				Timestamp:  entry.Timestamp.Unix(),
				SourcePeer: "retry",
				IsPrivate:  true,
			}
			if err := peerServer.ReplicateWriteAndWait(op, timeout); err != nil {
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
	Success   bool                   `json:"success"`
	OK        bool                   `json:"ok"`
	Status    string                 `json:"status,omitempty"`
	Code      string                 `json:"code,omitempty"`
	Command   string                 `json:"command,omitempty"`
	Message   string                 `json:"message,omitempty"`
	Data      map[string]interface{} `json:"data,omitempty"`
	Timestamp int64                  `json:"timestamp"`
}

func normalizeSocketResponse(req SocketRequest, resp SocketResponse) SocketResponse {
	resp.OK = resp.Success
	if resp.Status == "" {
		if resp.Success {
			resp.Status = "ok"
		} else {
			resp.Status = "error"
		}
	}
	if resp.Code == "" {
		if resp.Success {
			resp.Code = "ok"
		} else {
			resp.Code = "error"
		}
	}
	if resp.Command == "" {
		resp.Command = req.Command
	}
	if resp.Timestamp == 0 {
		resp.Timestamp = time.Now().Unix()
	}
	return resp
}

func writeJSONOutput(value interface{}) error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func requestServerStop(reason string) error {
	if serverStopChan == nil {
		return fmt.Errorf("server stop channel is unavailable")
	}
	select {
	case serverStopChan <- reason:
		return nil
	default:
		return fmt.Errorf("shutdown already in progress")
	}
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
func handleCLIViaSocket(export bool, exportDbName string, exportLastRuntime bool, importFile string, importClear bool, lastRuntime bool, loadLastRuntime bool, importPersistent bool, listUsers bool, listServices bool, listCORS bool, listCluster bool, listRuntimes bool, loadRuntime bool, loadCluster bool, invalidateUser string, addUser string, force bool, removeUser string, stop bool, flushTokens bool, flushWAL bool, configHTTP string, configTCP string, configWS string, configCORS string, peerMetrics bool, peerMetricsAddr string, addCluster string, removeCluster string, password string) error {
	var (
		req       *SocketRequest
		resp      *SocketResponse
		localResp *SocketResponse
	)

	switch {
	case export:
		req = &SocketRequest{Command: "export", Data: map[string]interface{}{"dbname": exportDbName}}
	case listUsers:
		req = &SocketRequest{Command: "listusers"}
	case listServices:
		req = &SocketRequest{Command: "listservices"}
	case listCORS:
		req = &SocketRequest{Command: "listcors"}
	case listCluster:
		req = &SocketRequest{Command: "listcluster"}
	case listRuntimes:
		req = &SocketRequest{Command: "listruntimes"}
	case loadRuntime:
		req = &SocketRequest{Command: "loadruntime"}
	case loadCluster:
		req = &SocketRequest{Command: "loadcluster", Data: map[string]interface{}{"force": force}}
	case invalidateUser != "":
		req = &SocketRequest{Command: "invalidate", Data: map[string]interface{}{"username": invalidateUser}}
	case addUser != "":
		userPassword := password
		if userPassword == "" {
			userPassword = generateRandomPassword(16)
		}
		req = &SocketRequest{
			Command: "adduser",
			Data: map[string]interface{}{
				"username": addUser,
				"force":    force,
				"password": userPassword,
			},
		}
		resp, err := sendSocketCommand(*req)
		if err != nil {
			return err
		}
		if password == "" {
			if resp.Data == nil {
				resp.Data = map[string]interface{}{}
			}
			resp.Data["generated_password"] = userPassword
		}
		localResp = resp
	case removeUser != "":
		req = &SocketRequest{Command: "removeuser", Data: map[string]interface{}{"username": removeUser}}
	case stop:
		req = &SocketRequest{Command: "stop"}
	case flushTokens:
		req = &SocketRequest{Command: "flushtokens"}
	case flushWAL:
		req = &SocketRequest{Command: "flushwal"}
	case configHTTP != "":
		req = &SocketRequest{Command: "configure", Data: map[string]interface{}{"service": "http", "config": configHTTP}}
	case configTCP != "":
		req = &SocketRequest{Command: "configure", Data: map[string]interface{}{"service": "tcp", "config": configTCP}}
	case configWS != "":
		req = &SocketRequest{Command: "configure", Data: map[string]interface{}{"service": "ws", "config": configWS}}
	case configCORS != "":
		req = &SocketRequest{Command: "configurecors", Data: map[string]interface{}{"config": configCORS}}
	case peerMetrics:
		req = &SocketRequest{Command: "peermetrics"}
		if peerMetricsAddr != "" {
			req.Data = map[string]interface{}{"peer_address": peerMetricsAddr}
		}
	case exportLastRuntime:
		req = &SocketRequest{Command: "exportlastruntime"}
	case lastRuntime:
		req = &SocketRequest{Command: "lastruntime"}
	case loadLastRuntime:
		req = &SocketRequest{Command: "loadlastruntime"}
	case importFile != "":
		data, readErr := os.ReadFile(importFile)
		if readErr != nil {
			return fmt.Errorf("failed to read import file: %v", readErr)
		}
		req = &SocketRequest{Command: "import", Data: map[string]interface{}{"json": string(data), "clear": importClear}}
	case importPersistent:
		req = &SocketRequest{Command: "importpersistent"}
	case addCluster != "" || removeCluster != "":
		req = &SocketRequest{Command: "cluster-config"}
		localResp = &SocketResponse{Success: false, Message: "add-cluster and remove-cluster are deprecated. Edit CLUSTER_PEERS in .env on all nodes."}
	default:
		return fmt.Errorf("no valid CLI command")
	}

	if localResp == nil {
		var err error
		resp, err = sendSocketCommand(*req)
		if err != nil {
			return err
		}
		localResp = resp
	}
	if req != nil {
		normalized := normalizeSocketResponse(*req, *localResp)
		localResp = &normalized
	}

	if err := writeJSONOutput(localResp); err != nil {
		return fmt.Errorf("failed to write json output: %w", err)
	}
	if !localResp.Success {
		return fmt.Errorf("%s failed: %s", localResp.Command, localResp.Message)
	}
	return nil
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

// handleSocketConnection handles incoming local socket connections.
func handleSocketConnection(conn net.Conn) {
	handleProtocolConnection(conn, true)
}

// executeSocketCommand executes commands received via socket
func executeSocketCommand(req SocketRequest, allowUnauthenticated bool) SocketResponse {
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

	case "listcors":
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"cors": db.GetAPICORSConfig()},
		}

	case "listcluster":
		servers := getClusterPeersView()
		return SocketResponse{
			Success: true,
			Data:    map[string]interface{}{"servers": servers},
		}

	case "listruntimes":
		runtimes := listRuntimeSnapshots()
		return SocketResponse{
			Success: true,
			Message: "runtime directories listed",
			Data: map[string]interface{}{
				"runtimes": runtimes,
				"count":    len(runtimes),
			},
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
		force, _ := req.Data["force"].(bool)
		if username == "" || password == "" {
			return SocketResponse{Success: false, Message: "username and password required"}
		}
		if err := applyClusterWrite("add_user", map[string]interface{}{
			"username": username,
			"force":    force,
			"password": password,
		}); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		if force {
			return SocketResponse{Success: true, Message: "✓ User '" + username + "' created successfully (force mode)"}
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

	case "flushwal":
		if err := db.FlushRuntimeWAL(); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{Success: true, Message: "Runtime WAL flushed; later lagging peers must full-sync"}

	case "stop":
		if err := requestServerStop("socket stop command"); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{
			Success: true,
			Message: "shutdown requested",
			Data: map[string]interface{}{
				"socket_path": socketPath,
			},
		}

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

	case "configurecors":
		configStr, _ := req.Data["config"].(string)
		if strings.TrimSpace(configStr) == "" {
			return SocketResponse{Success: false, Message: "config required"}
		}
		params := parseConfigParams(configStr)
		if err := applyClusterWrite("configure_api_cors", params); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{Success: true, Message: "API CORS configured successfully", Data: map[string]interface{}{"cors": db.GetAPICORSConfig()}}

	case "import":
		jsonStr, _ := req.Data["json"].(string)
		if jsonStr == "" {
			return SocketResponse{Success: false, Message: "json data required"}
		}
		dbname, _ := req.Data["dbname"].(string)
		clearMode, _ := req.Data["clear"].(bool)
		if err := applyClusterWrite("import", map[string]interface{}{
			"dbname": dbname,
			"json":   jsonStr,
			"clear":  clearMode,
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
		data, err := exportRuntimeDirJSON(lastPath)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("failed to export runtime database: %v", err)}
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

	case "loadlastruntime":
		// Find the second-to-last runtime (previous runtime, not current)
		lastPath := findPreviousRuntimePath()
		if lastPath == "" {
			return SocketResponse{Success: false, Message: "No previous runtime found"}
		}

		if err := loadRuntimeFromPath(lastPath); err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("failed to import database: %v", err)}
		}

		return SocketResponse{
			Success: true,
			Message: fmt.Sprintf("✓ Previous runtime database loaded from: %s", lastPath),
		}

	case "loadruntime":
		runtimePath, reason := selectRuntimeLoadCandidate()
		if runtimePath == "" {
			return SocketResponse{Success: false, Code: "no_runtime_candidate", Message: "No previous runtime candidate found"}
		}
		if err := loadRuntimeFromPath(runtimePath); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		return SocketResponse{
			Success: true,
			Message: fmt.Sprintf("Runtime loaded from: %s", runtimePath),
			Data: map[string]interface{}{
				"runtime_path": runtimePath,
				"reason":       reason,
			},
		}

	case "loadcluster":
		force, _ := req.Data["force"].(bool)
		result, err := loadClusterFromPeers(force)
		if err != nil {
			return SocketResponse{Success: false, Code: "cluster_load_failed", Message: err.Error()}
		}
		return SocketResponse{
			Success: true,
			Message: "Cluster runtime loaded",
			Data:    result,
		}

	case "invalidate":
		username, _ := req.Data["username"].(string)
		invalidBefore := time.Now().Unix()
		if err := applyClusterWrite("invalidate_user_tokens", map[string]interface{}{"username": username, "tokens_invalid_before": invalidBefore}); err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}
		cacheRemoved := 0
		if tokenManager != nil {
			cacheRemoved = tokenManager.InvalidateUserTokens(strings.TrimSpace(username))
		}
		return SocketResponse{
			Success: true,
			Message: fmt.Sprintf("Tokens invalidated for user '%s'", username),
			Data: map[string]interface{}{
				"username":              username,
				"tokens_invalid_before": invalidBefore,
				"cache_tokens_removed":  cacheRemoved,
			},
		}

	case "adjust":
		// Atomically adjust (add/subtract) a column value in a table row
		// Data: { token, dbname, table, column, delta (int), where (map) }
		dbname, _ := req.Data["dbname"].(string)
		tableName, _ := req.Data["table"].(string)
		columnName, _ := req.Data["column"].(string)
		deltaRaw, _ := req.Data["delta"]
		whereRaw, _ := req.Data["where"].(map[string]interface{})
		isPrivate, _ := req.Data["private"].(bool)

		if dbname == "" || tableName == "" || columnName == "" {
			return SocketResponse{Success: false, Message: "adjust requires: dbname, table, column, delta, where"}
		}

		username, _, nextToken, err := resolveCommandIdentity(req, allowUnauthenticated)
		if err != nil {
			return SocketResponse{Success: false, Message: err.Error()}
		}

		var delta float64
		switch v := deltaRaw.(type) {
		case float64:
			delta = v
		case int:
			delta = float64(v)
		case int64:
			delta = float64(v)
		case json.Number:
			parsed, parseErr := v.Float64()
			if parseErr != nil {
				return SocketResponse{Success: false, Message: "delta must be a number"}
			}
			delta = parsed
		case string:
			parsed, parseErr := strconv.ParseFloat(v, 64)
			if parseErr != nil {
				return SocketResponse{Success: false, Message: "delta must be a number"}
			}
			delta = parsed
		default:
			return SocketResponse{Success: false, Message: "delta must be a number"}
		}

		// Convert where map[string]interface{} to where map[string]string
		whereClause := make(map[string]string)
		for k, v := range whereRaw {
			whereClause[k] = fmt.Sprintf("%v", v)
		}

		// Perform atomically adjusted update
		newValue, err := db.AdjustColumn(dbname, tableName, columnName, delta, whereClause, isPrivate)
		if err != nil {
			return SocketResponse{Success: false, Message: fmt.Sprintf("adjust failed: %v", err)}
		}

		// Log the adjustment
		log.Printf("[adjust] user=%s dbname=%s table=%s column=%s delta=%g new_value=%g", username, dbname, tableName, columnName, delta, newValue)

		// Replicate to cluster
		if err := applyClusterWrite("adjust_column", map[string]interface{}{
			"dbname":    dbname,
			"table":     tableName,
			"column":    columnName,
			"delta":     delta,
			"where":     whereClause,
			"private":   isPrivate,
			"new_value": newValue,
		}); err != nil {
			log.Printf("[adjust:replicate:warning] failed to replicate: %v", err)
		}

		return SocketResponse{
			Success: true,
			Data: map[string]interface{}{
				"new_value": newValue,
				"token":     nextToken,
			},
		}

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

	return username, isAdmin, token, nil
}

func resolveCommandIdentity(req SocketRequest, allowUnauthenticated bool) (string, bool, string, error) {
	if allowUnauthenticated {
		return localSocketUser, true, localSocketToken, nil
	}
	if req.Data == nil {
		return "", false, "", fmt.Errorf("authentication required: call auth first, then include token")
	}
	token, _ := req.Data["token"].(string)
	return authenticateTCPToken(token)
}

func handleTCPSubscription(conn net.Conn, req SocketRequest, allowUnauthenticated bool) {
	if req.Data == nil {
		req.Data = map[string]interface{}{}
	}

	_, _, nextToken, err := resolveCommandIdentity(req, allowUnauthenticated)
	if err != nil {
		_ = json.NewEncoder(conn).Encode(normalizeSocketResponse(req, SocketResponse{Success: false, Message: err.Error()}))
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
			return encoder.Encode(normalizeSocketResponse(req, SocketResponse{Success: false, Message: fmt.Sprintf("failed to export database: %v", exportErr)}))
		}

		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		return encoder.Encode(normalizeSocketResponse(req, SocketResponse{
			Success: true,
			Message: message,
			Data: map[string]interface{}{
				"json":             payload,
				"token":            nextToken,
				"interval_seconds": intervalSeconds,
			},
		}))
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

func listCORSCLI() {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{Command: "listcors"})
		if err != nil {
			log.Fatalf("Failed to list API CORS configuration: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		fmt.Printf("API CORS: %+v\n", resp.Data["cors"])
		return
	}

	runWithTempServer(func() error {
		fmt.Printf("API CORS: %+v\n", db.GetAPICORSConfig())
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

func configureCORSCLI(configStr string) {
	if isServerRunning() {
		resp, err := sendSocketCommand(SocketRequest{
			Command: "configurecors",
			Data: map[string]interface{}{
				"config": configStr,
			},
		})
		if err != nil {
			log.Fatalf("Failed to configure API CORS: %v", err)
		}
		if !resp.Success {
			log.Fatalf("Error: %s", resp.Message)
		}
		fmt.Println(resp.Message)
		return
	}

	runWithTempServer(func() error {
		return configureCORS(configStr)
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

func configureCORS(configStr string) error {
	params := parseConfigParams(configStr)
	if err := applyClusterWrite("configure_api_cors", params); err != nil {
		return fmt.Errorf("failed to configure API CORS: %w", err)
	}

	fmt.Printf("✓ API CORS configured successfully\n")
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
	localPeer := "local"
	if clusterManager != nil && clusterManager.GetLocalPeer() != nil {
		localPeer = clusterManager.GetLocalPeer().Address
	}

	writeOp := &modules.WriteOperation{
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

	serial := db.CurrentLastAppliedSerial()
	writeID := fmt.Sprintf("%s-%020d", operation, serial)
	writeOp.Serial = serial
	writeOp.ID = writeID

	// Track for later replication before attempting immediate cluster commit.
	db.LogWriteWithIDAndSerial(writeID, serial, writeOp.Operation, writeOp.Database, writeOp.Table, writeOp.Data, writeOp.Where, writeOp.IsPrivate)

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
		force, _ := writeOp.Data["force"].(bool)
		if username == "" || password == "" {
			return fmt.Errorf("add_user requires username and password")
		}
		if db.UserExists(username) {
			if db.UserPasswordMatches(username, password) {
				return nil
			}
			return fmt.Errorf("user already exists")
		}
		if writeOp.Serial > 0 {
			return db.AddUserWithForceDirectWithSerial(username, password, force, writeOp.Serial)
		}
		return db.AddUserWithForceDirect(username, password, force)

	case "remove_user":
		username, _ := writeOp.Data["username"].(string)
		if username == "" || username == "admin" {
			return fmt.Errorf("invalid username")
		}
		if writeOp.Serial > 0 {
			return db.RemoveUserDirectWithSerial(username, writeOp.Serial)
		}
		return db.RemoveUserDirect(username)

	case "flush_tokens":
		var err error
		if writeOp.Serial > 0 {
			_, err = db.DeleteRowsDirectWithSerial("system", "tokens", map[string]string{}, true, writeOp.Serial)
		} else {
			_, err = db.DeleteRowsDirect("system", "tokens", map[string]string{}, true)
		}
		return err

	case "invalidate_user_tokens":
		username, _ := writeOp.Data["username"].(string)
		if strings.TrimSpace(username) == "" {
			return fmt.Errorf("invalidate_user_tokens requires username")
		}
		invalidBefore := time.Now().Unix()
		switch value := writeOp.Data["tokens_invalid_before"].(type) {
		case int64:
			invalidBefore = value
		case int:
			invalidBefore = int64(value)
		case float64:
			invalidBefore = int64(value)
		}
		var (
			updated int
			err     error
		)
		if writeOp.Serial > 0 {
			updated, err = db.UpdateRowsDirectWithSerial("system", "users", map[string]interface{}{"tokens_invalid_before": invalidBefore}, map[string]string{"username": username}, true, writeOp.Serial)
		} else {
			updated, err = db.UpdateRowsDirect("system", "users", map[string]interface{}{"tokens_invalid_before": invalidBefore}, map[string]string{"username": username}, true)
		}
		if err != nil {
			return err
		}
		if updated == 0 {
			return fmt.Errorf("user not found")
		}
		if tokenManager != nil {
			tokenManager.InvalidateUserTokens(strings.TrimSpace(username))
		}
		return nil

	case "system_log":
		if writeOp.Serial > 0 {
			return db.LogSystemEventDirectWithSerial(writeOp.Data, writeOp.Serial)
		}
		return db.LogSystemEventDirect(writeOp.Data)

	case "insert":
		// Use direct method to avoid recursive journal logging
		if writeOp.Serial > 0 {
			return db.InsertRowDirectWithSerial(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.IsPrivate, writeOp.Serial)
		}
		return db.InsertRowDirect(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.IsPrivate)

	case "insert_log":
		if writeOp.Serial > 0 {
			return db.InsertLogRowDirectWithSerial(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.Serial)
		}
		return db.InsertLogRowDirect(writeOp.Database, writeOp.Table, writeOp.Data)

	case "delete":
		// Use direct method to avoid recursive journal logging
		var err error
		if writeOp.Serial > 0 {
			_, err = db.DeleteRowsDirectWithSerialAdvanced(writeOp.Database, writeOp.Table, writeOp.Where, writeOp.WhereConditions, writeOp.IsPrivate, writeOp.Serial)
		} else {
			_, err = db.DeleteRowsDirectAdvanced(writeOp.Database, writeOp.Table, writeOp.Where, writeOp.WhereConditions, writeOp.IsPrivate)
		}
		return err

	case "update":
		// Use direct method to avoid recursive journal logging
		var err error
		if writeOp.Serial > 0 {
			_, err = db.UpdateRowsDirectWithSerialAdvanced(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.Where, writeOp.WhereConditions, writeOp.IsPrivate, writeOp.Serial)
		} else {
			_, err = db.UpdateRowsDirectAdvanced(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.Where, writeOp.WhereConditions, writeOp.IsPrivate)
		}
		return err

	case "upsert":
		// Use direct method to avoid recursive journal logging
		if writeOp.Serial > 0 {
			return db.UpsertRowDirectWithSerial(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.IsPrivate, writeOp.Serial)
		}
		return db.UpsertRowDirect(writeOp.Database, writeOp.Table, writeOp.Data, writeOp.IsPrivate)

	case "import":
		dbname, _ := writeOp.Data["dbname"].(string)
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
		if strings.TrimSpace(dbname) != "" {
			return db.ImportDatabaseFromJSON(strings.TrimSpace(dbname), []byte(jsonStr))
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
		if writeOp.Serial > 0 {
			if err := db.ConfigureServiceDirectWithSerial(writeOp.Data, writeOp.Serial); err != nil {
				return fmt.Errorf("failed to configure service: %w", err)
			}
			fmt.Printf("[CLUSTER] Service '%s' configured\n", service)
			return nil
		}
		if err := db.ConfigureServiceDirect(writeOp.Data); err != nil {
			return fmt.Errorf("failed to configure service: %w", err)
		}
		fmt.Printf("[CLUSTER] Service '%s' configured\n", service)
		return nil

	case "configure_api_cors":
		if writeOp.Serial > 0 {
			if err := db.ConfigureAPICORSDirectWithSerial(writeOp.Data, writeOp.Serial); err != nil {
				return fmt.Errorf("failed to configure API CORS: %w", err)
			}
			fmt.Printf("[CLUSTER] API CORS configured\n")
			return nil
		}
		if err := db.ConfigureAPICORSDirect(writeOp.Data); err != nil {
			return fmt.Errorf("failed to configure API CORS: %w", err)
		}
		fmt.Printf("[CLUSTER] API CORS configured\n")
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
	handleProtocolConnection(conn, false)
}

func handleProtocolConnection(conn net.Conn, allowUnauthenticated bool) {
	defer conn.Close()
	if !allowUnauthenticated && parseEnvBool(os.Getenv("TCP_LOG_CONNECTIONS"), false) {
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
				// On idle timeout: close silently. Sending an error response here
				// would leave a stale {Success:false} message in the client's read
				// buffer, causing the next legitimate request on the reused
				// connection to be misinterpreted as a failed auth response → 502.
				var netErr net.Error
				if errors.As(err, &netErr) && netErr.Timeout() {
					return
				}
				_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				_ = encoder.Encode(normalizeSocketResponse(SocketRequest{Command: "read"}, SocketResponse{Success: false, Message: fmt.Sprintf("failed to read request: %v", err)}))
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
				_ = encoder.Encode(normalizeSocketResponse(SocketRequest{Command: "parse"}, SocketResponse{Success: false, Message: fmt.Sprintf("invalid json request: %v", unmarshalErr)}))
				if errors.Is(err, io.EOF) {
					return
				}
				continue
			}
		} else {
			cmdReq, parseErr := parseTCPTextCommand(input)
			if parseErr != nil {
				_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
				_ = encoder.Encode(normalizeSocketResponse(SocketRequest{Command: "parse"}, SocketResponse{Success: false, Message: parseErr.Error()}))
				if errors.Is(err, io.EOF) {
					return
				}
				continue
			}
			req = cmdReq
		}

		if req.Command == "subscribe" {
			handleTCPSubscription(conn, req, allowUnauthenticated)
			return
		}

		resp := executeTCPCommand(req, allowUnauthenticated)
		resp = normalizeSocketResponse(req, resp)
		_ = conn.SetWriteDeadline(time.Now().Add(30 * time.Second))
		if encodeErr := encoder.Encode(resp); encodeErr != nil {
			if allowUnauthenticated {
				log.Printf("Failed to write socket response: %v", encodeErr)
			} else {
				log.Printf("Failed to write TCP response: %v", encodeErr)
			}
			return
		}

		if errors.Is(err, io.EOF) {
			return
		}
	}
}

func executeTCPCommand(req SocketRequest, allowUnauthenticated bool) SocketResponse {
	if req.Data == nil {
		req.Data = map[string]interface{}{}
	}

	// Step 1: username/password authentication to get JWT token
	if req.Command == "auth" {
		if allowUnauthenticated {
			return SocketResponse{
				Success: true,
				Message: "local socket access does not require authentication",
				Data: map[string]interface{}{
					"token":                   localSocketToken,
					"username":                localSocketUser,
					"is_admin":                true,
					"transport":               "unix",
					"authentication_required": false,
				},
			}
		}

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
	username, _, nextToken, err := resolveCommandIdentity(req, allowUnauthenticated)
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
		requestSource := "socket:" + username
		if allowUnauthenticated {
			requestSource = "socket:local"
		}
		result, err := parser.ExecuteWithSource(cmd, username, requestSource)
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

	resp := executeSocketCommand(req, allowUnauthenticated)

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
	case "listcors":
		return SocketRequest{Command: "listcors"}, nil
	case "listcluster":
		return SocketRequest{Command: "listcluster"}, nil
	case "configurecors":
		if len(parts) < 2 {
			return SocketRequest{}, fmt.Errorf("usage: configurecors <key=value ...>")
		}
		return SocketRequest{Command: "configurecors", Data: map[string]interface{}{"config": strings.Join(parts[1:], " ")}}, nil
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
		// with stable token reuse and keep-alive

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

	if err := loadRuntimeFromPath(latestPath); err != nil {
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

func exportRuntimeDirJSON(path string) ([]byte, error) {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return nil, fmt.Errorf("runtime path is required")
	}
	tempDB := modules.NewDatabase(trimmedPath, "", false, false)
	if err := tempDB.LoadRuntimeSnapshot(); err != nil {
		return nil, err
	}
	return tempDB.ExportToJSON()
}

func loadRuntimeFromPath(path string) error {
	trimmedPath := strings.TrimSpace(path)
	if trimmedPath == "" {
		return fmt.Errorf("runtime path is required")
	}

	if err := db.LoadRuntimeDataSnapshotFromDir(trimmedPath); err != nil {
		return fmt.Errorf("failed to import runtime database: %w", err)
	}
	if err := db.SaveRuntimeSnapshot(); err != nil {
		return fmt.Errorf("failed to save runtime snapshot: %w", err)
	}
	return nil
}

func selectRuntimeLoadCandidate() (string, string) {
	path := findPreviousRuntimePath()
	if path == "" {
		return "", "no_previous_runtime"
	}
	return path, "latest_non_current_runtime"
}

func loadClusterFromPeers(force bool) (map[string]interface{}, error) {
	if db == nil {
		return nil, fmt.Errorf("database not initialized")
	}
	if clusterManager == nil || peerServer == nil {
		return nil, fmt.Errorf("cluster peer sync is unavailable")
	}
	if len(db.ReplicationPeers()) == 0 {
		return nil, fmt.Errorf("no cluster peers configured")
	}
	if !force && !db.IsRuntimeDataEmpty() {
		return nil, fmt.Errorf("local runtime already has data; rerun with force to replace it from cluster")
	}

	beforeSerial, _, _ := db.RuntimeSerialState()
	beforeUpdatedAt := db.DatabaseUpdatedAt("")
	beforeEmpty := db.IsRuntimeDataEmpty()

	peerAddr, err := peerServer.RequestClusterSync(force, 3*time.Second)
	if err != nil {
		return nil, err
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		currentSerial, _, _ := db.RuntimeSerialState()
		currentUpdatedAt := db.DatabaseUpdatedAt("")
		if beforeEmpty {
			if !db.IsRuntimeDataEmpty() || currentSerial > beforeSerial || currentUpdatedAt > beforeUpdatedAt {
				return map[string]interface{}{
					"peer":          peerAddr,
					"force":         force,
					"serial_before": beforeSerial,
					"serial_after":  currentSerial,
				}, nil
			}
		} else if force {
			if currentSerial != beforeSerial || currentUpdatedAt != beforeUpdatedAt {
				return map[string]interface{}{
					"peer":          peerAddr,
					"force":         force,
					"serial_before": beforeSerial,
					"serial_after":  currentSerial,
				}, nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}

	return nil, fmt.Errorf("cluster sync was requested from %s but no runtime data was applied before timeout", peerAddr)
}

func executeAutoDecision() (map[string]interface{}, error) {
	decision := map[string]interface{}{
		"action":  "start_blank_runtime",
		"reason":  "empty_runtime_no_recovery_candidate",
		"details": map[string]interface{}{},
	}

	if db != nil && !db.IsRuntimeDataEmpty() {
		decision["action"] = "start_current_runtime"
		decision["reason"] = "current_runtime_has_state"
		decision["details"] = map[string]interface{}{
			"runtime_path": config.RuntimePath,
		}
		return decision, nil
	}

	if clusterManager != nil && len(db.ReplicationPeers()) > 0 {
		autoRecoveryEnabled = true
		runtimePath, _ := selectRuntimeLoadCandidate()
		autoRecoveryRuntimePath = runtimePath
		decision["action"] = "wait_for_cluster_sync"
		decision["reason"] = "system_snapshot_loaded_cluster_peers_available"
		decision["details"] = map[string]interface{}{
			"cluster_peers":         db.ReplicationPeers(),
			"runtime_fallback_path": runtimePath,
		}
		return decision, nil
	}

	if runtimePath, reason := selectRuntimeLoadCandidate(); runtimePath != "" {
		if err := loadRuntimeFromPath(runtimePath); err != nil {
			return nil, err
		}
		decision["action"] = "load_runtime"
		decision["reason"] = reason
		decision["details"] = map[string]interface{}{
			"runtime_path": runtimePath,
		}
		return decision, nil
	}

	if backupPath, err := findLatestPersistentBackup(config.PersistentBackupPath); err == nil {
		if err := loadPersistentBackupCLI(); err != nil {
			return nil, err
		}
		decision["action"] = "load_persistent_backup"
		decision["reason"] = "no_runtime_candidate"
		decision["details"] = map[string]interface{}{
			"backup_path": backupPath,
		}
		return decision, nil
	}

	decision["details"] = map[string]interface{}{
		"runtime_path": config.RuntimePath,
	}
	return decision, nil
}

func runAutoRecovery(serviceManager *modules.ServiceManager) {
	if db == nil || !db.IsRuntimeDataEmpty() {
		return
	}

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if !db.IsRuntimeDataEmpty() {
			logger.Info("Auto recovery completed from cluster sync before local runtime fallback")
			return
		}
		time.Sleep(2 * time.Second)
	}

	if db.IsRuntimeDataEmpty() && strings.TrimSpace(autoRecoveryRuntimePath) != "" {
		logger.Warn("Cluster did not provide runtime data in time; loading local runtime fallback from %s", autoRecoveryRuntimePath)
		if err := loadRuntimeFromPath(autoRecoveryRuntimePath); err != nil {
			logger.Error("Auto runtime fallback failed: %v", err)
			return
		}
		if serviceManager != nil {
			if err := serviceManager.LoadConfigFromDatabase(); err != nil {
				logger.Warn("Failed to reload service config after auto runtime fallback: %v", err)
			}
		}
	}
}
