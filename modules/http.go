package modules

import (
	"bufio"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// LoadEnvFile loads environment variables from a file
func LoadEnvFile(path string) map[string]string {
	env := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return env
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key := strings.TrimSpace(kv[0])
		val := strings.TrimSpace(kv[1])
		if strings.HasPrefix(val, "\"") && strings.HasSuffix(val, "\"") && len(val) >= 2 {
			val = val[1 : len(val)-1]
		}
		env[key] = val
	}

	return env
}

// GenerateRandomKey generates a random base64-encoded key of specified byte length
func GenerateRandomKey(byteLength int) string {
	bytes := make([]byte, byteLength)
	if _, err := rand.Read(bytes); err != nil {
		log.Printf("Warning: failed to generate random key: %v, using placeholder", err)
		return "placeholder-change-this-" + fmt.Sprintf("%d", byteLength)
	}
	return base64.StdEncoding.EncodeToString(bytes)
}

// InitializeEnv creates a new .env file with defaults and generated keys
// This requires explicit --init flag to prevent accidental overwrites
func InitializeEnv(path string) error {
	// Generate random keys
	cipherKey := GenerateRandomKey(32)  // 32 bytes for AES-256
	peerSecret := GenerateRandomKey(16) // 16 bytes for peer secret
	tcpKey := GenerateRandomKey(16)     // 16 bytes for TCP auth

	defaultEnv := fmt.Sprintf(`# HTTP server configuration
HTTP_ENABLED=true
HTTP_PORT=8888
HTTP_LISTEN_ADDR=[::]
HTTP_WHITELIST_IPS=0.0.0.0/0,::/0

	# HTTPS server configuration
	HTTPS_ENABLED=false
	HTTPS_PORT=8443
	HTTPS_LISTEN_ADDR=[::]
	HTTPS_CERT_FILE=
	HTTPS_KEY_FILE=

	# Optional virtual host restriction (comma-separated).
	# If empty and HTTPS is enabled, domains are auto-detected from certificate.
	LISTEN_DOMAINS=

# WebSocket configuration
WS_ENABLED=true

# Logging configuration
LOG_ENABLED=false
LOG_PATH=system.log

# Cluster configuration
CLUSTER_PORT=9000
CLUSTER_LISTEN_ADDR=[::]
CLUSTER_PEERS=[::1]
CLUSTER_CIPHER_KEY=%s
CLUSTER_PEER_SECRET=%s
CLUSTER_WHITELIST_IPS=0.0.0.0/0,::/0

# TCP protocol configuration (direct cluster communication)
TCP_ENABLED=true
TCP_PORT=9001
TCP_LISTEN_ADDR=[::]
TCP_KEY=%s
TCP_WHITELIST_IPS=0.0.0.0/0,::/0
TCP_TIMEOUT=30

# Authentication for private arrays
ADMIN_USERNAME=admin
ADMIN_PASSWORD=changeme
`, cipherKey, peerSecret, tcpKey)

	if err := os.WriteFile(path, []byte(defaultEnv), 0600); err != nil {
		return fmt.Errorf("failed to create .env file: %w", err)
	}

	return nil
}

// EnsureEnvFile creates a default .env file if it doesn't exist
func EnsureEnvFile(path string) error {
	// Check if file already exists
	if _, err := os.Stat(path); err == nil {
		return nil // File exists, nothing to do
	}

	// Generate random keys
	cipherKey := GenerateRandomKey(32)  // 32 bytes for AES-256
	peerSecret := GenerateRandomKey(16) // 16 bytes for peer secret
	tcpKey := GenerateRandomKey(16)     // 16 bytes for TCP auth

	defaultEnv := fmt.Sprintf(`HTTP_PORT=8888

	# HTTP and HTTPS server configuration
	HTTP_ENABLED=true
	HTTP_LISTEN_ADDR=0.0.0.0

	HTTPS_ENABLED=false
	HTTPS_PORT=8443
	HTTPS_LISTEN_ADDR=0.0.0.0
	HTTPS_CERT_FILE=
	HTTPS_KEY_FILE=
	LISTEN_DOMAINS=

# Cluster configuration
CLUSTER_PEERS=localhost:8889,localhost:8890
CLUSTER_PORT=9000
CLUSTER_LISTEN_ADDR=[::1]

# IP Whitelisting (comma-separated, default allows all: ::, 0.0.0.0)
# Leave empty to allow all IPs
HTTP_WHITELIST_IPS=::,0.0.0.0,127.0.0.1,::1
CLUSTER_WHITELIST_IPS=::,0.0.0.0,127.0.0.1,::1

# Security: AES-256 encryption key (32 bytes in base64)
CLUSTER_CIPHER_KEY=%s

# Security: Shared peer secret for authentication
CLUSTER_PEER_SECRET=%s

# HTTP server configuration
# WebSocket configuration
WS_ENABLED=true

# TCP protocol configuration (direct cluster communication)
TCP_ENABLED=true
TCP_PORT=9001
TCP_LISTEN_ADDR=0.0.0.0
TCP_KEY=%s
TCP_TIMEOUT=30
`, cipherKey, peerSecret, tcpKey)

	if err := os.WriteFile(path, []byte(defaultEnv), 0600); err != nil {
		return fmt.Errorf("failed to create .env file: %w", err)
	}

	log.Printf("Created default .env file at %s with generated keys", path)
	return nil
}

func envBool(value string, defaultValue bool) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return defaultValue
	}
	return v == "true" || v == "1" || v == "yes" || v == "on"
}

func normalizeDomain(d string) string {
	d = strings.ToLower(strings.TrimSpace(d))
	d = strings.TrimPrefix(d, "[")
	d = strings.TrimSuffix(d, "]")
	return d
}

func parseDomainList(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	seen := make(map[string]struct{})
	for _, p := range parts {
		d := normalizeDomain(p)
		if d == "" {
			continue
		}
		if _, ok := seen[d]; ok {
			continue
		}
		seen[d] = struct{}{}
		result = append(result, d)
	}
	return result
}

func parseDomainsFromCertificate(certFilePath string) ([]string, error) {
	certPEM, err := os.ReadFile(certFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read certificate file: %w", err)
	}

	var cert *x509.Certificate
	for len(certPEM) > 0 {
		var block *pem.Block
		block, certPEM = pem.Decode(certPEM)
		if block == nil {
			break
		}
		if block.Type != "CERTIFICATE" {
			continue
		}

		parsed, parseErr := x509.ParseCertificate(block.Bytes)
		if parseErr != nil {
			continue
		}
		cert = parsed
		break
	}

	if cert == nil {
		return nil, fmt.Errorf("no valid certificate found in %s", certFilePath)
	}

	domains := make([]string, 0, len(cert.DNSNames)+len(cert.IPAddresses)+1)
	domains = append(domains, cert.DNSNames...)
	for _, ip := range cert.IPAddresses {
		domains = append(domains, ip.String())
	}
	if cert.Subject.CommonName != "" {
		domains = append(domains, cert.Subject.CommonName)
	}

	unique := make([]string, 0, len(domains))
	seen := make(map[string]struct{})
	for _, d := range domains {
		n := normalizeDomain(d)
		if n == "" {
			continue
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		unique = append(unique, n)
	}

	return unique, nil
}

func extractHostFromRequestHost(hostport string) string {
	h := strings.TrimSpace(hostport)
	if h == "" {
		return ""
	}

	if strings.HasPrefix(h, "[") {
		if end := strings.Index(h, "]"); end > 1 {
			return strings.ToLower(h[1:end])
		}
	}

	if host, _, err := net.SplitHostPort(h); err == nil {
		return normalizeDomain(host)
	}

	return normalizeDomain(h)
}

func StartHTTPServer(loadLastConfig bool) error {
	// Cluster is already initialized in server.go
	// Load env for port and listen address
	env := LoadEnvFile(".env")

	// Get admin credentials from .env
	adminUsername := env["ADMIN_USERNAME"]
	adminPassword := env["ADMIN_PASSWORD"]
	if adminUsername == "" {
		adminUsername = "admin"
	}
	if adminPassword == "" {
		adminPassword = "changeme"
		log.Printf("[HTTP] Warning: Using default admin password. Please set ADMIN_PASSWORD in .env")
	}

	// Create runtime directory for HTTP server
	if err := GetClusterInstance().CreateRuntimeDirectory(); err != nil {
		return fmt.Errorf("failed to create runtime directory: %v", err)
	}

	httpEnabled := envBool(env["HTTP_ENABLED"], true)
	httpsEnabled := envBool(env["HTTPS_ENABLED"], false)
	if !httpEnabled && !httpsEnabled {
		return fmt.Errorf("both HTTP and HTTPS are disabled")
	}

	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = env["HTTP_PORT"]
	}
	if httpPort == "" {
		httpPort = "8888"
	}

	httpListenAddr := env["HTTP_LISTEN_ADDR"]
	if httpListenAddr == "" {
		httpListenAddr = "[::]"
	}

	httpsPort := env["HTTPS_PORT"]
	if httpsPort == "" {
		httpsPort = "8443"
	}

	httpsListenAddr := env["HTTPS_LISTEN_ADDR"]
	if httpsListenAddr == "" {
		httpsListenAddr = "[::]"
	}

	httpsCertFile := strings.TrimSpace(env["HTTPS_CERT_FILE"])
	httpsKeyFile := strings.TrimSpace(env["HTTPS_KEY_FILE"])
	if httpsEnabled {
		if httpsCertFile == "" || httpsKeyFile == "" {
			return fmt.Errorf("HTTPS_ENABLED=true requires HTTPS_CERT_FILE and HTTPS_KEY_FILE")
		}
	}

	allowedDomains := parseDomainList(env["LISTEN_DOMAINS"])
	if httpsEnabled && len(allowedDomains) == 0 {
		certDomains, err := parseDomainsFromCertificate(httpsCertFile)
		if err != nil {
			return fmt.Errorf("failed to parse certificate domains: %w", err)
		}
		allowedDomains = certDomains
		if len(allowedDomains) > 0 {
			log.Printf("[HTTP] LISTEN_DOMAINS auto-detected from certificate: %s", strings.Join(allowedDomains, ","))
		}
	}

	allowedDomainSet := make(map[string]struct{}, len(allowedDomains))
	for _, d := range allowedDomains {
		allowedDomainSet[d] = struct{}{}
	}
	enforceHostHeader := len(allowedDomainSet) > 0
	if enforceHostHeader {
		log.Printf("[HTTP] Virtual host filtering enabled for domains: %s", strings.Join(allowedDomains, ","))
	}

	mux := http.NewServeMux()

	// Simple authentication check helper - returns true if valid credentials provided
	checkAuth := func(r *http.Request) bool {
		// Check for Basic Auth
		username, password, ok := r.BasicAuth()
		if !ok {
			return false
		}
		return username == adminUsername && password == adminPassword
	}

	// IP whitelist check middleware wrapper
	ipCheckMiddleware := func(handler http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if enforceHostHeader {
				host := extractHostFromRequestHost(r.Host)
				if _, ok := allowedDomainSet[host]; !ok {
					http.Error(w, "Host not allowed", http.StatusForbidden)
					log.Printf("[HTTP] Rejected request for disallowed host: %s (remote: %s)", r.Host, r.RemoteAddr)
					return
				}
			}

			if !GetClusterInstance().isIPAllowed(r.RemoteAddr, GetClusterInstance().httpWhitelist) {
				http.Error(w, "Access denied: IP not whitelisted", http.StatusForbidden)
				log.Printf("[HTTP] Rejected request from %s: IP not whitelisted", r.RemoteAddr)
				return
			}
			handler(w, r)
		}
	}

	// Writes all arrays + metrics, or one specific array if ?array=<name> is provided.
	// If valid credentials provided via Basic Auth, returns private data as well.
	writeArraysResponse := func(w http.ResponseWriter, r *http.Request) {
		arrayName := strings.TrimSpace(r.URL.Query().Get("array"))

		// Check if valid credentials are provided
		isAuthenticated := checkAuth(r)

		w.Header().Set("Content-Type", "application/json")
		if arrayName == "" {
			// Return all arrays with metrics
			if isAuthenticated {
				json.NewEncoder(w).Encode(GetAllWithMetricsAndPrivate())
			} else {
				json.NewEncoder(w).Encode(GetAllWithMetrics())
			}
			return
		}

		// Return specific array (public only for now, could be extended)
		json.NewEncoder(w).Encode(GetArray(arrayName))
	}

	mux.HandleFunc("/", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		writeArraysResponse(w, r)
	}))

	// Test endpoint: POST /cluster/write?array=<name>&key=<key>&value=<value>
	mux.HandleFunc("/cluster/write", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		arrayName := r.URL.Query().Get("array")
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		if arrayName == "" || key == "" || value == "" {
			http.Error(w, "Missing array, key or value parameter", http.StatusBadRequest)
			return
		}

		if err := Write(arrayName, key+": "+value); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}))

	// Test endpoint: GET /cluster/read?array=<name>&key=<key>
	mux.HandleFunc("/cluster/read", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		arrayName := r.URL.Query().Get("array")
		key := r.URL.Query().Get("key")

		if arrayName == "" || key == "" {
			http.Error(w, "Missing array or key parameter", http.StatusBadRequest)
			return
		}

		value, err := Read(arrayName, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": value})
	}))

	// Test endpoint: GET /cluster/all - returns all arrays with metrics
	mux.HandleFunc("/cluster/all", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		writeArraysResponse(w, r)
	}))

	// Private array endpoints (require authentication)
	// POST /cluster/private/write?array=<name>&key=<key>&value=<value>
	mux.HandleFunc("/cluster/private/write", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if !checkAuth(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Private Array Access"`)
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		arrayName := r.URL.Query().Get("array")
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		if arrayName == "" || key == "" || value == "" {
			http.Error(w, "Missing array, key or value parameter", http.StatusBadRequest)
			return
		}

		if err := WritePrivate(arrayName, key+": "+value); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}))

	// GET /cluster/private/read?array=<name>&key=<key>
	mux.HandleFunc("/cluster/private/read", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Private Array Access"`)
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		arrayName := r.URL.Query().Get("array")
		key := r.URL.Query().Get("key")

		if arrayName == "" || key == "" {
			http.Error(w, "Missing array or key parameter", http.StatusBadRequest)
			return
		}

		value, err := ReadPrivate(arrayName, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"value": value})
	}))

	// DELETE /cluster/private/delete?array=<name>&key=<key>
	mux.HandleFunc("/cluster/private/delete", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete && r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if !checkAuth(r) {
			w.Header().Set("WWW-Authenticate", `Basic realm="Private Array Access"`)
			http.Error(w, "Authentication required", http.StatusUnauthorized)
			return
		}

		arrayName := r.URL.Query().Get("array")
		key := r.URL.Query().Get("key")

		if arrayName == "" {
			http.Error(w, "Missing array parameter", http.StatusBadRequest)
			return
		}

		var err error
		if key == "" {
			// Delete entire private array
			err = DeletePrivateArray(arrayName)
		} else {
			// Delete specific key
			err = DeletePrivate(arrayName, key)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}))

	// WebSocket endpoint for real-time array updates
	if GetClusterInstance().WsEnabled {
		mux.HandleFunc("/ws", HandleWebSocket)
	}

	// DELETE endpoint: DELETE /cluster/delete?array=<name>&key=<key>
	mux.HandleFunc("/cluster/delete", ipCheckMiddleware(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete && r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		arrayName := r.URL.Query().Get("array")
		key := r.URL.Query().Get("key")

		if arrayName == "" {
			http.Error(w, "Missing array parameter", http.StatusBadRequest)
			return
		}

		var err error
		if key == "" {
			// Delete entire array
			err = DeleteArray(arrayName)
		} else {
			// Delete specific key
			err = Delete(arrayName, key)
		}

		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "success"})
	}))

	makeServer := func(addr string) *http.Server {
		return &http.Server{
			Addr:              addr,
			Handler:           mux,
			ReadHeaderTimeout: 2 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      10 * time.Second,
			IdleTimeout:       60 * time.Second,
			MaxHeaderBytes:    1 << 20, // 1 MB
		}
	}

	errCh := make(chan error, 2)
	started := 0

	if httpEnabled {
		started++
		httpAddr := fmt.Sprintf("%s:%s", httpListenAddr, httpPort)
		httpServer := makeServer(httpAddr)
		go func() {
			log.Printf("Starting HTTP server on %s", httpAddr)
			if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				errCh <- fmt.Errorf("http listen failed: %w", err)
			}
		}()
	}

	if httpsEnabled {
		started++
		httpsAddr := fmt.Sprintf("%s:%s", httpsListenAddr, httpsPort)
		httpsServer := makeServer(httpsAddr)
		go func() {
			log.Printf("Starting HTTPS server on %s (cert: %s)", httpsAddr, httpsCertFile)
			if err := httpsServer.ListenAndServeTLS(httpsCertFile, httpsKeyFile); err != nil && err != http.ErrServerClosed {
				errCh <- fmt.Errorf("https listen failed: %w", err)
			}
		}()
	}

	if started == 0 {
		return fmt.Errorf("no HTTP/HTTPS listener started")
	}

	return <-errCh
}
