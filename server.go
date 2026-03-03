package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"cluster/modules"
)

func parseEnvBool(value string, defaultValue bool) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return defaultValue
	}
	return v == "true" || v == "1" || v == "yes" || v == "on"
}

type controlRequest struct {
	Command     string `json:"command"`
	TargetArray string `json:"target_array,omitempty"`
	ClearFirst  bool   `json:"clear_first,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
}

type controlResponse struct {
	OK      bool   `json:"ok"`
	Output  string `json:"output,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

func getControlSocketPath(env map[string]string) string {
	if p := strings.TrimSpace(env["CONTROL_SOCKET"]); p != "" {
		return p
	}
	return "/tmp/tcpclusterd.sock"
}

func isSocketServerRunning(socketPath string) bool {
	conn, err := net.DialTimeout("unix", socketPath, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(1 * time.Second))
	req := controlRequest{Command: "--ping"}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return false
	}

	var resp controlResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return false
	}
	return resp.OK
}

func sendCommandViaSocket(env map[string]string, command string, args []string) error {
	socketPath := getControlSocketPath(env)
	conn, err := net.DialTimeout("unix", socketPath, 2*time.Second)
	if err != nil {
		return fmt.Errorf("failed connecting to running instance: %v", err)
	}
	defer conn.Close()

	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	req := controlRequest{Command: command}

	switch command {
	case "--export":
		// no payload
	case "--recover":
		// no payload
	case "--import":
		for _, arg := range args {
			if arg == "--clear" {
				req.ClearFirst = true
				break
			}
		}
		var body []byte
		var err error
		fileArg := ""
		for _, arg := range args {
			if arg != "--clear" && strings.TrimSpace(arg) != "" {
				fileArg = arg
				break
			}
		}
		if fileArg != "" {
			body, err = os.ReadFile(fileArg)
			if err != nil {
				return fmt.Errorf("failed reading import file '%s': %v", fileArg, err)
			}
		} else {
			body, err = io.ReadAll(os.Stdin)
			if err != nil {
				return fmt.Errorf("failed reading stdin: %v", err)
			}
		}
		req.Payload = body
	case "--importfile":
		if len(args) < 2 {
			return fmt.Errorf("missing arguments for --importfile")
		}
		req.TargetArray = args[0]
		filePath := args[1]
		body, err := os.ReadFile(filePath)
		if err != nil {
			return fmt.Errorf("failed reading file '%s': %v", filePath, err)
		}
		req.Payload = body
	default:
		return fmt.Errorf("command %s not supported over control socket", command)
	}

	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return fmt.Errorf("failed sending request: %v", err)
	}

	var resp controlResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return fmt.Errorf("failed reading response: %v", err)
	}

	if !resp.OK {
		if resp.Error == "" {
			resp.Error = "unknown error"
		}
		return fmt.Errorf(resp.Error)
	}

	if resp.Output != "" {
		fmt.Print(resp.Output)
	}
	if resp.Message != "" {
		fmt.Println(resp.Message)
	}

	return nil
}

func startControlSocketServer(env map[string]string) error {
	socketPath := getControlSocketPath(env)
	if err := os.MkdirAll(filepath.Dir(socketPath), 0755); err != nil {
		return fmt.Errorf("failed creating socket directory: %v", err)
	}

	if _, err := os.Stat(socketPath); err == nil {
		if isSocketServerRunning(socketPath) {
			return fmt.Errorf("control socket already in use at %s", socketPath)
		}
		_ = os.Remove(socketPath)
	}

	ln, err := net.Listen("unix", socketPath)
	if err != nil {
		return fmt.Errorf("failed listening on control socket %s: %v", socketPath, err)
	}
	if err := os.Chmod(socketPath, 0600); err != nil {
		log.Printf("warning: could not set socket permissions: %v", err)
	}

	go func() {
		defer ln.Close()
		for {
			conn, err := ln.Accept()
			if err != nil {
				continue
			}
			go handleControlSocketConn(conn)
		}
	}()

	log.Printf("[CONTROL] Listening on unix socket %s", socketPath)
	return nil
}

func handleControlSocketConn(conn net.Conn) {
	defer conn.Close()

	writeResponse := func(resp controlResponse) {
		enc := json.NewEncoder(conn)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(resp)
	}

	var req controlRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		writeResponse(controlResponse{OK: false, Error: fmt.Sprintf("invalid request: %v", err)})
		return
	}

	respondErr := func(err error) {
		writeResponse(controlResponse{OK: false, Error: err.Error()})
	}

	switch req.Command {
	case "--ping":
		writeResponse(controlResponse{OK: true, Message: "pong"})
	case "--recover":
		if err := modules.RecoverFromDisk(); err != nil {
			respondErr(fmt.Errorf("recover failed: %v", err))
			return
		}
		writeResponse(controlResponse{OK: true, Message: "✓ recovered running server from disk"})
	case "--export":
		payload := modules.GetAllWithMetricsAndPrivate()
		buf := &bytes.Buffer{}
		enc := json.NewEncoder(buf)
		enc.SetIndent("", "  ")
		enc.SetEscapeHTML(false)
		if err := enc.Encode(payload); err != nil {
			respondErr(fmt.Errorf("failed encoding export: %v", err))
			return
		}
		writeResponse(controlResponse{OK: true, Output: buf.String()})
	case "--import":
		if len(req.Payload) == 0 {
			respondErr(fmt.Errorf("import payload is empty"))
			return
		}
		if err := modules.ImportFromBytes(req.Payload, req.ClearFirst); err != nil {
			respondErr(fmt.Errorf("import failed: %v", err))
			return
		}
		writeResponse(controlResponse{OK: true, Message: "✓ imported to running server"})
	case "--importfile":
		if req.TargetArray == "" {
			respondErr(fmt.Errorf("target array is required"))
			return
		}
		if len(req.Payload) == 0 {
			respondErr(fmt.Errorf("importfile payload is empty"))
			return
		}
		if err := modules.ImportJSONToArray(req.TargetArray, req.Payload); err != nil {
			respondErr(fmt.Errorf("importfile failed: %v", err))
			return
		}
		writeResponse(controlResponse{OK: true, Message: fmt.Sprintf("✓ imported payload -> %s", req.TargetArray)})
	default:
		respondErr(fmt.Errorf("unsupported command: %s", req.Command))
	}
}

func triggerReload(env map[string]string) {
	// Try to trigger reload on running server via HTTP
	httpPort := env["HTTP_PORT"]
	if httpPort == "" {
		httpPort = "8080"
	}

	// Use localhost instead of listen addr
	reloadURL := fmt.Sprintf("http://127.0.0.1:%s/cluster/reload", httpPort)

	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest("POST", reloadURL, bytes.NewReader([]byte{}))
	if err != nil {
		return // Silently skip if can't create request
	}

	// Add auth if configured
	if httpKey := env["HTTP_KEY"]; httpKey != "" {
		req.Header.Set("X-HTTP-Key", httpKey)
	}

	resp, err := client.Do(req)
	if err != nil {
		return // Silently skip if server not running
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("✓ server reloaded")
	}
}

func printUsage(app string) {
	fmt.Fprintf(os.Stderr, "Usage:\n")
	fmt.Fprintf(os.Stderr, "  %s                        - start server (syncs from peers only)\n", app)
	fmt.Fprintf(os.Stderr, "  %s --init                 - initialize .env file with defaults\n", app)
	fmt.Fprintf(os.Stderr, "  %s --recover              - recover from disk (or start+recover if no server is running)\n", app)
	fmt.Fprintf(os.Stderr, "  %s --export               - export all arrays (public + private) to stdout\n", app)
	fmt.Fprintf(os.Stderr, "  %s --import [--clear] [file.json] - import arrays from stdin or file (merge by default)\n", app)
	fmt.Fprintf(os.Stderr, "  %s --importfile <PUBLIC:array|PRIVATE:array> <file.json>\n", app)
}

func main() {
	app := os.Args[0]

	// Enforce that all command arguments use -- prefix.
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "--") {
		fmt.Fprintf(os.Stderr, "Invalid command format: %s\n", os.Args[1])
		fmt.Fprintf(os.Stderr, "Commands must start with '--'.\n")
		printUsage(app)
		os.Exit(1)
	}

	// Check for --init first (before .env check)
	initMode := len(os.Args) > 1 && os.Args[1] == "--init"

	// Handle init command
	if initMode {
		if err := modules.InitializeEnv(".env"); err != nil {
			log.Fatalf("initialization failed: %v", err)
		}
		fmt.Println("✓ .env file created successfully")
		return
	}

	// Check if .env exists (required for all other operations)
	if _, err := os.Stat(".env"); err != nil {
		fmt.Fprintf(os.Stderr, "Error: .env file not found\n")
		fmt.Fprintf(os.Stderr, "Please run: %s --init\n", app)
		os.Exit(1)
	}

	// Check for --recover flag
	loadLastConfig := false
	for _, arg := range os.Args[1:] {
		if arg == "--recover" {
			loadLastConfig = true
			break
		}
	}

	// Handle command-line arguments
	if len(os.Args) > 1 {
		command := os.Args[1]

		// Load environment
		env := modules.LoadEnvFile(".env")

		// If local control socket is up, send command to running server instance.
		switch command {
		case "--export", "--import", "--importfile", "--recover":
			if isSocketServerRunning(getControlSocketPath(env)) {
				if err := sendCommandViaSocket(env, command, os.Args[2:]); err != nil {
					log.Fatalf("failed: %v", err)
				}
				return
			}
			// Fall through to standalone mode if no server socket is available.
		}

		// For --recover without running server: continue startup flow below (start + recover).
		if command != "--recover" {
			// CLI mode for commands
			switch command {
			case "--export":
				env["NO_LISTEN"] = "true"
				modules.InitClusterArray(env, true) // Load from last runtime
				// Don't create a new runtime directory for export
				if err := modules.Export(); err != nil {
					log.Fatalf("export failed: %v", err)
				}
				return

			case "--import":
				// Load environment before importing
				env := modules.LoadEnvFile(".env")
				env["NO_LISTEN"] = "true"
				// Load existing data so import merges unless --clear is specified.
				modules.InitClusterArray(env, true)
				if err := modules.GetClusterInstance().EnsureRuntimeDirectory(); err != nil {
					log.Fatalf("import failed to ensure runtime directory: %v", err)
				}
				clearFirst := false
				importFile := ""
				for _, arg := range os.Args[2:] {
					if arg == "--clear" {
						clearFirst = true
						continue
					}
					if strings.TrimSpace(arg) != "" && importFile == "" {
						importFile = arg
					}
				}

				if importFile != "" {
					payload, err := os.ReadFile(importFile)
					if err != nil {
						log.Fatalf("import failed reading file '%s': %v", importFile, err)
					}
					if err := modules.ImportFromBytes(payload, clearFirst); err != nil {
						log.Fatalf("import failed: %v", err)
					}
				} else if err := modules.Import(clearFirst); err != nil {
					log.Fatalf("import failed: %v", err)
				}

				// Trigger reload on running server via HTTP
				triggerReload(env)
				return

			case "--importfile":
				if len(os.Args) < 4 {
					fmt.Fprintf(os.Stderr, "Usage: %s --importfile <PUBLIC:array|PRIVATE:array> <file.json>\n", app)
					os.Exit(1)
				}

				targetArray := os.Args[2]
				filePath := os.Args[3]
				if !(strings.HasPrefix(strings.ToUpper(targetArray), "PUBLIC:") || strings.HasPrefix(strings.ToUpper(targetArray), "PRIVATE:")) {
					log.Fatalf("importfile failed: target must start with PUBLIC: or PRIVATE:")
				}

				env := modules.LoadEnvFile(".env")
				env["NO_LISTEN"] = "true"
				// Load existing arrays so importfile does not implicitly clear existing data.
				modules.InitClusterArray(env, true)

				// importfile persists, so it needs an active runtime directory.
				if err := modules.GetClusterInstance().EnsureRuntimeDirectory(); err != nil {
					log.Fatalf("importfile failed to ensure runtime directory: %v", err)
				}

				if err := modules.ImportFileToArray(targetArray, filePath); err != nil {
					log.Fatalf("importfile failed: %v", err)
				}
				fmt.Printf("✓ imported %s -> %s\n", filePath, targetArray)

				// Trigger reload on running server via HTTP
				triggerReload(env)
				return

			default:
				fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
				printUsage(app)
				os.Exit(1)
			}
		}
	}

	// Load environment and initialize cluster
	env := modules.LoadEnvFile(".env")
	modules.InitClusterArray(env, loadLastConfig)
	if err := startControlSocketServer(env); err != nil {
		log.Fatalf("failed to start control socket: %v", err)
	}
	httpsEnabled := parseEnvBool(env["HTTPS_ENABLED"], false)
	tcpEnabled := parseEnvBool(env["TCP_ENABLED"], true)
	httpEnabled := modules.GetClusterInstance().HttpEnabled

	if !httpEnabled && !httpsEnabled && !tcpEnabled {
		log.Fatalf("startup aborted: HTTP, HTTPS, and TCP are all disabled")
	}

	// Start HTTP/HTTPS server if at least one is enabled (it will create runtime directory)
	if httpEnabled || httpsEnabled {
		if err := modules.StartHTTPServer(loadLastConfig); err != nil {
			log.Fatalf("server failed: %v", err)
		}
	} else {
		log.Printf("HTTP and HTTPS servers disabled; running with remaining protocols")
		// Create runtime directory even if HTTP is disabled
		if err := modules.GetClusterInstance().CreateRuntimeDirectory(); err != nil {
			log.Fatalf("failed to create runtime directory: %v", err)
		}
		// If HTTP disabled but TCP/WebSocket needed, keep the process alive
		// by blocking indefinitely
		select {}
	}
}
