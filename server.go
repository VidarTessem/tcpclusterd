package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"cluster/modules"
)

func parseEnvBool(value string, defaultValue bool) bool {
	v := strings.ToLower(strings.TrimSpace(value))
	if v == "" {
		return defaultValue
	}
	return v == "true" || v == "1" || v == "yes" || v == "on"
}

func main() {
	// Check for --init or init flag first (before other checks)
	initMode := false
	if len(os.Args) > 1 {
		if os.Args[1] == "--init" || os.Args[1] == "init" {
			initMode = true
		}
	}

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
		fmt.Fprintf(os.Stderr, "Please run: %s init\n", os.Args[0])
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

		// Skip if --recover (not a command)
		if command == "--recover" {
			// Continue to start server with recovery from disk
		} else {
			switch command {
			case "export":
				// Load environment and initialize cluster (without creating new runtime dir)
				env := modules.LoadEnvFile(".env")
				modules.InitClusterArray(env, true) // Load from last runtime
				// Don't create a new runtime directory for export
				if err := modules.Export(); err != nil {
					log.Fatalf("export failed: %v", err)
				}
				return

			case "import":
				// Load environment before importing
				env := modules.LoadEnvFile(".env")
				modules.InitClusterArray(env, false)
				// Import needs a writable runtime directory
				if err := modules.GetClusterInstance().CreateRuntimeDirectory(); err != nil {
					log.Fatalf("import failed to create runtime directory: %v", err)
				}
				clearFirst := false
				if len(os.Args) > 2 && os.Args[2] == "--clear" {
					clearFirst = true
				}
				if err := modules.Import(clearFirst); err != nil {
					log.Fatalf("import failed: %v", err)
				}
				return

			default:
				fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
				fmt.Fprintf(os.Stderr, "Usage:\n")
				fmt.Fprintf(os.Stderr, "  %s              - start server (syncs from peers only)\n", os.Args[0])
				fmt.Fprintf(os.Stderr, "  %s init         - initialize .env file with defaults\n", os.Args[0])
				fmt.Fprintf(os.Stderr, "  %s --recover    - start server and recover from last disk state\n", os.Args[0])
				fmt.Fprintf(os.Stderr, "  %s export       - export all arrays (public + private) to stdout\n", os.Args[0])
				fmt.Fprintf(os.Stderr, "  %s import       - import arrays from stdin\n", os.Args[0])
				fmt.Fprintf(os.Stderr, "  %s import --clear - clear and import arrays from stdin\n", os.Args[0])
				os.Exit(1)
			}
		}
	}

	// Load environment and initialize cluster
	env := modules.LoadEnvFile(".env")
	modules.InitClusterArray(env, loadLastConfig)
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
