package modules

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

// StartTCPServer starts the direct TCP protocol server
func (c *Cluster) StartTCPServer(port, listenAddr, key string) {
	addr := fmt.Sprintf("%s:%s", listenAddr, port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("[TCP] Error starting TCP server: %v\n", err)
		IncrementErrorCount()
		return
	}
	defer listener.Close()

	log.Printf("[TCP] Server started on %s\n", addr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[TCP] Accept error: %v\n", err)
			IncrementErrorCount()
			continue
		}

		go c.handleTCPConnection(conn, key)
	}
}

func (c *Cluster) handleTCPConnection(conn net.Conn, key string) {
	defer conn.Close()

	// Check if client IP is whitelisted
	if !c.isIPAllowed(conn.RemoteAddr().String(), c.tcpWhitelist) {
		fmt.Fprintf(conn, "ERROR: Access denied\n")
		log.Printf("[TCP] Rejected connection from %s: IP not whitelisted\n", conn.RemoteAddr())
		return
	}

	conn.SetReadDeadline(time.Now().Add(c.tcpTimeout))

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	// Track authentication levels:
	// 0 = no auth (public only)
	// 1 = TCP key auth (public only)
	// 2 = TCP key + admin credentials (public + private)
	authLevel := 0

	// Expect AUTH command first (optional for public access)
	line, err := reader.ReadString('\n')
	if err != nil {
		return
	}

	line = strings.TrimSpace(line)
	parts := strings.SplitN(line, " ", 3)

	if len(parts) >= 2 && parts[0] == "AUTH" {
		if parts[1] == key {
			authLevel = 1
			// Check for admin credentials (format: AUTH <tcp_key> <username>:<password>)
			if len(parts) == 3 {
				creds := strings.SplitN(parts[2], ":", 2)
				if len(creds) == 2 {
					// Check admin credentials from Cluster struct (loaded at startup)
					if creds[0] == c.adminUsername && creds[1] == c.adminPassword {
						authLevel = 2
						log.Printf("[TCP] Client authenticated with admin access from %s\n", conn.RemoteAddr())
					} else {
						log.Printf("[TCP] Invalid admin credentials from %s\n", conn.RemoteAddr())
					}
				}
			}
			if authLevel == 1 {
				log.Printf("[TCP] Client authenticated with public access from %s\n", conn.RemoteAddr())
			}
			fmt.Fprintf(writer, "OK\n")
		} else {
			// Invalid TCP key - allow public access only
			authLevel = 0
			fmt.Fprintf(writer, "OK PUBLIC_ONLY\n")
			log.Printf("[TCP] Invalid TCP key from %s, public access only\n", conn.RemoteAddr())
		}
	} else {
		// No AUTH command - public access only
		authLevel = 0
		fmt.Fprintf(writer, "OK PUBLIC_ONLY\n")
		log.Printf("[TCP] No authentication from %s, public access only\n", conn.RemoteAddr())
	}

	writer.Flush()

	// Handle commands
	for {
		conn.SetReadDeadline(time.Now().Add(c.tcpTimeout))
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 4)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "GET":
			c.handleTCPGet(writer, parts, authLevel)

		case "SET":
			c.handleTCPSet(writer, parts, authLevel)

		case "DEL":
			c.handleTCPDel(writer, parts, authLevel)

		case "DELARRAY":
			c.handleTCPDelArray(writer, parts, authLevel)

		case "QUIT":
			fmt.Fprintf(writer, "BYE\n")
			writer.Flush()
			return

		default:
			fmt.Fprintf(writer, "ERROR: Unknown command\n")
		}

		writer.Flush()
	}
}

func (c *Cluster) handleTCPGet(writer *bufio.Writer, parts []string, authLevel int) {
	var data interface{}

	if len(parts) < 2 {
		// Return all arrays based on auth level
		if authLevel >= 2 {
			data = GetAllWithMetricsAndPrivate()
		} else {
			data = GetAllWithMetrics()
		}
	} else {
		arrayName := strings.TrimSpace(parts[1])
		data = GetArray(arrayName)
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		fmt.Fprintf(writer, "ERROR: %v\n", err)
		return
	}

	fmt.Fprintf(writer, "OK ")
	writer.Write(jsonData)
	fmt.Fprintf(writer, "\n")
}

func (c *Cluster) handleTCPSet(writer *bufio.Writer, parts []string, authLevel int) {
	if len(parts) < 4 {
		fmt.Fprintf(writer, "ERROR: SET requires array, key, and value\n")
		return
	}

	arrayName := strings.TrimSpace(parts[1])
	key := strings.TrimSpace(parts[2])
	value := strings.TrimSpace(parts[3])

	// Check if setting to private array (prefix with PRIVATE:)
	if strings.HasPrefix(strings.ToUpper(arrayName), "PRIVATE:") {
		if authLevel < 2 {
			fmt.Fprintf(writer, "ERROR: Authentication required for private arrays\n")
			return
		}
		arrayName = strings.TrimPrefix(arrayName, "PRIVATE:")
		arrayName = strings.TrimPrefix(arrayName, "private:")
		if err := WritePrivate(arrayName, key+": "+value); err != nil {
			fmt.Fprintf(writer, "ERROR: %v\n", err)
			return
		}
	} else {
		if err := Write(arrayName, key+": "+value); err != nil {
			fmt.Fprintf(writer, "ERROR: %v\n", err)
			return
		}
	}

	fmt.Fprintf(writer, "OK\n")
}

func (c *Cluster) handleTCPDel(writer *bufio.Writer, parts []string, authLevel int) {
	if len(parts) < 3 {
		fmt.Fprintf(writer, "ERROR: DEL requires array and key\n")
		return
	}

	arrayName := strings.TrimSpace(parts[1])
	key := strings.TrimSpace(parts[2])

	// Check if deleting from private array
	if strings.HasPrefix(strings.ToUpper(arrayName), "PRIVATE:") {
		if authLevel < 2 {
			fmt.Fprintf(writer, "ERROR: Authentication required for private arrays\n")
			return
		}
		arrayName = strings.TrimPrefix(arrayName, "PRIVATE:")
		arrayName = strings.TrimPrefix(arrayName, "private:")
		if err := DeletePrivate(arrayName, key); err != nil {
			fmt.Fprintf(writer, "ERROR: %v\n", err)
			return
		}
	} else {
		if err := Delete(arrayName, key); err != nil {
			fmt.Fprintf(writer, "ERROR: %v\n", err)
			return
		}
	}

	fmt.Fprintf(writer, "OK\n")
}

func (c *Cluster) handleTCPDelArray(writer *bufio.Writer, parts []string, authLevel int) {
	if len(parts) < 2 {
		fmt.Fprintf(writer, "ERROR: DELARRAY requires array name\n")
		return
	}

	arrayName := strings.TrimSpace(parts[1])

	// Check if deleting private array
	if strings.HasPrefix(strings.ToUpper(arrayName), "PRIVATE:") {
		if authLevel < 2 {
			fmt.Fprintf(writer, "ERROR: Authentication required for private arrays\n")
			return
		}
		arrayName = strings.TrimPrefix(arrayName, "PRIVATE:")
		arrayName = strings.TrimPrefix(arrayName, "private:")
		if err := DeletePrivateArray(arrayName); err != nil {
			fmt.Fprintf(writer, "ERROR: %v\n", err)
			return
		}
	} else {
		if err := DeleteArray(arrayName); err != nil {
			fmt.Fprintf(writer, "ERROR: %v\n", err)
			return
		}
	}

	fmt.Fprintf(writer, "OK\n")
}
