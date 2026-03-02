package modules

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	wsReadTimeout  = 70 * time.Second
	wsWriteTimeout = 10 * time.Second
	wsPingInterval = 25 * time.Second
)

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		// Allow all origins for now; in production, be more restrictive
		return true
	},
}

// HandleWebSocket handles WebSocket connections
// Query params: ?array=<name>&interval=<ms>
// If array is specified, subscribes to that array's updates
// If interval is NOT specified, uses event-based push (immediate updates)
// If interval IS specified, polls at that interval (legacy mode)
func HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WS] Upgrade error: %v\n", err)
		IncrementErrorCount()
		return
	}
	defer conn.Close()

	arrayName := strings.TrimSpace(r.URL.Query().Get("array"))
	intervalStr := strings.TrimSpace(r.URL.Query().Get("interval"))

	// Determine mode: event-based (default) or polling
	usePolling := false
	var interval time.Duration
	if intervalStr != "" {
		usePolling = true
		interval = 1000 * time.Millisecond
		if ms, err := strconv.ParseInt(intervalStr, 10, 64); err == nil && ms > 0 {
			interval = time.Duration(ms) * time.Millisecond
		}
	}

	conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
	conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		return nil
	})

	if usePolling {
		// Legacy polling mode
		handleWebSocketPolling(conn, arrayName, interval)
	} else {
		// Event-based mode (default)
		handleWebSocketEvents(conn, arrayName)
	}
}

// handleWebSocketEvents uses event-based push (subscribes to updates)
func handleWebSocketEvents(conn *websocket.Conn, arrayName string) {
	// Subscribe to array updates
	updateChan, unsubscribe := GetClusterInstance().SubscribeToArray(arrayName)
	defer unsubscribe()

	// Send initial data
	if err := sendWSData(conn, arrayName); err != nil {
		log.Printf("[WS] Initial send error: %v\n", err)
		IncrementErrorCount()
		return
	}

	pingTicker := time.NewTicker(wsPingInterval)
	defer pingTicker.Stop()

	// Read control messages or wait for updates
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("[WS] Connection error: %v\n", err)
				}
				return
			}
		}
	}()

	// Wait for updates or connection close
	for {
		select {
		case _, ok := <-updateChan:
			if !ok {
				return // Channel closed
			}
			if err := sendWSData(conn, arrayName); err != nil {
				log.Printf("[WS] Send error: %v\n", err)
				IncrementErrorCount()
				return
			}

		case <-pingTicker.C:
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("[WS] Ping error: %v\n", err)
				return
			}

		case <-done:
			return // Connection closed
		}
	}
}

// handleWebSocketPolling uses polling mode (legacy, interval-based)
func handleWebSocketPolling(conn *websocket.Conn, arrayName string, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	pingTicker := time.NewTicker(wsPingInterval)
	defer pingTicker.Stop()

	// Send initial data
	if err := sendWSData(conn, arrayName); err != nil {
		log.Printf("[WS] Initial send error: %v\n", err)
		IncrementErrorCount()
		return
	}

	// Read control messages in background so periodic sends are not blocked
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("[WS] Connection error: %v\n", err)
				}
				return
			}
		}
	}()

	for {
		select {
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := sendWSData(conn, arrayName); err != nil {
				log.Printf("[WS] Send error: %v\n", err)
				IncrementErrorCount()
				return
			}

		case <-pingTicker.C:
			conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				log.Printf("[WS] Ping error: %v\n", err)
				return
			}

		case <-done:
			return
		}
	}
}

func sendWSData(conn *websocket.Conn, arrayName string) error {
	var data interface{}

	if arrayName == "" {
		data = GetAllWithMetrics()
	} else {
		data = map[string]interface{}{
			"arrays": map[string]map[string]string{
				arrayName: GetArray(arrayName),
			},
			"metrics": GetClusterInstance().GetMetrics(),
		}
	}

	conn.SetWriteDeadline(time.Now().Add(wsWriteTimeout))

	return conn.WriteJSON(data)
}
