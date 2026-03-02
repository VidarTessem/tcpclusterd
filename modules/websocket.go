package modules

import (
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
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
// If array is specified, sends only that array
// If interval is specified, pushes at that interval (ms)
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

	interval := 1000 * time.Millisecond // Default 1 second
	if intervalStr != "" {
		if ms, err := strconv.ParseInt(intervalStr, 10, 64); err == nil && ms > 0 {
			interval = time.Duration(ms) * time.Millisecond
		}
	}

	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Send initial data
	sendWSData(conn, arrayName)

	for {
		select {
		case <-ticker.C:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := sendWSData(conn, arrayName); err != nil {
				log.Printf("[WS] Send error: %v\n", err)
				IncrementErrorCount()
				return
			}

		default:
			// Read control messages (pings, closes, etc)
			conn.SetReadDeadline(time.Now().Add(60 * time.Second))
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					log.Printf("[WS] Connection error: %v\n", err)
				}
				return
			}
		}
	}
}

func sendWSData(conn *websocket.Conn, arrayName string) error {
	var data interface{}

	if arrayName == "" {
		data = GetAllWithMetrics()
	} else {
		data = GetArray(arrayName)
	}

	return conn.WriteJSON(data)
}
