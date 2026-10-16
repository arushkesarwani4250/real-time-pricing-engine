package downstream

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second
	pongWait       = 60 * time.Second
	pingPeriod     = (pongWait * 9) / 10
	maxMessageSize = 1024 * 16 // 16KB max client message
)

// Client represents a single connected downstream WebSocket client session.
type Client struct {
	hub           *Hub
	conn          *websocket.Conn
	send          chan []byte
	subMu         sync.RWMutex
	subscriptions map[string]bool // symbol -> true; "*" means subscribed to all
}

// NewClient initializes a client session.
func NewClient(hub *Hub, conn *websocket.Conn) *Client {
	return &Client{
		hub:           hub,
		conn:          conn,
		send:          make(chan []byte, 256),
		subscriptions: make(map[string]bool),
	}
}

// IsSubscribed checks if the client wants updates for a given symbol.
func (c *Client) IsSubscribed(symbol string) bool {
	c.subMu.RLock()
	defer c.subMu.RUnlock()

	// If subscribed to "*" or no specific filter, receive all
	if c.subscriptions["*"] || len(c.subscriptions) == 0 {
		return true
	}
	return c.subscriptions[symbol]
}

// Subscribe adds symbols to the client's subscription list.
func (c *Client) Subscribe(symbols []string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	if len(symbols) == 0 {
		c.subscriptions["*"] = true
		return
	}

	for _, s := range symbols {
		s = strings.ToUpper(strings.TrimSpace(s))
		if s != "" {
			c.subscriptions[s] = true
		}
	}
}

// Unsubscribe removes symbols from the client's subscription list.
func (c *Client) Unsubscribe(symbols []string) {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	for _, s := range symbols {
		s = strings.ToUpper(strings.TrimSpace(s))
		delete(c.subscriptions, s)
	}
}

// readPump reads incoming subscription requests from the client WebSocket.
func (c *Client) readPump() {
	defer func() {
		c.hub.unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(maxMessageSize)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			break
		}

		var req ClientRequest
		if err := json.Unmarshal(message, &req); err != nil {
			c.sendResponse(NewServerResponse("error", "invalid JSON format", nil))
			continue
		}

		switch strings.ToLower(req.Action) {
		case "subscribe":
			c.Subscribe(req.Symbols)
			c.sendResponse(NewServerResponse("ok", "subscribed successfully", req.Symbols))
		case "unsubscribe":
			c.Unsubscribe(req.Symbols)
			c.sendResponse(NewServerResponse("ok", "unsubscribed successfully", req.Symbols))
		case "snapshot":
			snapshot := c.hub.GetSnapshot()
			c.sendResponse(NewServerResponse("snapshot", "current market snapshot", snapshot))
		default:
			c.sendResponse(NewServerResponse("error", "unknown action. Supported: subscribe, unsubscribe, snapshot", nil))
		}
	}
}

// writePump drains outbound messages from c.send and writes them to WebSocket.
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				// The hub closed the channel
				_ = c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := c.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			_, _ = w.Write(message)

			// Flush any queued messages into the same write buffer for throughput
			n := len(c.send)
			for i := 0; i < n; i++ {
				_, _ = w.Write([]byte("\n"))
				_, _ = w.Write(<-c.send)
			}

			if err := w.Close(); err != nil {
				return
			}

		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// sendResponse writes a JSON response directly into the client's send buffer.
func (c *Client) sendResponse(res *ServerResponse) {
	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("[Client] Failed to marshal response: %v", err)
		return
	}
	select {
	case c.send <- data:
	default:
		// Client buffer full
	}
}
