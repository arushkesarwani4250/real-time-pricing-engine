package upstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// ErrAuthRejected indicates that upstream explicitly rejected the credentials.
var ErrAuthRejected = fmt.Errorf("upstream authentication rejected")

// Client manages the upstream WebSocket connection to LiveFXHub.
type Client struct {
	url       string
	apiKey    string
	secretKey string
	symbols   []string

	conn    *websocket.Conn
	writeMu sync.Mutex // Protects concurrent writes to conn (Gorilla WS requirement)
	done    chan struct{}
}

// NewClient initializes a new Upstream Client.
func NewClient(url, apiKey, secretKey string, symbols []string) *Client {
	if len(symbols) == 0 {
		symbols = AllRequiredSymbols
	}
	return &Client{
		url:       url,
		apiKey:    apiKey,
		secretKey: secretKey,
		symbols:   symbols,
		done:      make(chan struct{}),
	}
}

// Connect establishes the WebSocket handshake with the upstream server.
func (c *Client) Connect() error {
	log.Printf("[Upstream] Connecting to %s...", c.url)

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, resp, err := dialer.Dial(c.url, nil)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial failed with status %d: %w", resp.StatusCode, err)
		}
		return fmt.Errorf("dial failed: %w", err)
	}

	c.conn = conn
	log.Println("[Upstream] WebSocket connection established successfully.")
	return nil
}

// Authenticate sends the auth JSON payload required by LiveFXHub.
func (c *Client) Authenticate() error {
	log.Println("[Upstream] Sending authentication request...")

	req := AuthRequest{
		Action: "auth",
		APIKey: c.apiKey,
		Secret: c.secretKey,
	}

	if err := c.writeJSON(req); err != nil {
		return fmt.Errorf("failed to send auth payload: %w", err)
	}

	// Read immediate response to verify authentication result
	_, message, err := c.conn.ReadMessage()
	if err != nil {
		return fmt.Errorf("failed to read auth response: %w", err)
	}

	log.Printf("[Upstream] Auth response: %s", string(message))

	// Check if auth failed
	var res map[string]interface{}
	if err := json.Unmarshal(message, &res); err == nil {
		if event, ok := res["event"].(string); ok && event == "error" {
			return fmt.Errorf("%w: %v", ErrAuthRejected, res["message"])
		}
	}

	return nil
}

// Subscribe sends the symbol subscription request.
func (c *Client) Subscribe() error {
	log.Printf("[Upstream] Subscribing to %d market symbols...", len(c.symbols))

	req := SubscribeRequest{
		Action:  "subscribeMarketPrice",
		Symbols: c.symbols,
	}

	if err := c.writeJSON(req); err != nil {
		return fmt.Errorf("failed to send subscription payload: %w", err)
	}

	log.Println("[Upstream] Subscription request sent.")
	return nil
}

// Listen loops continuously, reading incoming messages and dispatching raw payload bytes to out.
func (c *Client) Listen(ctx context.Context, out chan<- []byte) error {
	defer func() {
		close(c.done)
		if c.conn != nil {
			_ = c.conn.Close()
		}
	}()

	log.Println("[Upstream] Listening for live market price ticks...")

	// Goroutine to watch context cancellation and trigger graceful WebSocket close
	go func() {
		<-ctx.Done()
		c.writeMu.Lock()
		if c.conn != nil {
			_ = c.conn.WriteMessage(
				websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutting down"),
			)
		}
		c.writeMu.Unlock()
	}()

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			select {
			case <-ctx.Done():
				return nil
			default:
				return fmt.Errorf("read error: %w", err)
			}
		}

		select {
		case out <- message:
		case <-ctx.Done():
			return nil
		}
	}
}

// writeJSON provides thread-safe JSON writing to the underlying WebSocket connection.
func (c *Client) writeJSON(v interface{}) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.conn == nil {
		return fmt.Errorf("connection is closed")
	}

	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
