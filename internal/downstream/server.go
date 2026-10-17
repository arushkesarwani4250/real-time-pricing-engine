package downstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

// makeCheckOrigin creates a configurable origin validator function for WebSocket upgrades.
func makeCheckOrigin(allowedOrigins []string) func(r *http.Request) bool {
	// If wildcard or empty, allow all origins (development mode)
	if len(allowedOrigins) == 0 || (len(allowedOrigins) == 1 && allowedOrigins[0] == "*") {
		return func(r *http.Request) bool { return true }
	}

	allowedMap := make(map[string]bool)
	for _, o := range allowedOrigins {
		allowedMap[strings.ToLower(strings.TrimSpace(o))] = true
	}

	return func(r *http.Request) bool {
		origin := strings.ToLower(r.Header.Get("Origin"))
		if origin == "" {
			return true // Allow non-browser clients (e.g. algo bots, curl, backend services)
		}
		return allowedMap[origin]
	}
}

// Server exposes the downstream HTTP and WebSocket endpoints.
type Server struct {
	port       string
	hub        *Hub
	httpServer *http.Server
	startTime  time.Time
	upgrader   websocket.Upgrader
}

// NewServer initializes the downstream HTTP/WS server with configurable origin checking.
func NewServer(port string, hub *Hub, allowedOrigins []string) *Server {
	return &Server{
		port:      port,
		hub:       hub,
		startTime: time.Now(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024 * 16,
			WriteBufferSize: 1024 * 16,
			CheckOrigin:     makeCheckOrigin(allowedOrigins),
		},
	}
}

// Start begins listening on the configured port.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	mux.HandleFunc("/ws", s.handleWebSocket)
	mux.HandleFunc("/snapshot", s.handleSnapshot)
	mux.HandleFunc("/health", s.handleHealth)

	s.httpServer = &http.Server{
		Addr:         ":" + s.port,
		Handler:      mux,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	log.Printf("[Server] Downstream server listening at http://localhost:%s (WS: ws://localhost:%s/ws)", s.port, s.port)

	go func() {
		<-ctx.Done()
		log.Println("[Server] Shutting down downstream server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.httpServer.Shutdown(shutdownCtx)
	}()

	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}

// handleWebSocket upgrades incoming client connections to WebSockets.
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] WebSocket upgrade rejected or failed: %v", err)
		return
	}

	client := NewClient(s.hub, conn)
	s.hub.register <- client

	go client.writePump()
	go client.readPump()
}

// handleSnapshot returns the current market price snapshot.
func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	snapshot := s.hub.GetSnapshot()
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"count":     len(snapshot),
		"snapshot":  snapshot,
		"timestamp": time.Now().UnixMilli(),
	})
}

// handleHealth returns server health and connection metrics.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "healthy",
		"active_clients": s.hub.ClientCount(),
		"uptime_seconds": time.Since(s.startTime).Seconds(),
		"timestamp":      time.Now().UnixMilli(),
	})
}
