package downstream

import "time"

// ClientRequest is sent by connected WebSocket clients to manage subscriptions.
type ClientRequest struct {
	Action  string   `json:"action"`            // "subscribe", "unsubscribe", "snapshot"
	Symbols []string `json:"symbols,omitempty"` // Symbol list, or empty for all symbols
}

// CompactPriceUpdate is the delta-encoded outbound price update sent over WebSocket.
type CompactPriceUpdate struct {
	Type      string  `json:"type"`       // "price_update"
	Symbol    string  `json:"symbol"`     // Instrument identifier (e.g. "BTCUSD")
	Buy       float64 `json:"buy"`        // Bid price
	Sell      float64 `json:"sell"`       // Ask price
	Mid       float64 `json:"mid"`        // Mid price: (Buy + Sell) / 2
	High24h   float64 `json:"high_24h"`   // Rolling 24-hour high
	Low24h    float64 `json:"low_24h"`    // Rolling 24-hour low
	Change24h float64 `json:"change_24h"` // Rolling 24-hour percentage change
	Timestamp int64   `json:"timestamp"`  // Unix millisecond timestamp
	LatencyMs float64 `json:"latency_ms"` // Internal pipeline processing latency
}

// ServerResponse is sent for acknowledgements or errors.
type ServerResponse struct {
	Status    string      `json:"status"` // "ok", "error", "snapshot"
	Message   string      `json:"message,omitempty"`
	Data      interface{} `json:"data,omitempty"`
	Timestamp int64       `json:"timestamp"`
}

// NewServerResponse creates a standard timestamped response.
func NewServerResponse(status, message string, data interface{}) *ServerResponse {
	return &ServerResponse{
		Status:    status,
		Message:   message,
		Data:      data,
		Timestamp: time.Now().UnixMilli(),
	}
}
