package engine

import "time"

// RawUpstreamTick matches the wire JSON schema streamed by the upstream feed.
// Upstream statistics (Low24h, High24h, Change24h) are ignored in favor of
// internally computed rolling-window metrics.
type RawUpstreamTick struct {
	Symbol    string  `json:"symbol"`
	Buy       float64 `json:"buy"`
	Sell      float64 `json:"sell"`
	Low24h    float64 `json:"low_24h"`
	High24h   float64 `json:"high_24h"`
	Change24h float64 `json:"change_24h"`
	Timestamp int64   `json:"timestamp"` // Unix timestamp in seconds or milliseconds
}

// CleanTick represents a validated, sanitized market price tick ready for engine processing.
type CleanTick struct {
	Symbol    string    `json:"symbol"`
	Buy       float64   `json:"buy"`
	Sell      float64   `json:"sell"`
	MidPrice  float64   `json:"mid_price"` // (Buy + Sell) / 2
	Timestamp time.Time `json:"timestamp"`
}

// MarketPrice is the fully enriched price record computed by our Pricing Engine.
type MarketPrice struct {
	Symbol    string    `json:"symbol"`
	Buy       float64   `json:"buy"`
	Sell      float64   `json:"sell"`
	MidPrice  float64   `json:"mid_price"`
	High24h   float64   `json:"high_24h"`   // Computed by our engine
	Low24h    float64   `json:"low_24h"`    // Computed by our engine
	Change24h float64   `json:"change_24h"` // Computed % change over 24h
	Timestamp time.Time `json:"timestamp"`
	LatencyMs float64   `json:"latency_ms"` // End-to-end processing latency
}

// PersistedSymbolState captures all active minute buckets and the last known price for a symbol.
type PersistedSymbolState struct {
	Symbol    string                  `json:"symbol"`
	LastPrice *MarketPrice            `json:"last_price"`
	Buckets   map[int64]*MinuteBucket `json:"buckets"`
}

// EngineState captures the full snapshot of all market instruments for crash recovery.
type EngineState struct {
	SavedAt time.Time                        `json:"saved_at"`
	Symbols map[string]*PersistedSymbolState `json:"symbols"`
}

// SanitizerMetrics tracks data hygiene and fault-tolerance statistics.
type SanitizerMetrics struct {
	TotalReceived     int64 `json:"total_received"`
	ValidProcessed    int64 `json:"valid_processed"`
	DroppedMalformed  int64 `json:"dropped_malformed"`
	DroppedDuplicate  int64 `json:"dropped_duplicate"`
	DroppedOutOfOrder int64 `json:"dropped_out_of_order"`
	DroppedSpike      int64 `json:"dropped_spike"`
}
