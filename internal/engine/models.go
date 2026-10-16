package engine

import "time"

// RawUpstreamTick matches the exact JSON schema streamed by the upstream feed.
// Note: low_24h, high_24h, and change_24h are received but MUST BE IGNORED per assignment instructions.
type RawUpstreamTick struct {
	Symbol    string  `json:"symbol"`
	Buy       float64 `json:"buy"`
	Sell      float64 `json:"sell"`
	Low24h    float64 `json:"low_24h"`    // IGNORED: We compute our own rolling 24h low
	High24h   float64 `json:"high_24h"`   // IGNORED: We compute our own rolling 24h high
	Change24h float64 `json:"change_24h"` // IGNORED: We compute our own rolling 24h % change
	Timestamp int64   `json:"timestamp"`  // Unix timestamp (milliseconds or seconds)
}

// CleanTick represents a validated, sanitized market price tick ready for engine processing.
type CleanTick struct {
	Symbol    string    `json:"symbol"`
	Buy       float64   `json:"buy"`
	Sell      float64   `json:"sell"`
	MidPrice  float64   `json:"mid_price"` // (Buy + Sell) / 2
	Timestamp time.Time `json:"timestamp"`
}

// SanitizerMetrics tracks data hygiene and fault-tolerance statistics.
type SanitizerMetrics struct {
	TotalReceived   int64 `json:"total_received"`
	ValidProcessed  int64 `json:"valid_processed"`
	DroppedMalformed int64 `json:"dropped_malformed"`
	DroppedDuplicate int64 `json:"dropped_duplicate"`
	DroppedOutOfOrder int64 `json:"dropped_out_of_order"`
	DroppedSpike    int64 `json:"dropped_spike"`
}
