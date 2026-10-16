package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

var (
	ErrMalformedJSON   = errors.New("malformed JSON payload")
	ErrEmptySymbol     = errors.New("empty symbol name")
	ErrInvalidPrice    = errors.New("buy or sell price must be positive")
	ErrDuplicateTick   = errors.New("duplicate tick timestamp")
	ErrOutOfOrderTick  = errors.New("out-of-order tick received")
	ErrPriceSpike      = errors.New("abnormal price spike detected")
)

// Sanitizer validates and filters raw inbound ticks to guarantee data integrity.
type Sanitizer struct {
	mu                  sync.Mutex
	lastTimestamps      map[string]int64   // symbol -> last seen Unix millisecond
	lastMidPrices       map[string]float64 // symbol -> last valid mid price
	maxSpikeRatio       float64            // Maximum allowable sudden change (e.g. 0.50 = 50% change)
	metrics             SanitizerMetrics
}

// NewSanitizer constructs a Sanitizer with the given spike threshold.
// maxSpikeRatio = 0.50 means a 50% jump in a single tick will be flagged as an anomaly.
func NewSanitizer(maxSpikeRatio float64) *Sanitizer {
	if maxSpikeRatio <= 0 {
		maxSpikeRatio = 0.50 // default: 50% spike protection band
	}
	return &Sanitizer{
		lastTimestamps: make(map[string]int64),
		lastMidPrices:  make(map[string]float64),
		maxSpikeRatio:  maxSpikeRatio,
	}
}

// SanitizeRaw parses raw JSON bytes, validates fields, and applies deduplication and spike filtering.
func (s *Sanitizer) SanitizeRaw(data []byte) (*CleanTick, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metrics.TotalReceived++

	// 1. JSON Parsing
	var raw RawUpstreamTick
	if err := json.Unmarshal(data, &raw); err != nil {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: %v", ErrMalformedJSON, err)
	}

	// 2. Structural & Field Validation
	raw.Symbol = strings.ToUpper(strings.TrimSpace(raw.Symbol))
	if raw.Symbol == "" {
		s.metrics.DroppedMalformed++
		return nil, ErrEmptySymbol
	}

	if raw.Buy <= 0 || raw.Sell <= 0 {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: buy=%.4f, sell=%.4f", ErrInvalidPrice, raw.Buy, raw.Sell)
	}

	// Normalize timestamp: Convert seconds (10 digits) to milliseconds if needed
	tsMillis := raw.Timestamp
	if tsMillis < 10000000000 { // Less than 10 digits -> seconds
		tsMillis *= 1000
	}

	// 3. Duplicate & Out-of-Order Checks
	if lastTS, exists := s.lastTimestamps[raw.Symbol]; exists {
		if tsMillis == lastTS {
			s.metrics.DroppedDuplicate++
			return nil, fmt.Errorf("%w for symbol %s (ts=%d)", ErrDuplicateTick, raw.Symbol, tsMillis)
		}
		if tsMillis < lastTS {
			s.metrics.DroppedOutOfOrder++
			return nil, fmt.Errorf("%w for symbol %s (received=%d < last=%d)", ErrOutOfOrderTick, raw.Symbol, tsMillis, lastTS)
		}
	}

	// Calculate Mid Price: (Buy + Sell) / 2
	midPrice := (raw.Buy + raw.Sell) / 2.0

	// 4. Spike Filter: Protect against corrupted feed anomalies
	if lastPrice, exists := s.lastMidPrices[raw.Symbol]; exists && lastPrice > 0 {
		ratio := math.Abs(midPrice-lastPrice) / lastPrice
		if ratio > s.maxSpikeRatio {
			s.metrics.DroppedSpike++
			return nil, fmt.Errorf("%w for %s: jumped from %.4f to %.4f (diff: %.1f%%)", ErrPriceSpike, raw.Symbol, lastPrice, midPrice, ratio*100)
		}
	}

	// Update state for next tick
	s.lastTimestamps[raw.Symbol] = tsMillis
	s.lastMidPrices[raw.Symbol] = midPrice
	s.metrics.ValidProcessed++

	return &CleanTick{
		Symbol:    raw.Symbol,
		Buy:       raw.Buy,
		Sell:      raw.Sell,
		MidPrice:  midPrice,
		Timestamp: time.UnixMilli(tsMillis),
	}, nil
}

// GetMetrics returns a snapshot of data processing and hygiene counters.
func (s *Sanitizer) GetMetrics() SanitizerMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.metrics
}
