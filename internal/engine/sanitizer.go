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
	ErrUnknownSymbol   = errors.New("unknown or unsupported market symbol")
	ErrInvalidPrice    = errors.New("buy or sell price must be positive")
	ErrNonFinitePrice  = errors.New("price must be a finite number")
	ErrInvertedSpread  = errors.New("buy price cannot exceed sell price")
	ErrDuplicateTick   = errors.New("duplicate tick timestamp")
	ErrOutOfOrderTick  = errors.New("out-of-order tick received")
	ErrFutureTimestamp = errors.New("tick timestamp is too far in future")
	ErrStaleTick       = errors.New("tick timestamp is older than 24 hours")
	ErrPriceSpike      = errors.New("abnormal price spike detected")
)

// Sanitizer validates and filters raw inbound ticks to guarantee data integrity.
type Sanitizer struct {
	mu             sync.Mutex
	lastTimestamps map[string]int64   // symbol -> last seen Unix millisecond
	lastMidPrices  map[string]float64 // symbol -> last valid mid price
	maxSpikeRatio  float64            // Maximum allowable sudden change (e.g. 0.50 = 50% change)
	allowedSymbols map[string]bool    // Whitelist of valid symbols (empty means accept any non-empty)
	metrics        SanitizerMetrics
}

// NewSanitizer constructs a Sanitizer with the given spike threshold.
// maxSpikeRatio = 0.50 means a 50% jump in a single tick will be flagged as an anomaly.
func NewSanitizer(maxSpikeRatio float64) *Sanitizer {
	if maxSpikeRatio <= 0 {
		maxSpikeRatio = 0.50
	}
	return &Sanitizer{
		lastTimestamps: make(map[string]int64),
		lastMidPrices:  make(map[string]float64),
		maxSpikeRatio:  maxSpikeRatio,
		allowedSymbols: make(map[string]bool),
	}
}

// SetAllowedSymbols restricts valid ticks to the specified symbol whitelist.
func (s *Sanitizer) SetAllowedSymbols(symbols []string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.allowedSymbols = make(map[string]bool, len(symbols))
	for _, sym := range symbols {
		s.allowedSymbols[strings.ToUpper(strings.TrimSpace(sym))] = true
	}
}

// SanitizeRaw parses raw JSON bytes, validates fields, and applies deduplication and spike filtering.
func (s *Sanitizer) SanitizeRaw(data []byte) (*CleanTick, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.metrics.TotalReceived++

	var raw RawUpstreamTick
	if err := json.Unmarshal(data, &raw); err != nil {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: %v", ErrMalformedJSON, err)
	}

	raw.Symbol = strings.ToUpper(strings.TrimSpace(raw.Symbol))
	if raw.Symbol == "" {
		s.metrics.DroppedMalformed++
		return nil, ErrEmptySymbol
	}

	// Validate against known symbol whitelist if configured
	if len(s.allowedSymbols) > 0 && !s.allowedSymbols[raw.Symbol] {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: %s", ErrUnknownSymbol, raw.Symbol)
	}

	// Check for non-finite values (NaN / Inf)
	if math.IsNaN(raw.Buy) || math.IsInf(raw.Buy, 0) || math.IsNaN(raw.Sell) || math.IsInf(raw.Sell, 0) {
		s.metrics.DroppedMalformed++
		return nil, ErrNonFinitePrice
	}

	// Check for positive price values
	if raw.Buy <= 0 || raw.Sell <= 0 {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: buy=%.4f, sell=%.4f", ErrInvalidPrice, raw.Buy, raw.Sell)
	}

	// Spread validation: Buy (Bid) must not exceed Sell (Ask)
	if raw.Buy > raw.Sell {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: buy=%.4f > sell=%.4f", ErrInvertedSpread, raw.Buy, raw.Sell)
	}

	// Normalize unix epoch seconds to milliseconds when necessary
	tsMillis := raw.Timestamp
	if tsMillis < 10000000000 {
		tsMillis *= 1000
	}

	nowMs := time.Now().UnixMilli()

	// Reject future timestamps (>5 minutes in the future)
	if tsMillis > nowMs+300000 {
		s.metrics.DroppedMalformed++
		return nil, fmt.Errorf("%w: ts=%d, now=%d", ErrFutureTimestamp, tsMillis, nowMs)
	}

	// Reject stale ticks older than 24 hours
	if tsMillis < nowMs-86400000 {
		s.metrics.DroppedOutOfOrder++
		return nil, fmt.Errorf("%w: ts=%d, now=%d", ErrStaleTick, tsMillis, nowMs)
	}

	// Reject duplicates and out-of-order deliveries
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

	midPrice := (raw.Buy + raw.Sell) / 2.0

	// Reject abnormal price spikes from corrupted quotes
	if lastPrice, exists := s.lastMidPrices[raw.Symbol]; exists && lastPrice > 0 {
		ratio := math.Abs(midPrice-lastPrice) / lastPrice
		if ratio > s.maxSpikeRatio {
			s.metrics.DroppedSpike++
			return nil, fmt.Errorf("%w for %s: jumped from %.4f to %.4f (diff: %.1f%%)", ErrPriceSpike, raw.Symbol, lastPrice, midPrice, ratio*100)
		}
	}

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
