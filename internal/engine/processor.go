package engine

import (
	"sync"
	"time"
)

// PricingEngine orchestrates rolling window statistics and state management for all market symbols.
type PricingEngine struct {
	mu           sync.RWMutex
	windows      map[string]*RollingWindow
	latestPrices map[string]*MarketPrice
}

// NewPricingEngine initializes a new PricingEngine.
func NewPricingEngine() *PricingEngine {
	return &PricingEngine{
		windows:      make(map[string]*RollingWindow),
		latestPrices: make(map[string]*MarketPrice),
	}
}

// ProcessTick processes a clean tick, computes rolling 24h stats, and records latency.
func (pe *PricingEngine) ProcessTick(tick *CleanTick) *MarketPrice {
	pe.mu.Lock()
	rw, exists := pe.windows[tick.Symbol]
	if !exists {
		rw = NewRollingWindow(tick.Symbol)
		pe.windows[tick.Symbol] = rw
	}
	pe.mu.Unlock()

	// 1. Add tick to the rolling window
	rw.AddTick(tick.MidPrice, tick.Timestamp)

	// 2. Compute our own 24h High, 24h Low, and 24h % Change
	high24h, low24h, change24h := rw.Get24hStats(tick.MidPrice, tick.Timestamp)

	// 3. Measure end-to-end processing latency from tick timestamp to now
	latency := float64(time.Since(tick.Timestamp).Microseconds()) / 1000.0
	if latency < 0 {
		latency = 0.001 // sub-millisecond
	}

	enriched := &MarketPrice{
		Symbol:    tick.Symbol,
		Buy:       tick.Buy,
		Sell:      tick.Sell,
		MidPrice:  tick.MidPrice,
		High24h:   high24h,
		Low24h:    low24h,
		Change24h: change24h,
		Timestamp: tick.Timestamp,
		LatencyMs: latency,
	}

	pe.mu.Lock()
	pe.latestPrices[tick.Symbol] = enriched
	pe.mu.Unlock()

	return enriched
}

// GetPrice returns the latest price record for a given symbol.
func (pe *PricingEngine) GetPrice(symbol string) (*MarketPrice, bool) {
	pe.mu.RLock()
	defer pe.mu.RUnlock()
	p, ok := pe.latestPrices[symbol]
	return p, ok
}

// GetAllPrices returns a slice of all current market prices.
func (pe *PricingEngine) GetAllPrices() []*MarketPrice {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	res := make([]*MarketPrice, 0, len(pe.latestPrices))
	for _, p := range pe.latestPrices {
		res = append(res, p)
	}
	return res
}
