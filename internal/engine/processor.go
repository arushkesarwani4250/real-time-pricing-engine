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

	rw.AddTick(tick.MidPrice, tick.Timestamp)
	high24h, low24h, change24h := rw.Get24hStats(tick.MidPrice, tick.Timestamp)

	latency := float64(time.Since(tick.Timestamp).Microseconds()) / 1000.0
	if latency < 0 {
		latency = 0.001
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

// ExportState extracts the in-memory rolling window state for persistence.
func (pe *PricingEngine) ExportState() *EngineState {
	pe.mu.RLock()
	defer pe.mu.RUnlock()

	state := &EngineState{
		SavedAt: time.Now(),
		Symbols: make(map[string]*PersistedSymbolState),
	}

	for sym, rw := range pe.windows {
		state.Symbols[sym] = &PersistedSymbolState{
			Symbol:    sym,
			LastPrice: pe.latestPrices[sym],
			Buckets:   rw.ExportBuckets(),
		}
	}

	return state
}

// RestoreState repopulates the engine with persisted historical state after a restart.
func (pe *PricingEngine) RestoreState(state *EngineState, now time.Time) int {
	if state == nil || len(state.Symbols) == 0 {
		return 0
	}

	pe.mu.Lock()
	defer pe.mu.Unlock()

	restoredCount := 0
	for sym, pState := range state.Symbols {
		rw := NewRollingWindow(sym)
		rw.ImportBuckets(pState.Buckets, now)
		pe.windows[sym] = rw

		if pState.LastPrice != nil {
			// Recalculate 24h stats based on restored buckets and current time
			h, l, c := rw.Get24hStats(pState.LastPrice.MidPrice, now)
			pState.LastPrice.High24h = h
			pState.LastPrice.Low24h = l
			pState.LastPrice.Change24h = c
			pe.latestPrices[sym] = pState.LastPrice
		}
		restoredCount++
	}

	return restoredCount
}
