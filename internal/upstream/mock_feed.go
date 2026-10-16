package upstream

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"
)

// MockFeed simulates an active upstream WebSocket stream when offline or testing.
// It continuously produces realistic market ticks for all 61 symbols.
type MockFeed struct {
	symbols    []string
	basePrices map[string]float64
}

// NewMockFeed initializes a mock generator with realistic initial market prices.
func NewMockFeed(symbols []string) *MockFeed {
	if len(symbols) == 0 {
		symbols = AllRequiredSymbols
	}

	basePrices := make(map[string]float64)
	for _, s := range symbols {
		switch {
		case s == "BTCUSD":
			basePrices[s] = 65000.0
		case s == "ETHUSD":
			basePrices[s] = 3500.0
		case s == "SOLUSD":
			basePrices[s] = 150.0
		case s == "XAUUSD": // Gold
			basePrices[s] = 2350.0
		case s == "EURUSD":
			basePrices[s] = 1.0850
		case s == "GBPUSD":
			basePrices[s] = 1.2950
		case s == "USDJPY":
			basePrices[s] = 155.20
		default:
			basePrices[s] = 10.0 + rand.Float64()*50.0
		}
	}

	return &MockFeed{
		symbols:    symbols,
		basePrices: basePrices,
	}
}

// Run streams simulated JSON ticks onto the provided channel at the given frequency.
func (m *MockFeed) Run(ctx context.Context, out chan<- []byte, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Pick a random symbol
			sym := m.symbols[r.Intn(len(m.symbols))]
			base := m.basePrices[sym]

			// Drift price slightly (+- 0.1%)
			drift := (r.Float64() - 0.495) * 0.002 * base
			current := base + drift
			m.basePrices[sym] = current

			spread := current * 0.0002 // 2 bps spread
			buy := current - (spread / 2)
			sell := current + (spread / 2)

			// Construct raw payload exactly as upstream specifies
			raw := map[string]interface{}{
				"symbol":     sym,
				"buy":        buy,
				"sell":       sell,
				"low_24h":    current * 0.95, // Upstream dummy value (MUST BE IGNORED)
				"high_24h":   current * 1.05, // Upstream dummy value (MUST BE IGNORED)
				"change_24h": 1.25,           // Upstream dummy value (MUST BE IGNORED)
				"timestamp":  time.Now().UnixMilli(),
			}

			data, err := json.Marshal(raw)
			if err == nil {
				select {
				case out <- data:
				case <-ctx.Done():
					return
				default:
					// Drop if channel buffer is full to avoid blocking mock
				}
			}
		}
	}
}
