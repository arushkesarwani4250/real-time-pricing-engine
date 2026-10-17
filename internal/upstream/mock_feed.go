package upstream

import (
	"context"
	"encoding/json"
	"math/rand"
	"time"
)

// defaultSymbolPrices contains realistic baseline market prices for all 61 instruments.
var defaultSymbolPrices = map[string]float64{
	// FX Majors & Direct Pairs
	"EURUSD": 1.0850, "GBPUSD": 1.2950, "AUDUSD": 0.6550, "NZDUSD": 0.5950,
	"USDCAD": 1.3850, "USDCHF": 0.8650, "USDJPY": 152.40,
	// European Crosses
	"EURGBP": 0.8350, "EURCHF": 0.9400, "EURJPY": 165.30, "EURAUD": 1.6550,
	"EURCAD": 1.5020, "EURNZD": 1.8220, "EURTRY": 37.20,
	// Sterling Crosses
	"GBPJPY": 197.40, "GBPCHF": 1.1200, "GBPAUD": 1.9750, "GBPCAD": 1.7920,
	"GBPNZD": 2.1750, "GBPTRY": 44.50,
	// Aussie & Kiwi Crosses
	"AUDJPY": 99.80, "AUDCAD": 0.9070, "AUDCHF": 0.5660, "AUDNZD": 1.1010,
	"AUDSGD": 0.8680, "NZDJPY": 90.70, "NZDCAD": 0.8240, "NZDCHF": 0.5140,
	// Canadian & Swiss Crosses
	"CADJPY": 110.05, "CADCHF": 0.6240, "CHFJPY": 176.20, "CHFSGD": 1.5320,
	// Emerging & Exotic Pairs
	"USDCNH": 7.2350, "USDMXN": 20.050, "USDNOK": 11.020, "USDPLN": 4.0450,
	"USDSEK": 10.920, "USDSGD": 1.3250, "USDTRY": 34.350, "USDZAR": 17.850,
	"NOKJPY": 13.82, "SEKJPY": 13.95, "SGDJPY": 115.02, "ZARJPY": 8.53,
	// Precious Metals
	"XAUUSD": 2650.00, "XAGUSD": 31.50,
	// Cryptocurrencies
	"BTCUSD": 68500.00, "ETHUSD": 2620.00, "SOLUSD": 165.00, "BNBUSD": 585.00,
	"XRPUSD": 0.5400, "ADAUSD": 0.3500, "DOGUSD": 0.1250, "DOTUSD": 4.2500,
	"LTCUSD": 71.50, "BCHUSD": 345.00, "XLMUSD": 0.0950, "TRXUSD": 0.1600,
	"UNIUSD": 7.40, "FILUSD": 3.65, "AVXUSD": 27.80,
}

// MockFeed generates realistic simulated market ticks for offline testing and benchmarking.
type MockFeed struct {
	symbols    []string
	basePrices map[string]float64
	seed       int64
}

// NewMockFeed initializes a mock generator with realistic asset prices for all symbols.
func NewMockFeed(symbols []string) *MockFeed {
	return NewMockFeedWithSeed(symbols, time.Now().UnixNano())
}

// NewMockFeedWithSeed initializes a mock generator with a fixed seed for deterministic testing.
func NewMockFeedWithSeed(symbols []string, seed int64) *MockFeed {
	if len(symbols) == 0 {
		symbols = AllRequiredSymbols
	}

	basePrices := make(map[string]float64, len(symbols))
	for _, s := range symbols {
		if p, ok := defaultSymbolPrices[s]; ok {
			basePrices[s] = p
		} else {
			basePrices[s] = 100.0
		}
	}

	return &MockFeed{
		symbols:    symbols,
		basePrices: basePrices,
		seed:       seed,
	}
}

// Run streams simulated JSON ticks onto the provided channel at the given interval.
func (m *MockFeed) Run(ctx context.Context, out chan<- []byte, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	r := rand.New(rand.NewSource(m.seed))

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sym := m.symbols[r.Intn(len(m.symbols))]
			base := m.basePrices[sym]

			// Controlled small drift (+- 0.02%)
			drift := (r.Float64() - 0.5) * 0.0004 * base
			current := base + drift
			m.basePrices[sym] = current

			// Tight realistic spread (1-2 basis points)
			spread := current * 0.00015
			buy := current - (spread / 2)
			sell := current + (spread / 2)

			raw := map[string]interface{}{
				"symbol":     sym,
				"buy":        buy,
				"sell":       sell,
				"low_24h":    current * 0.95,
				"high_24h":   current * 1.05,
				"change_24h": 1.25,
				"timestamp":  time.Now().UnixMilli(),
			}

			data, err := json.Marshal(raw)
			if err == nil {
				select {
				case out <- data:
				case <-ctx.Done():
					return
				default:
				}
			}
		}
	}
}
