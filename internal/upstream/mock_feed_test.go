package upstream

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestMockFeed_InitialPrices(t *testing.T) {
	mock := NewMockFeed(AllRequiredSymbols)

	if len(mock.basePrices) != len(AllRequiredSymbols) {
		t.Fatalf("expected %d symbol prices, got %d", len(AllRequiredSymbols), len(mock.basePrices))
	}

	for _, sym := range AllRequiredSymbols {
		price, ok := mock.basePrices[sym]
		if !ok || price <= 0 {
			t.Errorf("symbol %s has invalid initial price: %f", sym, price)
		}
	}

	// Spot-check specific assets for realistic pricing
	if mock.basePrices["BTCUSD"] < 50000.0 || mock.basePrices["BTCUSD"] > 100000.0 {
		t.Errorf("BTCUSD price not in realistic range: %f", mock.basePrices["BTCUSD"])
	}
	if mock.basePrices["EURUSD"] < 0.90 || mock.basePrices["EURUSD"] > 1.30 {
		t.Errorf("EURUSD price not in realistic range: %f", mock.basePrices["EURUSD"])
	}
	if mock.basePrices["XAUUSD"] < 1500.0 || mock.basePrices["XAUUSD"] > 3500.0 {
		t.Errorf("XAUUSD gold price not in realistic range: %f", mock.basePrices["XAUUSD"])
	}
}

func TestMockFeed_StreamTicks(t *testing.T) {
	mock := NewMockFeedWithSeed([]string{"BTCUSD", "ETHUSD"}, 42)
	out := make(chan []byte, 10)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	go mock.Run(ctx, out, 10*time.Millisecond)

	received := 0
	for {
		select {
		case data, ok := <-out:
			if !ok {
				return
			}
			var tick map[string]interface{}
			if err := json.Unmarshal(data, &tick); err != nil {
				t.Fatalf("malformed tick JSON: %v", err)
			}
			buy := tick["buy"].(float64)
			sell := tick["sell"].(float64)
			if buy <= 0 || sell <= 0 {
				t.Errorf("prices must be positive, got buy=%.2f, sell=%.2f", buy, sell)
			}
			if buy > sell {
				t.Errorf("buy %.4f must not exceed sell %.4f", buy, sell)
			}
			received++
			if received >= 3 {
				return
			}
		case <-ctx.Done():
			if received == 0 {
				t.Fatal("no ticks produced within timeout")
			}
			return
		}
	}
}
