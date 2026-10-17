package engine

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// TestLatencyPercentiles measures tick processing latency over 10,000 operations
// and computes empirical p50, p90, p95, and p99 statistics.
func TestLatencyPercentiles(t *testing.T) {
	pe := NewPricingEngine()
	now := time.Now()

	// Pre-fill full 24-hour capacity (1,440 one-minute buckets)
	for m := 0; m < 1440; m++ {
		tBucket := now.Add(time.Duration(m-1440) * time.Minute)
		pe.ProcessTick(&CleanTick{
			Symbol:    "BTCUSD",
			Buy:       65000.0 + float64(m%50),
			Sell:      65010.0 + float64(m%50),
			MidPrice:  65005.0 + float64(m%50),
			Timestamp: tBucket,
		})
	}

	const samples = 100
	const batchSize = 50
	latencies := make([]float64, samples)

	for s := 0; s < samples; s++ {
		start := time.Now()
		for b := 0; b < batchSize; b++ {
			idx := s*batchSize + b
			tick := &CleanTick{
				Symbol:    "BTCUSD",
				Buy:       65000.0 + float64(idx%100),
				Sell:      65010.0 + float64(idx%100),
				MidPrice:  65005.0 + float64(idx%100),
				Timestamp: now.Add(time.Duration(idx) * time.Second),
			}
			_ = pe.ProcessTick(tick)
		}
		// Per-tick latency in microseconds
		perTickUs := float64(time.Since(start).Nanoseconds()) / (float64(batchSize) * 1000.0)
		latencies[s] = perTickUs
	}

	sort.Float64s(latencies)

	min := latencies[0]
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p90 := latencies[int(float64(len(latencies))*0.90)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]
	max := latencies[len(latencies)-1]

	t.Logf("=== Empirical Per-Tick Latency Distribution (%d samples of %d ticks) ===", samples, batchSize)
	t.Logf("Min: %.2f µs (%.4f ms)", min, min/1000.0)
	t.Logf("p50: %.2f µs (%.4f ms)", p50, p50/1000.0)
	t.Logf("p90: %.2f µs (%.4f ms)", p90, p90/1000.0)
	t.Logf("p95: %.2f µs (%.4f ms)", p95, p95/1000.0)
	t.Logf("p99: %.2f µs (%.4f ms)", p99, p99/1000.0)
	t.Logf("Max: %.2f µs (%.4f ms)", max, max/1000.0)

	// Verify SLA: p99 must be well below 50ms budget (50,000 µs)
	if p99 > 50000.0 {
		t.Fatalf("p99 latency %.2f µs exceeded 50ms budget", p99)
	}

	// Verify sub-millisecond per-tick calculation
	if p99 > 1000.0 {
		t.Errorf("expected sub-millisecond p99 latency (< 1000 µs), got %.2f µs", p99)
	}
}

// TestRollingWindow_ExactBoundaryExpiration validates tick retention at minute boundaries.
func TestRollingWindow_ExactBoundaryExpiration(t *testing.T) {
	rw := NewRollingWindow("ETHUSD")
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// T=0: Extreme peak at 4000.0
	rw.AddTick(4000.0, baseTime)

	// T=1439 minutes (23h 59m): Price at 3000.0
	t1439 := baseTime.Add(1439 * time.Minute)
	rw.AddTick(3000.0, t1439)

	// At T=1439m, the peak 4000.0 is still within the 1440-minute window
	h1, _, _ := rw.Get24hStats(3000.0, t1439)
	if h1 != 4000.0 {
		t.Errorf("at minute 1439, expected high 4000.0 still active, got %.2f", h1)
	}

	// At T=1441 minutes (24h 01m): Price at 3100.0
	// T=0 bucket has now aged out (>1440 minutes ago)
	t1441 := baseTime.Add(1441 * time.Minute)
	rw.AddTick(3100.0, t1441)

	h2, l2, _ := rw.Get24hStats(3100.0, t1441)
	if h2 >= 4000.0 {
		t.Errorf("at minute 1441, the 4000.0 peak must expire! Got %.2f", h2)
	}
	if h2 != 3100.0 {
		t.Errorf("expected new high to be 3100.0, got %.2f", h2)
	}
	if l2 != 3000.0 {
		t.Errorf("expected low to be 3000.0, got %.2f", l2)
	}
}

// TestUpstreamFieldsIgnored verifies that upstream 24h fields are ignored in favor of local computation.
func TestUpstreamFieldsIgnored(t *testing.T) {
	sanitizer := NewSanitizer(0.50)
	pe := NewPricingEngine()

	now := time.Now().UnixMilli()
	payload := fmt.Sprintf(`{"symbol":"BTCUSD","buy":49999.0,"sell":50001.0,"low_24h":1.0,"high_24h":999999.0,"change_24h":999.0,"timestamp":%d}`, now)
	cleanTick, err := sanitizer.SanitizeRaw([]byte(payload))
	if err != nil {
		t.Fatalf("sanitizer failed: %v", err)
	}

	enriched := pe.ProcessTick(cleanTick)

	if enriched.High24h == 999999.0 {
		t.Fatal("expected engine to compute high_24h, got upstream payload value")
	}
	if enriched.Low24h == 1.0 {
		t.Fatal("expected engine to compute low_24h, got upstream payload value")
	}
	if enriched.Change24h == 999.0 {
		t.Fatal("expected engine to compute change_24h, got upstream payload value")
	}

	if enriched.High24h != 50000.0 || enriched.Low24h != 50000.0 {
		t.Errorf("expected high and low 50000.0, got high=%.2f, low=%.2f", enriched.High24h, enriched.Low24h)
	}
}

// TestPricingEngine_ExportAndRestoreState verifies that state can be serialized,
// restored into a new engine instance, and correctly reflects surviving buckets and expired buckets.
func TestPricingEngine_ExportAndRestoreState(t *testing.T) {
	pe1 := NewPricingEngine()
	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Ingest ticks for BTCUSD across two hours
	pe1.ProcessTick(&CleanTick{
		Symbol:    "BTCUSD",
		Buy:       60000.0,
		Sell:      60010.0,
		MidPrice:  60005.0,
		Timestamp: baseTime,
	})
	pe1.ProcessTick(&CleanTick{
		Symbol:    "BTCUSD",
		Buy:       62000.0,
		Sell:      62010.0,
		MidPrice:  62005.0,
		Timestamp: baseTime.Add(30 * time.Minute),
	})
	pe1.ProcessTick(&CleanTick{
		Symbol:    "ETHUSD",
		Buy:       3000.0,
		Sell:      3002.0,
		MidPrice:  3001.0,
		Timestamp: baseTime.Add(45 * time.Minute),
	})

	// Export state
	exported := pe1.ExportState()
	if len(exported.Symbols) != 2 {
		t.Fatalf("expected 2 symbols in exported state, got %d", len(exported.Symbols))
	}

	// 1. Restore into fresh engine 1 hour later (all ticks should be retained)
	pe2 := NewPricingEngine()
	restoredCount := pe2.RestoreState(exported, baseTime.Add(1*time.Hour))
	if restoredCount != 2 {
		t.Fatalf("expected 2 symbols restored, got %d", restoredCount)
	}

	snapBTC, ok := pe2.GetPrice("BTCUSD")
	if !ok || snapBTC == nil {
		t.Fatal("expected BTCUSD snapshot in restored engine")
	}
	if snapBTC.High24h != 62005.0 {
		t.Errorf("expected restored High24h 62005.0, got %.2f", snapBTC.High24h)
	}
	if snapBTC.Low24h != 60005.0 {
		t.Errorf("expected restored Low24h 60005.0, got %.2f", snapBTC.Low24h)
	}

	// 2. Restore into fresh engine 25 hours later (buckets should age out)
	pe3 := NewPricingEngine()
	pe3.RestoreState(exported, baseTime.Add(25*time.Hour))
	snapBTC25, ok := pe3.GetPrice("BTCUSD")
	if !ok || snapBTC25 == nil {
		t.Fatal("expected BTCUSD snapshot in restored engine")
	}
	// At T+25h, T=0 and T=30m buckets have expired, so high/low fall back to last known mid
	if snapBTC25.High24h != 62005.0 || snapBTC25.Low24h != 62005.0 {
		t.Errorf("expected high and low 62005.0 after expiry, got high=%.2f, low=%.2f", snapBTC25.High24h, snapBTC25.Low24h)
	}
}
