package engine

import (
	"math"
	"testing"
	"time"
)

func TestRollingWindow_SingleMinuteHighLow(t *testing.T) {
	rw := NewRollingWindow("BTCUSD")
	now := time.Now()

	rw.AddTick(50000.0, now)
	rw.AddTick(52000.0, now.Add(5*time.Second))
	rw.AddTick(48000.0, now.Add(10*time.Second))
	rw.AddTick(51000.0, now.Add(15*time.Second))

	high, low, change := rw.Get24hStats(51000.0, now.Add(15*time.Second))

	if high != 52000.0 {
		t.Errorf("expected high 52000.0, got %.2f", high)
	}
	if low != 48000.0 {
		t.Errorf("expected low 48000.0, got %.2f", low)
	}
	// Initial open was 50000, current is 51000 -> +2% change
	expectedChange := ((51000.0 - 50000.0) / 50000.0) * 100.0
	if math.Abs(change-expectedChange) > 0.001 {
		t.Errorf("expected change %.2f%%, got %.2f%%", expectedChange, change)
	}
}

func TestRollingWindow_MultiMinuteTracking(t *testing.T) {
	rw := NewRollingWindow("ETHUSD")
	baseTime := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

	// Minute 0: Open at 3000
	rw.AddTick(3000.0, baseTime)

	// Minute 10: Spike high to 3500
	rw.AddTick(3500.0, baseTime.Add(10*time.Minute))

	// Minute 60: Dip low to 2800
	rw.AddTick(2800.0, baseTime.Add(60*time.Minute))

	// Minute 120: Current price 3300
	currentTime := baseTime.Add(120 * time.Minute)
	high, low, change := rw.Get24hStats(3300.0, currentTime)

	if high != 3500.0 {
		t.Errorf("expected high 3500.0, got %.2f", high)
	}
	if low != 2800.0 {
		t.Errorf("expected low 2800.0, got %.2f", low)
	}
	// Open was 3000, current is 3300 -> +10%
	if math.Abs(change-10.0) > 0.001 {
		t.Errorf("expected change 10.0%%, got %.2f%%", change)
	}
}

func TestRollingWindow_24HourExpiryRollOff(t *testing.T) {
	rw := NewRollingWindow("SOLUSD")
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Day 1, 00:00: Giant high of 200.0
	rw.AddTick(200.0, t0)

	// Day 1, 12:00: Price at 100.0
	rw.AddTick(100.0, t0.Add(12*time.Hour))

	// At 23 hours later: The 200.0 price should STILL be in the 24h window
	tAt23h := t0.Add(23 * time.Hour)
	high, _, _ := rw.Get24hStats(105.0, tAt23h)
	if high != 200.0 {
		t.Errorf("at 23h, expected high 200.0 still active, got %.2f", high)
	}

	// At 25 hours later: The 200.0 price occurred > 24 hours ago, so it MUST roll off!
	tAt25h := t0.Add(25 * time.Hour)
	rw.AddTick(110.0, tAt25h) // Adds tick and triggers pruning
	highAfterRollOff, lowAfterRollOff, _ := rw.Get24hStats(110.0, tAt25h)

	if highAfterRollOff >= 200.0 {
		t.Errorf("after 25h, price 200.0 should have expired! Got high %.2f", highAfterRollOff)
	}
	if highAfterRollOff != 110.0 {
		t.Errorf("expected new high to be 110.0, got %.2f", highAfterRollOff)
	}
	if lowAfterRollOff != 100.0 {
		t.Errorf("expected low to be 100.0, got %.2f", lowAfterRollOff)
	}
}

func BenchmarkRollingWindow_ProcessTick(b *testing.B) {
	rw := NewRollingWindow("BTCUSD")
	now := time.Now()

	// Pre-fill with 1,440 buckets (full 24h capacity)
	for i := 0; i < 1440; i++ {
		rw.AddTick(60000.0+float64(i), now.Add(time.Duration(i)*time.Minute))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rw.AddTick(65000.0, now.Add(time.Duration(1441)*time.Minute))
		_, _, _ = rw.Get24hStats(65000.0, now.Add(time.Duration(1441)*time.Minute))
	}
}
