package engine

import (
	"math"
	"sync"
	"time"
)

// MinuteBucket aggregates ticks that occurred within a single 1-minute window.
type MinuteBucket struct {
	MinuteKey int64   `json:"minute_key"` // Unix timestamp in minutes: timestamp / 60
	High      float64 `json:"high"`
	Low       float64 `json:"low"`
	Open      float64 `json:"open"`
	Close     float64 `json:"close"`
	TickCount int64   `json:"tick_count"`
}

// RollingWindow maintains a sliding 24-hour (1,440 minutes) window of price buckets.
type RollingWindow struct {
	mu      sync.RWMutex
	symbol  string
	buckets map[int64]*MinuteBucket // MinuteKey -> Bucket
}

// NewRollingWindow creates a 24-hour sliding window tracker for a symbol.
func NewRollingWindow(symbol string) *RollingWindow {
	return &RollingWindow{
		symbol:  symbol,
		buckets: make(map[int64]*MinuteBucket),
	}
}

// AddTick records a new price tick into the appropriate 1-minute bucket.
func (rw *RollingWindow) AddTick(price float64, t time.Time) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	minKey := t.Unix() / 60

	b, exists := rw.buckets[minKey]
	if !exists {
		rw.buckets[minKey] = &MinuteBucket{
			MinuteKey: minKey,
			High:      price,
			Low:       price,
			Open:      price,
			Close:     price,
			TickCount: 1,
		}
	} else {
		if price > b.High {
			b.High = price
		}
		if price < b.Low {
			b.Low = price
		}
		b.Close = price
		b.TickCount++
	}

	// Prune buckets older than 24 hours (1440 minutes)
	cutoff := minKey - 1440
	for k := range rw.buckets {
		if k < cutoff {
			delete(rw.buckets, k)
		}
	}
}

// Get24hStats calculates the rolling 24h High, Low, and % Change.
func (rw *RollingWindow) Get24hStats(currentPrice float64, now time.Time) (float64, float64, float64) {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	if len(rw.buckets) == 0 {
		return currentPrice, currentPrice, 0.0
	}

	currentMin := now.Unix() / 60
	cutoff := currentMin - 1440

	high := currentPrice
	low := currentPrice
	var oldestMinKey int64 = math.MaxInt64
	var oldestOpen float64 = 0.0

	first := true
	for k, b := range rw.buckets {
		if k < cutoff {
			continue // Skip expired buckets
		}

		if first {
			high = b.High
			low = b.Low
			first = false
		} else {
			if b.High > high {
				high = b.High
			}
			if b.Low < low {
				low = b.Low
			}
		}

		// Track the oldest bucket in the 24h window to calculate % change
		if k < oldestMinKey {
			oldestMinKey = k
			oldestOpen = b.Open
		}
	}

	if currentPrice > high {
		high = currentPrice
	}
	if currentPrice < low {
		low = currentPrice
	}

	changePct := 0.0
	if oldestOpen > 0 {
		changePct = ((currentPrice - oldestOpen) / oldestOpen) * 100.0
	}

	return high, low, changePct
}

// ExportBuckets copies all active buckets for persistence.
func (rw *RollingWindow) ExportBuckets() map[int64]*MinuteBucket {
	rw.mu.RLock()
	defer rw.mu.RUnlock()

	res := make(map[int64]*MinuteBucket, len(rw.buckets))
	for k, v := range rw.buckets {
		bCopy := *v
		res[k] = &bCopy
	}
	return res
}

// ImportBuckets restores buckets from storage and prunes any that expired while offline.
func (rw *RollingWindow) ImportBuckets(buckets map[int64]*MinuteBucket, now time.Time) {
	rw.mu.Lock()
	defer rw.mu.Unlock()

	cutoff := (now.Unix() / 60) - 1440
	rw.buckets = make(map[int64]*MinuteBucket)

	for k, v := range buckets {
		if k >= cutoff {
			bCopy := *v
			rw.buckets[k] = &bCopy
		}
	}
}

// BucketCount returns current active buckets in memory.
func (rw *RollingWindow) BucketCount() int {
	rw.mu.RLock()
	defer rw.mu.RUnlock()
	return len(rw.buckets)
}
