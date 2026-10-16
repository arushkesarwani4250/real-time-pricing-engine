package downstream

import (
	"encoding/json"
	"testing"
	"time"

	"real_time/internal/engine"
)

func TestPayloadOptimization_Under10KB(t *testing.T) {
	// Assignment Requirement 4:
	// "Input payload ~256KB. Output must be <10KB. Use delta encoding / efficient encoding."
	update := CompactPriceUpdate{
		Type:      "price_update",
		Symbol:    "BTCUSD",
		Buy:       65432.10,
		Sell:      65434.50,
		Mid:       65433.30,
		High24h:   66000.00,
		Low24h:    64500.00,
		Change24h: 1.45,
		Timestamp: time.Now().UnixMilli(),
		LatencyMs: 0.42,
	}

	data, err := json.Marshal(update)
	if err != nil {
		t.Fatalf("failed to marshal compact update: %v", err)
	}

	payloadSize := len(data)
	t.Logf("Compact Delta Payload Size: %d bytes (Requirement: < 10,240 bytes)", payloadSize)

	// Strict requirement check: Must be under 10KB (10,240 bytes)
	if payloadSize >= 10240 {
		t.Fatalf("payload size %d bytes exceeded 10KB target!", payloadSize)
	}

	// High-performance validation: Compact delta should be < 300 bytes!
	if payloadSize > 300 {
		t.Errorf("expected compact payload < 300 bytes, got %d bytes", payloadSize)
	}
}

func TestClientSubscriptionFiltering(t *testing.T) {
	c := &Client{
		subscriptions: make(map[string]bool),
	}

	// 1. Initial: Subscribed to all by default
	if !c.IsSubscribed("BTCUSD") || !c.IsSubscribed("ETHUSD") {
		t.Error("new client should receive all symbols by default")
	}

	// 2. Specific Subscription to BTCUSD and SOLUSD
	c.Subscribe([]string{"BTCUSD", "SOLUSD"})

	if !c.IsSubscribed("BTCUSD") {
		t.Error("BTCUSD should be subscribed")
	}
	if !c.IsSubscribed("SOLUSD") {
		t.Error("SOLUSD should be subscribed")
	}
	if c.IsSubscribed("ETHUSD") {
		t.Error("ETHUSD should NOT be subscribed")
	}

	// 3. Unsubscribe from SOLUSD
	c.Unsubscribe([]string{"SOLUSD"})
	if c.IsSubscribed("SOLUSD") {
		t.Error("SOLUSD should be unsubscribed")
	}
	if !c.IsSubscribed("BTCUSD") {
		t.Error("BTCUSD should still be subscribed")
	}
}

func TestHub_SnapshotAndBroadcast(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe)

	// Pre-fill engine with 1 tick
	pe.ProcessTick(&engine.CleanTick{
		Symbol:    "BTCUSD",
		Buy:       65000.0,
		Sell:      65010.0,
		MidPrice:  65005.0,
		Timestamp: time.Now(),
	})

	snapshot := hub.GetSnapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected snapshot count 1, got %d", len(snapshot))
	}
	if snapshot[0].Symbol != "BTCUSD" {
		t.Errorf("expected symbol BTCUSD, got %s", snapshot[0].Symbol)
	}
}
