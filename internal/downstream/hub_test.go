package downstream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"

	"real_time/internal/engine"
)

func TestPayloadOptimization_Under10KB(t *testing.T) {
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
	t.Logf("compact delta payload size: %d bytes (limit: 10,240 bytes)", payloadSize)

	if payloadSize >= 10240 {
		t.Fatalf("payload size %d bytes exceeded 10KB threshold", payloadSize)
	}

	if payloadSize > 300 {
		t.Errorf("expected compact payload < 300 bytes, got %d bytes", payloadSize)
	}
}

func TestPayloadSize_AllSymbols(t *testing.T) {
	symbols := []string{"BTCUSD", "ETHUSD", "EURUSD", "USDJPY", "XAUUSD", "SOLUSD"}
	for _, sym := range symbols {
		update := CompactPriceUpdate{
			Type:      "price_update",
			Symbol:    sym,
			Buy:       12345.6789,
			Sell:      12346.1234,
			Mid:       12345.9011,
			High24h:   13000.0000,
			Low24h:    12000.0000,
			Change24h: -5.43,
			Timestamp: time.Now().UnixMilli(),
			LatencyMs: 0.15,
		}
		data, err := json.Marshal(update)
		if err != nil {
			t.Fatalf("failed to marshal %s: %v", sym, err)
		}
		if len(data) >= 10240 {
			t.Errorf("symbol %s payload size %d exceeded 10KB", sym, len(data))
		}
	}
}

func TestClientSubscriptionFiltering(t *testing.T) {
	c := &Client{
		subscriptions: make(map[string]bool),
	}

	// Clients with empty subscriptions receive all symbols
	if !c.IsSubscribed("BTCUSD") || !c.IsSubscribed("ETHUSD") {
		t.Error("new client should receive all symbols by default")
	}

	// Filter down to specific symbols
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

	// Unsubscribe from a symbol
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

func TestServer_OriginChecking(t *testing.T) {
	wildcardCheck := makeCheckOrigin([]string{"*"})
	req1, _ := http.NewRequest("GET", "/ws", nil)
	req1.Header.Set("Origin", "http://evil.com")
	if !wildcardCheck(req1) {
		t.Error("wildcard should allow all origins")
	}

	strictCheck := makeCheckOrigin([]string{"http://trusted.com", "http://localhost:3000"})
	req2, _ := http.NewRequest("GET", "/ws", nil)
	if !strictCheck(req2) {
		t.Error("non-browser client without Origin header should be allowed")
	}

	req3, _ := http.NewRequest("GET", "/ws", nil)
	req3.Header.Set("Origin", "http://trusted.com")
	if !strictCheck(req3) {
		t.Error("trusted origin should be allowed")
	}

	req4, _ := http.NewRequest("GET", "/ws", nil)
	req4.Header.Set("Origin", "http://untrusted.com")
	if strictCheck(req4) {
		t.Error("untrusted origin should be blocked")
	}
}

func TestHub_ConcurrentLifecycle(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	clients := make([]*Client, 5)
	for i := 0; i < 5; i++ {
		clients[i] = &Client{
			hub:           hub,
			send:          make(chan []byte, 10),
			subscriptions: make(map[string]bool),
		}
		clients[i].Subscribe([]string{"BTCUSD"})
		hub.register <- clients[i]
	}

	time.Sleep(50 * time.Millisecond)
	if hub.ClientCount() != 5 {
		t.Fatalf("expected 5 clients, got %d", hub.ClientCount())
	}

	price := &engine.MarketPrice{
		Symbol:    "BTCUSD",
		Buy:       65000.0,
		Sell:      65010.0,
		MidPrice:  65005.0,
		Timestamp: time.Now(),
	}
	hub.Broadcast(price)

	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 3; i++ {
		hub.unregister <- clients[i]
	}

	time.Sleep(50 * time.Millisecond)
	if hub.ClientCount() != 2 {
		t.Fatalf("expected 2 clients remaining, got %d", hub.ClientCount())
	}
}

func TestServer_HealthEndpoint(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe)
	srv := NewServer("8080", hub, []string{"*"})

	// Verify health check on cold start before any ticks arrive
	rec := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/health", nil)
	srv.handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec.Code)
	}

	var res map[string]interface{}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if res["status"] != "healthy" {
		t.Errorf("expected status healthy, got %v", res["status"])
	}
	if res["active_clients"].(float64) != 0 {
		t.Errorf("expected 0 active clients on cold start, got %v", res["active_clients"])
	}
}

func TestServer_SnapshotEndpoint(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe)
	srv := NewServer("8080", hub, []string{"*"})

	// 1. Cold start snapshot: no ticks have arrived yet
	rec1 := httptest.NewRecorder()
	req1, _ := http.NewRequest("GET", "/snapshot", nil)
	srv.handleSnapshot(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", rec1.Code)
	}

	var res1 map[string]interface{}
	if err := json.Unmarshal(rec1.Body.Bytes(), &res1); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if res1["status"] != "ok" || res1["count"].(float64) != 0 {
		t.Errorf("expected count 0 on cold start, got %v", res1["count"])
	}

	// 2. Add tick and verify populated snapshot
	pe.ProcessTick(&engine.CleanTick{
		Symbol:    "ETHUSD",
		Buy:       3500.0,
		Sell:      3505.0,
		MidPrice:  3502.5,
		Timestamp: time.Now(),
	})

	rec2 := httptest.NewRecorder()
	req2, _ := http.NewRequest("GET", "/snapshot", nil)
	srv.handleSnapshot(rec2, req2)

	var res2 map[string]interface{}
	_ = json.Unmarshal(rec2.Body.Bytes(), &res2)
	if res2["count"].(float64) != 1 {
		t.Errorf("expected count 1 after tick, got %v", res2["count"])
	}
}

func TestHub_ValidateSymbols(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe, []string{"BTCUSD", "ETHUSD", "EURUSD"})

	valid, invalid := hub.ValidateSymbols([]string{"BTCUSD", "UNKNOWN", "ETHUSD", "FAKE"})
	if len(valid) != 2 || valid[0] != "BTCUSD" || valid[1] != "ETHUSD" {
		t.Errorf("unexpected valid symbols: %v", valid)
	}
	if len(invalid) != 2 || invalid[0] != "UNKNOWN" || invalid[1] != "FAKE" {
		t.Errorf("unexpected invalid symbols: %v", invalid)
	}
}

func TestEndToEnd_DeliveryLatency(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go hub.Run(ctx)

	const numClients = 10
	const numTicks = 200

	clients := make([]*Client, numClients)
	for i := 0; i < numClients; i++ {
		clients[i] = &Client{
			hub:           hub,
			send:          make(chan []byte, 2000),
			subscriptions: make(map[string]bool),
		}
		clients[i].Subscribe([]string{"BTCUSD"})
		hub.register <- clients[i]
	}

	stopCh := make(chan struct{})
	defer close(stopCh)
	for i := 1; i < numClients; i++ {
		go func(c *Client) {
			for {
				select {
				case <-c.send:
				case <-stopCh:
					return
				}
			}
		}(clients[i])
	}

	time.Sleep(20 * time.Millisecond)

	const samples = 20
	const batchSize = 250
	latencies := make([]float64, samples)

	for s := 0; s < samples; s++ {
		start := time.Now()
		for b := 0; b < batchSize; b++ {
			tick := &engine.CleanTick{
				Symbol:    "BTCUSD",
				Buy:       65000.0 + float64(s*batchSize+b),
				Sell:      65010.0 + float64(s*batchSize+b),
				MidPrice:  65005.0 + float64(s*batchSize+b),
				Timestamp: time.Now(),
			}

			enriched := pe.ProcessTick(tick)
			hub.Broadcast(enriched)
		}

		for b := 0; b < batchSize; b++ {
			select {
			case msg := <-clients[0].send:
				if len(msg) == 0 {
					t.Fatal("empty broadcast message")
				}
			case <-time.After(500 * time.Millisecond):
				t.Fatalf("timeout waiting for client 0 to receive batch sample %d", s)
			}
		}

		perTickUs := float64(time.Since(start).Nanoseconds()) / (float64(batchSize) * 1000.0)
		latencies[s] = perTickUs
	}

	sort.Float64s(latencies)
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[len(latencies)-1]

	t.Logf("=== In-Process Hub Broadcast Latency (%d concurrent clients, %d ticks total) ===", numClients, samples*batchSize)
	t.Logf("p50: %.2f µs (%.4f ms)", p50, p50/1000.0)
	t.Logf("p95: %.2f µs (%.4f ms)", p95, p95/1000.0)
	t.Logf("p99: %.2f µs (%.4f ms)", p99, p99/1000.0)

	// Verify SLA: p99 must be <= 50ms (50,000 µs)
	if p99 > 50000.0 {
		t.Fatalf("p99 delivery latency %.2f µs exceeded 50ms SLA", p99)
	}
}
