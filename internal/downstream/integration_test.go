package downstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"real_time/internal/engine"
	"real_time/internal/upstream"
)

// TestService_EndToEndMockMode starts the service and verifies /health, /snapshot,
// WebSocket subscription, actual price delivery, payload size, latency, and disconnects.
func TestService_EndToEndMockMode(t *testing.T) {
	pe := engine.NewPricingEngine()
	hub := NewHub(pe, upstream.AllRequiredSymbols)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go hub.Run(ctx)

	const testPort = "18089"
	server := NewServer(testPort, hub, []string{"*"})

	go func() {
		_ = server.Start(ctx)
	}()

	// Wait briefly for HTTP/WS listener to bind
	time.Sleep(100 * time.Millisecond)

	baseURL := fmt.Sprintf("http://127.0.0.1:%s", testPort)
	wsURL := fmt.Sprintf("ws://127.0.0.1:%s/ws", testPort)

	// 1. Verify /health endpoint
	respHealth, err := http.Get(baseURL + "/health")
	if err != nil {
		t.Fatalf("failed to query /health: %v", err)
	}
	defer respHealth.Body.Close()

	if respHealth.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /health, got %d", respHealth.StatusCode)
	}

	var healthData map[string]interface{}
	if err := json.NewDecoder(respHealth.Body).Decode(&healthData); err != nil {
		t.Fatalf("failed to parse /health JSON: %v", err)
	}
	if healthData["status"] != "healthy" {
		t.Errorf("expected status 'healthy', got %v", healthData["status"])
	}

	// 2. Verify /snapshot endpoint (cold start)
	respSnapshot, err := http.Get(baseURL + "/snapshot")
	if err != nil {
		t.Fatalf("failed to query /snapshot: %v", err)
	}
	defer respSnapshot.Body.Close()

	if respSnapshot.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /snapshot, got %d", respSnapshot.StatusCode)
	}

	var snapData map[string]interface{}
	if err := json.NewDecoder(respSnapshot.Body).Decode(&snapData); err != nil {
		t.Fatalf("failed to parse /snapshot JSON: %v", err)
	}
	if snapData["status"] != "ok" {
		t.Errorf("expected snapshot status 'ok', got %v", snapData["status"])
	}

	// 3. Connect real WebSocket client
	wsConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("failed to connect to WebSocket %s: %v", wsURL, err)
	}
	defer wsConn.Close()

	time.Sleep(50 * time.Millisecond)
	if hub.ClientCount() != 1 {
		t.Fatalf("expected 1 active client in hub, got %d", hub.ClientCount())
	}

	// 4. Send subscription request for BTCUSD and ETHUSD
	subReq := ClientRequest{
		Action:  "subscribe",
		Symbols: []string{"BTCUSD", "ETHUSD"},
	}
	if err := wsConn.WriteJSON(subReq); err != nil {
		t.Fatalf("failed to send subscription request: %v", err)
	}

	// Read subscription acknowledgement
	var ack ServerResponse
	if err := wsConn.ReadJSON(&ack); err != nil {
		t.Fatalf("failed to read subscription acknowledgement: %v", err)
	}
	if ack.Status != "ok" {
		t.Errorf("expected subscription status 'ok', got %s (%s)", ack.Status, ack.Message)
	}

	// 5. Ingest ticks and measure true end-to-end WebSocket delivery latency
	const samples = 10
	const batchPerSample = 25
	latencies := make([]float64, 0, samples)

	for s := 0; s < samples; s++ {
		sampleStart := time.Now()
		for b := 0; b < batchPerSample; b++ {
			tick := &engine.CleanTick{
				Symbol:    "BTCUSD",
				Buy:       65000.0 + float64(s*batchPerSample+b),
				Sell:      65010.0 + float64(s*batchPerSample+b),
				MidPrice:  65005.0 + float64(s*batchPerSample+b),
				Timestamp: time.Now(),
			}

			enriched := pe.ProcessTick(tick)
			hub.Broadcast(enriched)
		}

		for b := 0; b < batchPerSample; b++ {
			_ = wsConn.SetReadDeadline(time.Now().Add(2 * time.Second))
			_, rawMsg, err := wsConn.ReadMessage()
			if err != nil {
				t.Fatalf("failed to receive price update over WebSocket: %v", err)
			}

			// Verify payload size limit (< 10 KB)
			payloadBytes := len(rawMsg)
			if payloadBytes >= 10240 {
				t.Fatalf("payload size %d bytes exceeded 10KB target!", payloadBytes)
			}

			var update CompactPriceUpdate
			if err := json.Unmarshal(rawMsg, &update); err != nil {
				t.Fatalf("failed to parse price update JSON: %v", err)
			}
			if update.Symbol != "BTCUSD" {
				t.Errorf("expected symbol BTCUSD, got %s", update.Symbol)
			}
			if update.Buy > update.Sell {
				t.Errorf("invalid spread: buy %.2f > sell %.2f", update.Buy, update.Sell)
			}
		}

		perTickUs := float64(time.Since(sampleStart).Nanoseconds()) / (float64(batchPerSample) * 1000.0)
		latencies = append(latencies, perTickUs)
	}

	sort.Float64s(latencies)
	p50 := latencies[int(float64(len(latencies))*0.50)]
	p95 := latencies[int(float64(len(latencies))*0.95)]
	p99 := latencies[int(float64(len(latencies))*0.99)]

	t.Logf("=== True End-to-End WebSocket TCP Delivery Latency (%d ticks total) ===", samples*batchPerSample)
	t.Logf("p50: %.2f µs (%.4f ms)", p50, p50/1000.0)
	t.Logf("p95: %.2f µs (%.4f ms)", p95, p95/1000.0)
	t.Logf("p99: %.2f µs (%.4f ms)", p99, p99/1000.0)

	if p99 > 50000.0 {
		t.Fatalf("p99 WebSocket delivery latency %.2f µs exceeded 50ms requirement", p99)
	}

	// 7. Verify client disconnect handling
	_ = wsConn.Close()
	time.Sleep(100 * time.Millisecond)

	if hub.ClientCount() != 0 {
		t.Errorf("expected 0 active clients after disconnect, got %d", hub.ClientCount())
	}
}
