package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real_time/config"
	"real_time/internal/engine"
	"real_time/internal/upstream"
)

func main() {
	log.Println("[Main] Initializing Real-Time Pricing Engine...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("[Main] Configuration error: %v", err)
	}

	log.Printf("[Main] Configuration loaded.")
	log.Printf("[Main] Upstream WS: %s", cfg.UpstreamWSURL)
	log.Printf("[Main] API Key:     %s", config.MaskKey(cfg.APIKey))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// 1. Buffered channel for raw inbound ticks from upstream
	rawTickChan := make(chan []byte, 5000)

	// 2. Initialize the Sanitizer (Milestone 3)
	sanitizer := engine.NewSanitizer(0.50) // 50% single-tick spike limit

	// 3. Connect to live upstream or fallback to simulation feed
	client := upstream.NewClient(cfg.UpstreamWSURL, cfg.APIKey, cfg.SecretKey, upstream.AllRequiredSymbols)

	useMock := cfg.MockFeed
	if !useMock {
		if err := client.Connect(); err != nil {
			log.Printf("[Main] Live upstream connect failed: %v. Falling back to test feed generator.", err)
			useMock = true
		} else if err := client.Authenticate(); err != nil {
			log.Printf("[Main] Live upstream auth rejected: %v. Falling back to test feed generator.", err)
			useMock = true
		} else if err := client.Subscribe(); err != nil {
			log.Printf("[Main] Live upstream subscribe failed: %v. Falling back to test feed generator.", err)
			useMock = true
		}
	}

	if useMock {
		log.Println("[Main] Running with Mock Feed Generator (simulating 61 live currency pairs).")
		mock := upstream.NewMockFeed(upstream.AllRequiredSymbols)
		go mock.Run(ctx, rawTickChan, 20*time.Millisecond) // ~50 ticks/sec
	} else {
		log.Println("[Main] Streaming live market data from upstream WebSocket...")
		go func() {
			if err := client.Listen(ctx, rawTickChan); err != nil {
				log.Printf("[Main] Upstream listener error: %v", err)
			}
		}()
	}

	// 4. Milestone 3 Processing Worker: Reads raw ticks, sanitizes, and deduplicates
	go func() {
		log.Println("[Engine Worker] Sanitizer pipeline active...")
		for {
			select {
			case <-ctx.Done():
				return
			case rawData, ok := <-rawTickChan:
				if !ok {
					return
				}

				cleanTick, err := sanitizer.SanitizeRaw(rawData)
				if err != nil {
					// Log dropped anomalies
					continue
				}

				// Periodically log clean ticks to confirm ingestion
				m := sanitizer.GetMetrics()
				if m.ValidProcessed%100 == 0 {
					log.Printf("[Engine] [Tick #%d] Symbol: %-7s | MidPrice: %.5f | Buy: %.5f | Sell: %.5f",
						m.ValidProcessed, cleanTick.Symbol, cleanTick.MidPrice, cleanTick.Buy, cleanTick.Sell)
				}
			}
		}
	}()

	log.Println("[Main] Engine running smoothly! Press Ctrl+C to stop.")

	<-ctx.Done()
	log.Println("[Main] Shutdown signal received, terminating...")
	time.Sleep(200 * time.Millisecond)

	m := sanitizer.GetMetrics()
	log.Printf("[Main] Final Ingestion Stats: Total=%d | Valid=%d | Duplicates=%d | OutOfOrder=%d | Spikes=%d",
		m.TotalReceived, m.ValidProcessed, m.DroppedDuplicate, m.DroppedOutOfOrder, m.DroppedSpike)

	log.Println("[Main] Clean shutdown complete.")
}
