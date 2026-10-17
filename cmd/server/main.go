package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"real_time/config"
	"real_time/internal/downstream"
	"real_time/internal/engine"
	"real_time/internal/storage"
	"real_time/internal/upstream"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("failed to load configuration: %v", err)
	}

	log.Printf("Starting pricing engine (port=%s, upstream=%s, key=%s)",
		cfg.Port, cfg.UpstreamWSURL, config.MaskKey(cfg.APIKey))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Persistence store for surviving process restarts
	store, err := storage.NewFileStore("data/state.json")
	if err != nil {
		log.Fatalf("failed to initialize state store: %v", err)
	}

	sanitizer := engine.NewSanitizer(0.50)
	sanitizer.SetAllowedSymbols(upstream.AllRequiredSymbols)
	pricingEngine := engine.NewPricingEngine()

	// Restore historical sliding window on startup
	if savedState, err := store.LoadState(); err != nil {
		log.Printf("warning: could not load previous state: %v", err)
	} else if savedState != nil {
		restored := pricingEngine.RestoreState(savedState, time.Now())
		log.Printf("restored state for %d symbols from data/state.json", restored)
	} else {
		log.Println("cold start: no previous state file found")
	}

	// Downstream pub/sub hub and HTTP/WS server
	hub := downstream.NewHub(pricingEngine, upstream.AllRequiredSymbols)
	go hub.Run(ctx)

	server := downstream.NewServer(cfg.Port, hub, cfg.AllowedOrigins)
	go func() {
		if err := server.Start(ctx); err != nil {
			log.Printf("downstream server stopped: %v", err)
		}
	}()

	rawTickChan := make(chan []byte, 5000)

	// Upstream ingestion supervisor or local mock feed
	if cfg.MockFeed {
		log.Println("mock feed enabled: streaming simulated market ticks")
		mock := upstream.NewMockFeed(upstream.AllRequiredSymbols)
		go mock.Run(ctx, rawTickChan, 20*time.Millisecond)
	} else {
		client := upstream.NewClient(cfg.UpstreamWSURL, cfg.APIKey, cfg.SecretKey, upstream.AllRequiredSymbols)
		supervisor := upstream.NewSupervisor(client, upstream.DefaultSupervisorConfig())
		go supervisor.Run(ctx, rawTickChan)
	}

	// Processing pipeline: sanitize tick -> compute rolling metrics -> broadcast delta
	go func() {
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
					continue
				}

				enriched := pricingEngine.ProcessTick(cleanTick)
				hub.Broadcast(enriched)

				m := sanitizer.GetMetrics()
				if m.ValidProcessed%50 == 0 {
					log.Printf("[Tick #%4d] %-7s | Mid: %9.4f | 24h High: %9.4f | 24h Low: %9.4f | 24h Chg: %+6.2f%% | Clients: %d",
						m.ValidProcessed,
						enriched.Symbol,
						enriched.MidPrice,
						enriched.High24h,
						enriched.Low24h,
						enriched.Change24h,
						hub.ClientCount(),
					)
				}
			}
		}
	}()

	// Periodic snapshot persistence
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				state := pricingEngine.ExportState()
				if err := store.SaveState(state); err != nil {
					log.Printf("periodic state snapshot failed: %v", err)
				}
			}
		}
	}()

	log.Printf("pricing engine online at :%s", cfg.Port)

	<-ctx.Done()
	log.Println("shutdown signal received, flushing state...")

	finalState := pricingEngine.ExportState()
	if err := store.SaveState(finalState); err != nil {
		log.Printf("failed to save final state snapshot: %v", err)
	}

	m := sanitizer.GetMetrics()
	log.Printf("shutdown complete: processed %d ticks, active clients: %d", m.ValidProcessed, hub.ClientCount())
}
