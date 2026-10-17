package upstream

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"time"
)

// SupervisorConfig controls the retry and reconnection dynamics.
type SupervisorConfig struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
}

// DefaultSupervisorConfig returns sensible production defaults for connection retries.
func DefaultSupervisorConfig() SupervisorConfig {
	return SupervisorConfig{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		BackoffFactor:  2.0,
	}
}

// Supervisor manages the connection lifecycle, automatic reconnection, and backoff of the Upstream Client.
type Supervisor struct {
	client     *Client
	config     SupervisorConfig
	mu         sync.Mutex
	reconnects int64
}

// NewSupervisor creates a supervisor for the given upstream client.
func NewSupervisor(client *Client, cfg SupervisorConfig) *Supervisor {
	return &Supervisor{
		client: client,
		config: cfg,
	}
}

// Run executes the resilient supervision loop. It automatically reconnects, authenticates,
// and resubscribes whenever the upstream connection is dropped.
func (s *Supervisor) Run(ctx context.Context, out chan<- []byte) {
	currentBackoff := s.config.InitialBackoff

	for {
		select {
		case <-ctx.Done():
			log.Println("[Supervisor] Shutdown signal received. Stopping upstream supervisor.")
			_ = s.client.Close()
			return
		default:
		}

		log.Println("[Supervisor] Establishing upstream WebSocket connection...")

		if err := s.client.Connect(); err != nil {
			log.Printf("[Supervisor] Upstream connection failed: %v", err)
			currentBackoff = s.sleepWithBackoff(ctx, currentBackoff)
			continue
		}

		if err := s.client.Authenticate(); err != nil {
			log.Printf("[Supervisor] Upstream authentication failed: %v", err)
			_ = s.client.Close()

			// Fast-fail if credentials are fundamentally rejected to avoid IP ban
			if errors.Is(err, ErrAuthRejected) {
				log.Println("[Supervisor] Authentication rejected by upstream provider (invalid credentials).")
				log.Println("[Supervisor] Check API_KEY and SECRET_KEY, or run with MOCK_FEED=true for simulated data.")
				log.Println("[Supervisor] Halting supervisor to prevent IP rate-limiting.")
				return
			}

			currentBackoff = s.sleepWithBackoff(ctx, currentBackoff)
			continue
		}

		if err := s.client.Subscribe(); err != nil {
			log.Printf("[Supervisor] Upstream subscription failed: %v", err)
			_ = s.client.Close()
			currentBackoff = s.sleepWithBackoff(ctx, currentBackoff)
			continue
		}

		s.mu.Lock()
		s.reconnects++
		s.mu.Unlock()
		currentBackoff = s.config.InitialBackoff
		log.Println("[Supervisor] Connected and subscribed. Streaming market data...")

		err := s.client.Listen(ctx, out)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				log.Printf("[Supervisor] Upstream disconnected: %v. Preparing reconnection...", err)
				currentBackoff = s.sleepWithBackoff(ctx, currentBackoff)
			}
		}
	}
}

// sleepWithBackoff pauses execution with exponential backoff and randomized jitter to prevent thundering herds.
func (s *Supervisor) sleepWithBackoff(ctx context.Context, current time.Duration) time.Duration {
	// Add 10-20% jitter
	jitter := time.Duration(rand.Int63n(int64(current / 5)))
	sleepDuration := current + jitter

	log.Printf("[Supervisor] Waiting %v before attempting reconnection...", sleepDuration.Round(time.Millisecond))

	select {
	case <-time.After(sleepDuration):
	case <-ctx.Done():
		return current
	}

	next := time.Duration(float64(current) * s.config.BackoffFactor)
	if next > s.config.MaxBackoff {
		next = s.config.MaxBackoff
	}
	return next
}

// ReconnectCount returns the number of successful connection establishments.
func (s *Supervisor) ReconnectCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reconnects
}
