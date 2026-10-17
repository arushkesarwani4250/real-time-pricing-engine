package upstream

import (
	"context"
	"testing"
	"time"
)

func TestSupervisor_BackoffProgression(t *testing.T) {
	cfg := SupervisorConfig{
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     800 * time.Millisecond,
		BackoffFactor:  2.0,
	}

	client := NewClient("ws://dummy", "key", "secret", nil)
	sup := NewSupervisor(client, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	next := sup.sleepWithBackoff(ctx, 100*time.Millisecond)
	if next < 200*time.Millisecond {
		t.Errorf("expected next backoff >= 200ms, got %v", next)
	}

	next = sup.sleepWithBackoff(ctx, next)
	if next < 400*time.Millisecond {
		t.Errorf("expected next backoff >= 400ms, got %v", next)
	}

	// Verify clamping at MaxBackoff
	next = sup.sleepWithBackoff(ctx, 600*time.Millisecond)
	if next > 800*time.Millisecond {
		t.Errorf("expected backoff to clamp at 800ms, got %v", next)
	}
}

func TestSupervisor_ContextCancellation(t *testing.T) {
	cfg := DefaultSupervisorConfig()
	client := NewClient("ws://localhost:9999/dummy", "key", "secret", nil)
	sup := NewSupervisor(client, cfg)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	out := make(chan []byte, 10)
	done := make(chan struct{})

	go func() {
		sup.Run(ctx, out)
		close(done)
	}()

	select {
	case <-done:
		// Succeeded: Supervisor cleanly exited on context timeout
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not exit on context cancellation; possible goroutine leak")
	}
}
