package engine

import (
	"errors"
	"testing"
)

func TestSanitizer_ValidTick(t *testing.T) {
	s := NewSanitizer(0.50)

	payload := `{"symbol":"BTCUSD","buy":65000.0,"sell":65010.0,"low_24h":60000.0,"high_24h":66000.0,"change_24h":2.5,"timestamp":1700000000000}`
	tick, err := s.SanitizeRaw([]byte(payload))
	if err != nil {
		t.Fatalf("expected valid tick, got error: %v", err)
	}

	if tick.Symbol != "BTCUSD" {
		t.Errorf("expected symbol BTCUSD, got %s", tick.Symbol)
	}
	if tick.MidPrice != 65005.0 {
		t.Errorf("expected mid price 65005.0, got %.2f", tick.MidPrice)
	}
}

func TestSanitizer_Deduplication(t *testing.T) {
	s := NewSanitizer(0.50)

	payload := `{"symbol":"EURUSD","buy":1.0850,"sell":1.0852,"timestamp":1700000001000}`
	_, err := s.SanitizeRaw([]byte(payload))
	if err != nil {
		t.Fatalf("first tick should succeed, got: %v", err)
	}

	// Send exact same timestamp again (duplicate)
	_, err = s.SanitizeRaw([]byte(payload))
	if !errors.Is(err, ErrDuplicateTick) {
		t.Errorf("expected ErrDuplicateTick, got: %v", err)
	}

	metrics := s.GetMetrics()
	if metrics.DroppedDuplicate != 1 {
		t.Errorf("expected DroppedDuplicate=1, got %d", metrics.DroppedDuplicate)
	}
}

func TestSanitizer_OutOfOrder(t *testing.T) {
	s := NewSanitizer(0.50)

	newer := `{"symbol":"ETHUSD","buy":3500.0,"sell":3502.0,"timestamp":1700000010000}`
	older := `{"symbol":"ETHUSD","buy":3495.0,"sell":3498.0,"timestamp":1700000005000}`

	if _, err := s.SanitizeRaw([]byte(newer)); err != nil {
		t.Fatalf("newer tick failed: %v", err)
	}

	// Older tick arrives after newer tick
	_, err := s.SanitizeRaw([]byte(older))
	if !errors.Is(err, ErrOutOfOrderTick) {
		t.Errorf("expected ErrOutOfOrderTick, got: %v", err)
	}

	metrics := s.GetMetrics()
	if metrics.DroppedOutOfOrder != 1 {
		t.Errorf("expected DroppedOutOfOrder=1, got %d", metrics.DroppedOutOfOrder)
	}
}

func TestSanitizer_PriceSpikeFilter(t *testing.T) {
	s := NewSanitizer(0.50) // 50% max allowable single-tick spike

	t1 := `{"symbol":"SOLUSD","buy":100.0,"sell":100.2,"timestamp":1700000001000}`
	if _, err := s.SanitizeRaw([]byte(t1)); err != nil {
		t.Fatalf("t1 failed: %v", err)
	}

	// Normal move: 100 -> 102 (2% change)
	t2 := `{"symbol":"SOLUSD","buy":102.0,"sell":102.2,"timestamp":1700000002000}`
	if _, err := s.SanitizeRaw([]byte(t2)); err != nil {
		t.Fatalf("t2 normal move should succeed, got: %v", err)
	}

	// Corrupted feed spike: 102 -> 250 (145% jump)
	tSpike := `{"symbol":"SOLUSD","buy":250.0,"sell":250.2,"timestamp":1700000003000}`
	_, err := s.SanitizeRaw([]byte(tSpike))
	if !errors.Is(err, ErrPriceSpike) {
		t.Errorf("expected ErrPriceSpike, got: %v", err)
	}

	metrics := s.GetMetrics()
	if metrics.DroppedSpike != 1 {
		t.Errorf("expected DroppedSpike=1, got %d", metrics.DroppedSpike)
	}
}

func TestSanitizer_InvalidPrices(t *testing.T) {
	s := NewSanitizer(0.50)

	badPayload := `{"symbol":"ADAUSD","buy":-1.0,"sell":0.5,"timestamp":1700000001000}`
	_, err := s.SanitizeRaw([]byte(badPayload))
	if !errors.Is(err, ErrInvalidPrice) {
		t.Errorf("expected ErrInvalidPrice, got: %v", err)
	}
}
