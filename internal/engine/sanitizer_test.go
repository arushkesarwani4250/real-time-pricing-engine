package engine

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestSanitizer_ValidTick(t *testing.T) {
	s := NewSanitizer(0.50)
	now := time.Now().UnixMilli()

	payload := fmt.Sprintf(`{"symbol":"BTCUSD","buy":65000.0,"sell":65010.0,"low_24h":60000.0,"high_24h":66000.0,"change_24h":2.5,"timestamp":%d}`, now)
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

func TestSanitizer_InvertedSpread(t *testing.T) {
	s := NewSanitizer(0.50)
	now := time.Now().UnixMilli()

	// Buy (Bid) > Sell (Ask) is an inverted/crossed spread
	payload := fmt.Sprintf(`{"symbol":"BTCUSD","buy":65020.0,"sell":65010.0,"timestamp":%d}`, now)
	_, err := s.SanitizeRaw([]byte(payload))
	if !errors.Is(err, ErrInvertedSpread) {
		t.Errorf("expected ErrInvertedSpread, got: %v", err)
	}
}

func TestSanitizer_NonFinitePrice(t *testing.T) {
	s := NewSanitizer(0.50)
	now := time.Now().UnixMilli()

	// Malformed JSON numbers (e.g. NaN, null, string prices)
	nanPayload := fmt.Sprintf(`{"symbol":"ETHUSD","buy":"NaN","sell":3500.0,"timestamp":%d}`, now)
	_, err := s.SanitizeRaw([]byte(nanPayload))
	if err == nil {
		t.Error("expected error for non-finite price string")
	}
}

func TestSanitizer_FutureTimestamp(t *testing.T) {
	s := NewSanitizer(0.50)
	futureTime := time.Now().Add(10 * time.Minute).UnixMilli()

	payload := fmt.Sprintf(`{"symbol":"SOLUSD","buy":150.0,"sell":150.2,"timestamp":%d}`, futureTime)
	_, err := s.SanitizeRaw([]byte(payload))
	if !errors.Is(err, ErrFutureTimestamp) {
		t.Errorf("expected ErrFutureTimestamp, got: %v", err)
	}
}

func TestSanitizer_StaleTimestamp(t *testing.T) {
	s := NewSanitizer(0.50)
	staleTime := time.Now().Add(-25 * time.Hour).UnixMilli()

	payload := fmt.Sprintf(`{"symbol":"SOLUSD","buy":150.0,"sell":150.2,"timestamp":%d}`, staleTime)
	_, err := s.SanitizeRaw([]byte(payload))
	if !errors.Is(err, ErrStaleTick) {
		t.Errorf("expected ErrStaleTick, got: %v", err)
	}
}

func TestSanitizer_UnknownSymbol(t *testing.T) {
	s := NewSanitizer(0.50)
	s.SetAllowedSymbols([]string{"BTCUSD", "ETHUSD"})
	now := time.Now().UnixMilli()

	// Allowed symbol
	validPayload := fmt.Sprintf(`{"symbol":"BTCUSD","buy":65000.0,"sell":65010.0,"timestamp":%d}`, now)
	if _, err := s.SanitizeRaw([]byte(validPayload)); err != nil {
		t.Fatalf("expected valid symbol to pass, got: %v", err)
	}

	// Unknown symbol
	badPayload := fmt.Sprintf(`{"symbol":"FAKECOIN","buy":1.0,"sell":1.1,"timestamp":%d}`, now)
	_, err := s.SanitizeRaw([]byte(badPayload))
	if !errors.Is(err, ErrUnknownSymbol) {
		t.Errorf("expected ErrUnknownSymbol, got: %v", err)
	}
}

func TestSanitizer_Deduplication(t *testing.T) {
	s := NewSanitizer(0.50)
	now := time.Now().UnixMilli()

	payload := fmt.Sprintf(`{"symbol":"EURUSD","buy":1.0850,"sell":1.0852,"timestamp":%d}`, now)
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
	now := time.Now().UnixMilli()

	newer := fmt.Sprintf(`{"symbol":"ETHUSD","buy":3500.0,"sell":3502.0,"timestamp":%d}`, now)
	older := fmt.Sprintf(`{"symbol":"ETHUSD","buy":3495.0,"sell":3498.0,"timestamp":%d}`, now-5000)

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
	s := NewSanitizer(0.50)
	now := time.Now().UnixMilli()

	t1 := fmt.Sprintf(`{"symbol":"SOLUSD","buy":100.0,"sell":100.2,"timestamp":%d}`, now)
	if _, err := s.SanitizeRaw([]byte(t1)); err != nil {
		t.Fatalf("t1 failed: %v", err)
	}

	// Normal move: 100 -> 102 (2% change)
	t2 := fmt.Sprintf(`{"symbol":"SOLUSD","buy":102.0,"sell":102.2,"timestamp":%d}`, now+1000)
	if _, err := s.SanitizeRaw([]byte(t2)); err != nil {
		t.Fatalf("t2 normal move should succeed, got: %v", err)
	}

	// Corrupted feed spike: 102 -> 250 (145% jump)
	tSpike := fmt.Sprintf(`{"symbol":"SOLUSD","buy":250.0,"sell":250.2,"timestamp":%d}`, now+2000)
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
	now := time.Now().UnixMilli()

	badPayload := fmt.Sprintf(`{"symbol":"ADAUSD","buy":-1.0,"sell":0.5,"timestamp":%d}`, now)
	_, err := s.SanitizeRaw([]byte(badPayload))
	if !errors.Is(err, ErrInvalidPrice) {
		t.Errorf("expected ErrInvalidPrice, got: %v", err)
	}
}
