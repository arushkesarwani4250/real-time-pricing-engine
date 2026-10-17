package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"real_time/internal/engine"
)

func TestFileStore_SaveAndRestore(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "state.json")

	store, err := NewFileStore(stateFile)
	if err != nil {
		t.Fatalf("failed to create FileStore: %v", err)
	}

	pe1 := engine.NewPricingEngine()
	now := time.Now()

	pe1.ProcessTick(&engine.CleanTick{
		Symbol:    "BTCUSD",
		Buy:       65000.0,
		Sell:      65010.0,
		MidPrice:  65005.0,
		Timestamp: now,
	})
	pe1.ProcessTick(&engine.CleanTick{
		Symbol:    "BTCUSD",
		Buy:       66000.0,
		Sell:      66010.0,
		MidPrice:  66005.0,
		Timestamp: now.Add(1 * time.Minute),
	})

	stateToSave := pe1.ExportState()
	if err := store.SaveState(stateToSave); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatalf("state file was not created on disk: %s", stateFile)
	}

	// Verify fresh engine instance restores correctly from stored state
	pe2 := engine.NewPricingEngine()
	if _, exists := pe2.GetPrice("BTCUSD"); exists {
		t.Fatal("new engine should have zero state before restore")
	}

	loadedState, err := store.LoadState()
	if err != nil {
		t.Fatalf("failed to load state from disk: %v", err)
	}

	restored := pe2.RestoreState(loadedState, now.Add(2*time.Minute))
	if restored != 1 {
		t.Fatalf("expected 1 symbol restored, got %d", restored)
	}

	price, exists := pe2.GetPrice("BTCUSD")
	if !exists {
		t.Fatal("BTCUSD should exist in restored engine")
	}

	if price.High24h != 66005.0 {
		t.Errorf("expected restored High24h 66005.0, got %.2f", price.High24h)
	}
	if price.Low24h != 65005.0 {
		t.Errorf("expected restored Low24h 65005.0, got %.2f", price.Low24h)
	}
}

func TestFileStore_ColdStart(t *testing.T) {
	tempDir := t.TempDir()
	nonExistentFile := filepath.Join(tempDir, "non_existent.json")

	store, err := NewFileStore(nonExistentFile)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}

	state, err := store.LoadState()
	if err != nil {
		t.Fatalf("expected nil error on cold start, got: %v", err)
	}
	if state != nil {
		t.Fatalf("expected nil state on cold start, got: %+v", state)
	}
}

func TestFileStore_CorruptedFileRecovery(t *testing.T) {
	tempDir := t.TempDir()
	stateFile := filepath.Join(tempDir, "corrupted_state.json")

	corruptedData := []byte(`{"saved_at":"2026-01-01T00:00:00Z", "symbols": { "BTCUSD": {"incomplete json...`)
	if err := os.WriteFile(stateFile, corruptedData, 0644); err != nil {
		t.Fatalf("failed to write corrupted file: %v", err)
	}

	store, err := NewFileStore(stateFile)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}

	state, err := store.LoadState()
	if err == nil {
		t.Fatal("expected error when loading corrupted state file, got nil")
	}
	if state != nil {
		t.Fatalf("expected nil state on corrupted file, got: %+v", state)
	}

	// Verify that saving valid state overwrites corrupted file cleanly
	pe := engine.NewPricingEngine()
	pe.ProcessTick(&engine.CleanTick{
		Symbol:    "ETHUSD",
		Buy:       3500.0,
		Sell:      3505.0,
		MidPrice:  3502.5,
		Timestamp: time.Now(),
	})

	cleanState := pe.ExportState()
	if err := store.SaveState(cleanState); err != nil {
		t.Fatalf("failed to overwrite corrupted file with clean state: %v", err)
	}

	recoveredState, err := store.LoadState()
	if err != nil {
		t.Fatalf("expected clean state after overwrite, got error: %v", err)
	}
	if recoveredState == nil || len(recoveredState.Symbols) != 1 {
		t.Fatalf("expected 1 recovered symbol, got: %+v", recoveredState)
	}
}

func TestFileStore_EmptyFile(t *testing.T) {
	tempDir := t.TempDir()
	emptyFile := filepath.Join(tempDir, "empty_state.json")

	if err := os.WriteFile(emptyFile, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}

	store, err := NewFileStore(emptyFile)
	if err != nil {
		t.Fatalf("NewFileStore failed: %v", err)
	}

	state, err := store.LoadState()
	if err != nil {
		t.Fatalf("expected nil error on empty state file, got: %v", err)
	}
	if state != nil {
		t.Fatalf("expected nil state on empty state file, got: %+v", state)
	}
}
