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

	// 1. Setup first engine and process some ticks
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

	// 2. Export state and save to disk
	stateToSave := pe1.ExportState()
	if err := store.SaveState(stateToSave); err != nil {
		t.Fatalf("failed to save state: %v", err)
	}

	// Verify file was created on disk
	if _, err := os.Stat(stateFile); os.IsNotExist(err) {
		t.Fatalf("state file was not created on disk: %s", stateFile)
	}

	// 3. Simulate process crash / server restart: Create BRAND NEW empty engine
	pe2 := engine.NewPricingEngine()
	if _, exists := pe2.GetPrice("BTCUSD"); exists {
		t.Fatal("new engine should have zero state before restore")
	}

	// 4. Load from disk and restore into pe2
	loadedState, err := store.LoadState()
	if err != nil {
		t.Fatalf("failed to load state from disk: %v", err)
	}

	restored := pe2.RestoreState(loadedState, now.Add(2*time.Minute))
	if restored != 1 {
		t.Fatalf("expected 1 symbol restored, got %d", restored)
	}

	// 5. Verify restored metrics survive restart!
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
