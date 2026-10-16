package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"real_time/internal/engine"
)

// Store handles persisting and restoring pricing engine state across restarts.
type Store interface {
	SaveState(state *engine.EngineState) error
	LoadState() (*engine.EngineState, error)
}

// FileStore implements atomic file-based persistence on disk.
type FileStore struct {
	mu       sync.Mutex
	filePath string
}

// NewFileStore initializes a FileStore at the given path (e.g., "data/state.json").
func NewFileStore(filePath string) (*FileStore, error) {
	dir := filepath.Dir(filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory %s: %w", dir, err)
	}
	return &FileStore{filePath: filePath}, nil
}

// SaveState writes state atomically: writes to a temp file first, then atomically renames it.
// This guarantees zero file corruption even if the server crashes mid-write.
func (fs *FileStore) SaveState(state *engine.EngineState) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal engine state: %w", err)
	}

	tempPath := fs.filePath + ".tmp"
	if err := os.WriteFile(tempPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write temp state file: %w", err)
	}

	// Atomic rename replaces the existing state.json cleanly in 1 OS syscall
	if err := os.Rename(tempPath, fs.filePath); err != nil {
		return fmt.Errorf("failed to commit state file via atomic rename: %w", err)
	}

	return nil
}

// LoadState reads and restores engine state from disk.
// Returns nil if no prior state file exists (cold start).
func (fs *FileStore) LoadState() (*engine.EngineState, error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	if _, err := os.Stat(fs.filePath); os.IsNotExist(err) {
		return nil, nil // First time run; clean state
	}

	data, err := os.ReadFile(fs.filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read state file: %w", err)
	}

	var state engine.EngineState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("failed to unmarshal state file: %w", err)
	}

	return &state, nil
}
