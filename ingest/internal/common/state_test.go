package common

import (
	"os"
	"path/filepath"
	"testing"
)

func TestStateManager_LoadState(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	// Cursor should be initialized to current time when no state file exists
	if sm.GetCursor() == nil {
		t.Errorf("Expected cursor to be initialized, got nil")
	}
}

func TestStateManager_UpdateCursor(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	timeUs := int64(1234567890000000)
	if err := sm.UpdateCursor(timeUs); err != nil {
		t.Fatalf("Failed to update cursor: %v", err)
	}

	cursor := sm.GetCursor()
	if cursor == nil {
		t.Fatal("Expected cursor to be set")
	}

	if cursor.LastTimeUs != timeUs {
		t.Errorf("Expected cursor time %d, got %d", timeUs, cursor.LastTimeUs)
	}
}

func TestStateManager_SaveAndLoad(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm1, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	timeUs := int64(9876543210000000)
	if err := sm1.UpdateCursor(timeUs); err != nil {
		t.Fatalf("Failed to update cursor: %v", err)
	}

	sm2, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to load state manager: %v", err)
	}

	cursor := sm2.GetCursor()
	if cursor == nil {
		t.Fatal("Expected cursor to be loaded")
	}

	if cursor.LastTimeUs != timeUs {
		t.Errorf("Expected cursor time %d after reload, got %d", timeUs, cursor.LastTimeUs)
	}
}

func TestStateManager_EmptyStateFile(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	if err := os.WriteFile(stateFile, []byte(""), 0644); err != nil {
		t.Fatalf("Failed to create empty state file: %v", err)
	}

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager with empty file: %v", err)
	}

	// Cursor should be initialized to current time when state file is empty
	if sm.GetCursor() == nil {
		t.Errorf("Expected cursor to be initialized, got nil")
	}
}
