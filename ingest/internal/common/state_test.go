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

	if len(sm.state) != 0 {
		t.Errorf("Expected empty state on new state manager, got %d entries", len(sm.state))
	}
}

func TestStateManager_MarkProcessed(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	filename := "test_file.db.zip"
	if err := sm.MarkProcessed(filename); err != nil {
		t.Fatalf("Failed to mark file as processed: %v", err)
	}

	if !sm.IsProcessed(filename) {
		t.Error("Expected file to be marked as processed")
	}

	if sm.IsFailed(filename) {
		t.Error("Expected file not to be marked as failed")
	}
}

func TestStateManager_MarkFailed(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	filename := "test_file.db.zip"
	errMsg := "test error message"
	if err := sm.MarkFailed(filename, errMsg); err != nil {
		t.Fatalf("Failed to mark file as failed: %v", err)
	}

	if !sm.IsFailed(filename) {
		t.Error("Expected file to be marked as failed")
	}

	if sm.IsProcessed(filename) {
		t.Error("Expected file not to be marked as processed")
	}

	entry := sm.state[filename]
	if entry.Error != errMsg {
		t.Errorf("Expected error message %q, got %q", errMsg, entry.Error)
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

	file1 := "file1.db.zip"
	file2 := "file2.db.zip"

	if err := sm1.MarkProcessed(file1); err != nil {
		t.Fatalf("Failed to mark file1 as processed: %v", err)
	}

	if err := sm1.MarkFailed(file2, "test error"); err != nil {
		t.Fatalf("Failed to mark file2 as failed: %v", err)
	}

	sm2, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to load state manager: %v", err)
	}

	if !sm2.IsProcessed(file1) {
		t.Error("Expected file1 to be processed after reload")
	}

	if !sm2.IsFailed(file2) {
		t.Error("Expected file2 to be failed after reload")
	}

	if len(sm2.state) != 2 {
		t.Errorf("Expected 2 entries in state after reload, got %d", len(sm2.state))
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

	if len(sm.state) != 0 {
		t.Errorf("Expected empty state, got %d entries", len(sm.state))
	}
}
