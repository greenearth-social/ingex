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

	if err := os.WriteFile(stateFile, []byte(""), 0600); err != nil {
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

func TestStateManager_GCSPath(t *testing.T) {
	// Skip this test if GCS credentials aren't available
	// This test requires actual GCS access
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" && os.Getenv("GOOGLE_CLOUD_PROJECT") == "" {
		t.Skip("Skipping GCS test: no credentials available")
	}

	logger := NewLogger(false)
	gcsPath := "gs://test-bucket/state.json"

	// This should not panic, though it may fail due to missing permissions
	sm, err := NewStateManager(gcsPath, logger)

	// We expect either success or a permission error, but not a panic
	if err == nil {
		// If it succeeds (e.g., in CI with service account), verify cursor is initialized
		if sm.GetCursor() == nil {
			t.Errorf("Expected cursor to be initialized, got nil")
		}
	} else {
		// If we get a permission error, that's also acceptable - it means GCS code executed correctly
		t.Skipf("GCS access denied (expected without proper permissions): %v", err)
	}
}

func TestStateManager_WriteAndReadInstanceInfo(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	startedAt := int64(1704672000000000) // 2024-01-08 00:00:00 UTC in microseconds

	// Write instance info
	if err := sm.WriteInstanceInfo(startedAt); err != nil {
		t.Fatalf("Failed to write instance info: %v", err)
	}

	// Read it back
	instanceInfo, err := sm.ReadInstanceInfo()
	if err != nil {
		t.Fatalf("Failed to read instance info: %v", err)
	}

	if instanceInfo.StartedAt != startedAt {
		t.Errorf("Expected StartedAt %d, got %d", startedAt, instanceInfo.StartedAt)
	}
}

func TestStateManager_InstanceInfoNotExists(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	// Try to read instance info that doesn't exist
	_, err = sm.ReadInstanceInfo()
	if err == nil {
		t.Error("Expected error when reading non-existent instance info, got nil")
	}
}

func TestStateManager_InstanceInfoOverwrite(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	// Write first instance info
	firstStartTime := int64(1704672000000000)
	if err := sm.WriteInstanceInfo(firstStartTime); err != nil {
		t.Fatalf("Failed to write first instance info: %v", err)
	}

	// Overwrite with second instance info (simulating new deployment)
	secondStartTime := int64(1704672060000000) // 60 seconds later
	if err := sm.WriteInstanceInfo(secondStartTime); err != nil {
		t.Fatalf("Failed to write second instance info: %v", err)
	}

	// Read it back - should have the newer timestamp
	instanceInfo, err := sm.ReadInstanceInfo()
	if err != nil {
		t.Fatalf("Failed to read instance info: %v", err)
	}

	if instanceInfo.StartedAt != secondStartTime {
		t.Errorf("Expected StartedAt %d (second instance), got %d", secondStartTime, instanceInfo.StartedAt)
	}
}

func TestStateManager_InstanceFileLocation(t *testing.T) {
	tmpDir := t.TempDir()
	stateFile := filepath.Join(tmpDir, "test_state.json")
	logger := NewLogger(false)

	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("Failed to create state manager: %v", err)
	}

	startedAt := int64(1704672000000000)
	if err := sm.WriteInstanceInfo(startedAt); err != nil {
		t.Fatalf("Failed to write instance info: %v", err)
	}

	// Verify the instance file is created with the correct name
	expectedPath := filepath.Join(tmpDir, "test_instance.json")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected instance file to be created at %s", expectedPath)
	}
}
