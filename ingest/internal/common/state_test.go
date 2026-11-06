package common

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"
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

func TestStateManager_ContextSaveAndLoad(t *testing.T) {
	ctx := context.Background()
	stateFile := filepath.Join(t.TempDir(), "state.json")
	logger := NewLogger(false)
	sm, err := NewStateManagerWithContext(ctx, stateFile, logger)
	if err != nil {
		t.Fatalf("NewStateManagerWithContext: %v", err)
	}
	if err := sm.UpdateCursorContext(ctx, 123); err != nil {
		t.Fatalf("UpdateCursorContext: %v", err)
	}
	reloaded, err := NewStateManagerWithContext(ctx, stateFile, logger)
	if err != nil {
		t.Fatalf("reload state manager: %v", err)
	}
	if got := reloaded.GetCursor().LastTimeUs; got != 123 {
		t.Fatalf("persisted cursor = %d, want 123", got)
	}
	if err := sm.UpdateCursorContext(ctx, 456); err != nil {
		t.Fatalf("second UpdateCursorContext: %v", err)
	}
	if err := reloaded.LoadStateContext(ctx); err != nil {
		t.Fatalf("LoadStateContext: %v", err)
	}
	if got := reloaded.GetCursor().LastTimeUs; got != 456 {
		t.Fatalf("reloaded cursor = %d, want 456", got)
	}
	if err := sm.Close(); err != nil {
		t.Fatalf("Close local state manager: %v", err)
	}
}

func TestStateManager_CancelledContextDoesNotWriteCursor(t *testing.T) {
	stateFile := filepath.Join(t.TempDir(), "state.json")
	logger := NewLogger(false)
	sm, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("NewStateManager: %v", err)
	}
	if err := sm.UpdateCursor(123); err != nil {
		t.Fatalf("UpdateCursor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sm.UpdateCursorContext(ctx, 456); !errors.Is(err, context.Canceled) {
		t.Fatalf("UpdateCursorContext error = %v, want context.Canceled", err)
	}
	if got := sm.GetCursor().LastTimeUs; got != 123 {
		t.Fatalf("cached cursor changed after cancellation: %d", got)
	}
	reloaded, err := NewStateManager(stateFile, logger)
	if err != nil {
		t.Fatalf("reload state manager: %v", err)
	}
	if got := reloaded.GetCursor().LastTimeUs; got != 123 {
		t.Fatalf("persisted cursor changed after cancellation: %d", got)
	}
	if err := sm.LoadStateContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("LoadStateContext error = %v, want context.Canceled", err)
	}
	if _, err := NewStateManagerWithContext(ctx, stateFile, logger); !errors.Is(err, context.Canceled) {
		t.Fatalf("NewStateManagerWithContext error = %v, want context.Canceled", err)
	}
}

type stateContextTransport func(*http.Request) (*http.Response, error)

func (f stateContextTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestStateManager_GCSOperationsCancelInFlight(t *testing.T) {
	// Hold the HTTP operation open until its own context is canceled. This checks
	// propagation through the GCS client, rather than only the initial ctx.Err check.
	for _, operation := range []string{"load", "update"} {
		t.Run(operation, func(t *testing.T) {
			started := make(chan struct{}, 1)
			client, err := storage.NewClient(context.Background(), option.WithHTTPClient(&http.Client{
				Transport: stateContextTransport(func(req *http.Request) (*http.Response, error) {
					select {
					case started <- struct{}{}:
					default:
					}
					<-req.Context().Done()
					return nil, req.Context().Err()
				}),
			}))
			if err != nil {
				t.Fatalf("storage.NewClient: %v", err)
			}
			sm := &StateManager{
				gcsClient: client,
				gcsBucket: "test-bucket",
				gcsObject: "state.json",
				useGCS:    true,
				logger:    NewLogger(false),
				cursor:    &CursorState{LastTimeUs: 123},
			}
			t.Cleanup(func() {
				if err := sm.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if operation == "load" {
					done <- sm.LoadStateContext(ctx)
				} else {
					done <- sm.UpdateCursorContext(ctx, 456)
				}
			}()
			select {
			case <-started:
			case <-time.After(5 * time.Second):
				t.Fatal("GCS operation did not start")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("GCS operation error = %v, want context.Canceled", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("GCS operation did not stop after cancellation")
			}
			if got := sm.GetCursor().LastTimeUs; got != 123 {
				t.Fatalf("cached cursor changed after failed GCS operation: %d", got)
			}
		})
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
