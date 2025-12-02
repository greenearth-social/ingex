package common

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
)

// CursorState represents the current processing position and metadata for file ingestion
type CursorState struct {
	LastTimeUs int64     `json:"last_time_us"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// StateManager manages file processing state and cursor position
type StateManager struct {
	stateFilePath string
	mu            sync.RWMutex
	cursor        *CursorState
	logger        *IngestLogger
	gcsClient     *storage.Client
	gcsBucket     string
	gcsObject     string
	useGCS        bool
}

// NewStateManager creates a new state manager with the given state file path
// Supports both local file paths and GCS paths (gs://bucket/path/to/file)
func NewStateManager(stateFilePath string, logger *IngestLogger) (*StateManager, error) {
	sm := &StateManager{
		stateFilePath: stateFilePath,
		logger:        logger,
	}

	// Check if this is a GCS path
	if strings.HasPrefix(stateFilePath, "gs://") {
		sm.useGCS = true
		// Parse GCS path: gs://bucket/object
		path := strings.TrimPrefix(stateFilePath, "gs://")
		parts := strings.SplitN(path, "/", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid GCS path format: %s (expected gs://bucket/object)", stateFilePath)
		}
		sm.gcsBucket = parts[0]
		sm.gcsObject = parts[1]

		// Initialize GCS client
		ctx := context.Background()
		client, err := storage.NewClient(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to create GCS client: %w", err)
		}
		sm.gcsClient = client
		logger.Info("Using GCS for state storage: gs://%s/%s", sm.gcsBucket, sm.gcsObject)
	} else {
		logger.Info("Using local filesystem for state storage: %s", stateFilePath)
	}

	if err := sm.LoadState(); err != nil {
		return nil, err
	}

	// Initialize cursor to current time if no state was loaded
	if sm.cursor == nil {
		sm.cursor = &CursorState{
			LastTimeUs: time.Now().UnixMicro(),
			UpdatedAt:  time.Now().UTC(),
		}
		sm.logger.Info("No existing state found, initialized cursor to current time: %d", sm.cursor.LastTimeUs)
	}

	return sm, nil
}

// LoadState loads the processing state from the state file
func (sm *StateManager) LoadState() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var data []byte
	var err error

	if sm.useGCS {
		// Load from GCS
		ctx := context.Background()
		reader, err := sm.gcsClient.Bucket(sm.gcsBucket).Object(sm.gcsObject).NewReader(ctx)
		if err != nil {
			if err == storage.ErrObjectNotExist {
				sm.logger.Info("State file does not exist in GCS, starting with empty state")
				return nil
			}
			return fmt.Errorf("failed to read state from GCS: %w", err)
		}
		defer reader.Close()

		data = make([]byte, reader.Attrs.Size)
		_, err = reader.Read(data)
		if err != nil && err.Error() != "EOF" {
			return fmt.Errorf("failed to read GCS object: %w", err)
		}
	} else {
		// Load from local filesystem
		if _, err := os.Stat(sm.stateFilePath); os.IsNotExist(err) {
			sm.logger.Info("State file does not exist, starting with empty state")
			return nil
		}

		data, err = os.ReadFile(sm.stateFilePath)
		if err != nil {
			return fmt.Errorf("failed to read state file: %w", err)
		}
	}

	if len(data) == 0 {
		sm.logger.Info("State file is empty, starting with empty state")
		return nil
	}

	if err := json.Unmarshal(data, &sm.cursor); err != nil {
		return fmt.Errorf("failed to parse state file: %w", err)
	}

	if sm.cursor != nil {
		sm.logger.Info("Loaded state with cursor (last_time_us: %d)", sm.cursor.LastTimeUs)
	} else {
		sm.logger.Info("Loaded empty state")
	}
	return nil
}

// GetCursor returns the current cursor state indicating the last processed timestamp
func (sm *StateManager) GetCursor() *CursorState {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.cursor
}

// UpdateCursor updates the cursor state with a new timestamp
func (sm *StateManager) UpdateCursor(timeUs int64) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.cursor = &CursorState{
		LastTimeUs: timeUs,
		UpdatedAt:  time.Now().UTC(),
	}

	data, err := json.MarshalIndent(sm.cursor, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if sm.useGCS {
		// Write to GCS
		ctx := context.Background()
		writer := sm.gcsClient.Bucket(sm.gcsBucket).Object(sm.gcsObject).NewWriter(ctx)
		if _, err := writer.Write(data); err != nil {
			writer.Close()
			return fmt.Errorf("failed to write state to GCS: %w", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close GCS writer: %w", err)
		}
	} else {
		// Write to local filesystem
		if err := os.WriteFile(sm.stateFilePath, data, 0600); err != nil {
			return fmt.Errorf("failed to write state file: %w", err)
		}
	}

	return nil
}
