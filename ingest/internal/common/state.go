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
		defer func() { _ = reader.Close() }() // Best-effort close for read operation

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
			_ = writer.Close() // Best-effort close on error
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

// InstanceInfo represents information about a running instance
type InstanceInfo struct {
	StartedAt int64 `json:"started_at"` // Unix timestamp in microseconds
}

// WriteInstanceInfo writes the current instance's start time to a coordination file
// This allows multiple instances to detect each other
func (sm *StateManager) WriteInstanceInfo(startedAt int64) error {
	info := InstanceInfo{
		StartedAt: startedAt,
	}

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal instance info: %w", err)
	}

	instancePath := sm.getInstancePath()

	if sm.useGCS {
		ctx := context.Background()
		writer := sm.gcsClient.Bucket(sm.gcsBucket).Object(instancePath).NewWriter(ctx)
		if _, err := writer.Write(data); err != nil {
			_ = writer.Close()
			return fmt.Errorf("failed to write instance info to GCS: %w", err)
		}
		if err := writer.Close(); err != nil {
			return fmt.Errorf("failed to close GCS writer: %w", err)
		}
	} else {
		filePath := strings.Replace(sm.stateFilePath, "_state.json", "_instance.json", 1)
		if err := os.WriteFile(filePath, data, 0600); err != nil {
			return fmt.Errorf("failed to write instance info file: %w", err)
		}
	}

	return nil
}

// ReadInstanceInfo reads the instance coordination file
func (sm *StateManager) ReadInstanceInfo() (*InstanceInfo, error) {
	instancePath := sm.getInstancePath()

	var data []byte
	var err error

	if sm.useGCS {
		ctx := context.Background()
		reader, err := sm.gcsClient.Bucket(sm.gcsBucket).Object(instancePath).NewReader(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to read instance info from GCS: %w", err)
		}
		defer func() {
			if closeErr := reader.Close(); closeErr != nil {
				sm.logger.Error("Failed to close GCS reader: %v", closeErr)
			}
		}()

		// Read all data from GCS reader
		buf := make([]byte, 1024) // Small buffer for instance info
		n, err := reader.Read(buf)
		if err != nil && err.Error() != "EOF" {
			return nil, fmt.Errorf("failed to read instance info data: %w", err)
		}
		data = buf[:n]
	} else {
		filePath := strings.Replace(sm.stateFilePath, "_state.json", "_instance.json", 1)
		data, err = os.ReadFile(filePath) // #nosec G304 - filePath is a controlled configuration value
		if err != nil {
			return nil, fmt.Errorf("failed to read instance info file: %w", err)
		}
	}

	var info InstanceInfo
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal instance info: %w", err)
	}

	return &info, nil
}

// getInstancePath returns the GCS object path for instance info (replaces _state.json with _instance.json)
func (sm *StateManager) getInstancePath() string {
	if sm.useGCS {
		return strings.Replace(sm.gcsObject, "_state.json", "_instance.json", 1)
	}
	return strings.Replace(sm.stateFilePath, "_state.json", "_instance.json", 1)
}
