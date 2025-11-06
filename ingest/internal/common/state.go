package common

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

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
}

// NewStateManager creates a new state manager with the given state file path
func NewStateManager(stateFilePath string, logger *IngestLogger) (*StateManager, error) {
	sm := &StateManager{
		stateFilePath: stateFilePath,
		logger:        logger,
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

	if _, err := os.Stat(sm.stateFilePath); os.IsNotExist(err) {
		sm.logger.Info("State file does not exist, starting with empty state")
		return nil
	}

	data, err := os.ReadFile(sm.stateFilePath)
	if err != nil {
		return fmt.Errorf("failed to read state file: %w", err)
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

	if err := os.WriteFile(sm.stateFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}
