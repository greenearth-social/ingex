package common

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type FileStatus string

const (
	FileStatusProcessed FileStatus = "processed"
	FileStatusFailed    FileStatus = "failed"
)

type FileStateEntry struct {
	Filename  string     `json:"filename"`
	Status    FileStatus `json:"status"`
	Timestamp time.Time  `json:"timestamp"`
	Error     string     `json:"error,omitempty"`
}

type StateManager struct {
	stateFilePath string
	mu            sync.RWMutex
	state         map[string]FileStateEntry
	logger        *IngestLogger
}

func NewStateManager(stateFilePath string, logger *IngestLogger) (*StateManager, error) {
	sm := &StateManager{
		stateFilePath: stateFilePath,
		state:         make(map[string]FileStateEntry),
		logger:        logger,
	}

	if err := sm.LoadState(); err != nil {
		return nil, err
	}

	return sm, nil
}

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

	var entries []FileStateEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return fmt.Errorf("failed to unmarshal state file: %w", err)
	}

	for _, entry := range entries {
		sm.state[entry.Filename] = entry
	}

	sm.logger.Info("Loaded state with %d entries", len(sm.state))
	return nil
}

func (sm *StateManager) SaveState() error {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	entries := make([]FileStateEntry, 0, len(sm.state))
	for _, entry := range sm.state {
		entries = append(entries, entry)
	}

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(sm.stateFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}

func (sm *StateManager) IsProcessed(filename string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	entry, exists := sm.state[filename]
	return exists && entry.Status == FileStatusProcessed
}

func (sm *StateManager) IsFailed(filename string) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	entry, exists := sm.state[filename]
	return exists && entry.Status == FileStatusFailed
}

func (sm *StateManager) MarkProcessed(filename string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.state[filename] = FileStateEntry{
		Filename:  filename,
		Status:    FileStatusProcessed,
		Timestamp: time.Now().UTC(),
	}

	if err := sm.saveStateUnsafe(); err != nil {
		return err
	}

	sm.logger.Info("Marked file as processed: %s", filename)
	return nil
}

func (sm *StateManager) MarkFailed(filename string, errMsg string) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.state[filename] = FileStateEntry{
		Filename:  filename,
		Status:    FileStatusFailed,
		Timestamp: time.Now().UTC(),
		Error:     errMsg,
	}

	if err := sm.saveStateUnsafe(); err != nil {
		return err
	}

	sm.logger.Error("Marked file as failed: %s - %s", filename, errMsg)
	return nil
}

func (sm *StateManager) saveStateUnsafe() error {
	entries := make([]FileStateEntry, 0, len(sm.state))
	for _, entry := range sm.state {
		entries = append(entries, entry)
	}

	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal state: %w", err)
	}

	if err := os.WriteFile(sm.stateFilePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write state file: %w", err)
	}

	return nil
}
