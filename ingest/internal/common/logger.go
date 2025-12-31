package common

import (
	"io"
	"log"
	"os"
)

// IngestLogger implements the Logger interface with configurable output
type IngestLogger struct {
	infoLogger   *log.Logger
	errorLogger  *log.Logger
	debugLogger  *log.Logger
	enabled      bool
	debugEnabled bool
}

// NewLogger creates a new logger with configurable output destinations
func NewLogger(enabled bool) *IngestLogger {
	return &IngestLogger{
		infoLogger:   log.New(os.Stdout, "[INFO] ", log.LstdFlags),
		errorLogger:  log.New(os.Stderr, "[ERROR] ", log.LstdFlags),
		debugLogger:  log.New(os.Stdout, "[DEBUG] ", log.LstdFlags),
		enabled:      enabled,
		debugEnabled: false,
	}
}

// Info logs an informational message
func (l *IngestLogger) Info(msg string, args ...interface{}) {
	if !l.enabled {
		return
	}
	l.infoLogger.Printf(msg, args...)
}

// Error logs an error message
func (l *IngestLogger) Error(msg string, args ...interface{}) {
	if !l.enabled {
		return
	}
	l.errorLogger.Printf(msg, args...)
}

// Debug logs a debug message
func (l *IngestLogger) Debug(msg string, args ...interface{}) {
	if !l.enabled || !l.debugEnabled {
		return
	}
	l.debugLogger.Printf(msg, args...)
}

// SetDebugEnabled enables or disables debug logging
func (l *IngestLogger) SetDebugEnabled(enabled bool) {
	l.debugEnabled = enabled
}

// SetOutput sets the output destination for all loggers
func (l *IngestLogger) SetOutput(w io.Writer) {
	l.infoLogger.SetOutput(w)
	l.errorLogger.SetOutput(w)
	l.debugLogger.SetOutput(w)
}
