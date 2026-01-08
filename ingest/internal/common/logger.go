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
	gitSHA       string
}

// NewLogger creates a new logger with configurable output destinations
func NewLogger(enabled bool) *IngestLogger {
	gitSHA := os.Getenv("GE_GIT_SHA")
	var prefix string
	if gitSHA != "" {
		prefix = "[" + gitSHA + "] "
	}

	return &IngestLogger{
		infoLogger:   log.New(os.Stdout, prefix+"[INFO] ", 0),
		errorLogger:  log.New(os.Stderr, prefix+"[ERROR] ", 0),
		debugLogger:  log.New(os.Stdout, prefix+"[DEBUG] ", 0),
		enabled:      enabled,
		debugEnabled: false,
		gitSHA:       gitSHA,
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
