package common

import (
	"bytes"
	"strings"
	"testing"
)

func TestNewLogger(t *testing.T) {
	logger := NewLogger(true)
	if logger == nil {
		t.Fatal("Expected logger to be created, got nil")
	}

	if !logger.enabled {
		t.Error("Expected logger to be enabled")
	}
}

func TestLoggerEnabled(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)

	logger.Info("test info message")
	output := buf.String()

	if !strings.Contains(output, "[INFO]") {
		t.Error("Expected [INFO] in output")
	}
	if !strings.Contains(output, "test info message") {
		t.Error("Expected message in output")
	}
}

func TestLoggerDisabled(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(false)
	logger.SetOutput(&buf)

	logger.Info("test info message")
	logger.Error("test error message")
	logger.Debug("test debug message")

	output := buf.String()
	if output != "" {
		t.Errorf("Expected no output when disabled, got: %s", output)
	}
}

func TestLoggerLevels(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)

	logger.Info("info message")
	logger.Error("error message")
	logger.Debug("debug message")

	output := buf.String()

	if !strings.Contains(output, "[INFO]") {
		t.Error("Expected [INFO] in output")
	}
	if !strings.Contains(output, "[ERROR]") {
		t.Error("Expected [ERROR] in output")
	}
	if !strings.Contains(output, "[DEBUG]") {
		t.Error("Expected [DEBUG] in output")
	}
}

func TestLoggerFormatting(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)

	logger.Info("message with %s and %d", "string", 42)
	output := buf.String()

	if !strings.Contains(output, "message with string and 42") {
		t.Error("Expected formatted message in output")
	}
}
