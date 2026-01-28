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
	logger.SetDebugEnabled(true) // Enable debug logging
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

func TestLoggerDebugDisabledByDefault(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)

	logger.Info("info message")
	logger.Debug("debug message")

	output := buf.String()

	if !strings.Contains(output, "[INFO]") {
		t.Error("Expected [INFO] in output")
	}
	if strings.Contains(output, "[DEBUG]") {
		t.Error("Expected no [DEBUG] in output when debug is not enabled")
	}
	if strings.Contains(output, "debug message") {
		t.Error("Expected no debug message content when debug is not enabled")
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

func TestMetricDisabledLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(false)
	logger.SetOutput(&buf)
	logger.SetMetricCollector(NewInMemoryMetricCollector(), 1.0)

	logger.Metric("test.metric", 42.0)

	output := buf.String()
	if output != "" {
		t.Errorf("Expected no output when logger disabled, got: %s", output)
	}
}

func TestMetricNoCollector(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)

	// Should not panic when no collector is set
	logger.Metric("test.metric", 42.0)

	output := buf.String()
	if output != "" {
		t.Errorf("Expected no output when no collector set, got: %s", output)
	}
}

func TestMetricFullSamplingRatio(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)
	logger.SetMetricCollector(NewInMemoryMetricCollector(), 1.0)

	logger.Metric("test.metric", 42.0)
	logger.Metric("test.metric", 50.0)

	output := buf.String()
	if !strings.Contains(output, "[METRIC]") {
		t.Error("Expected [METRIC] in output with 1.0 sampling ratio")
	}
	if strings.Count(output, "[METRIC]") != 2 {
		t.Errorf("Expected 2 metric log lines with 1.0 sampling, got %d", strings.Count(output, "[METRIC]"))
	}
}

func TestMetricLowSamplingRatio(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)
	logger.SetMetricCollector(NewInMemoryMetricCollector(), 0.01)

	// Record 100 values — with 0.01 ratio, should log on every 100th observation
	for i := 0; i < 100; i++ {
		logger.Metric("test.metric", float64(i))
	}

	output := buf.String()
	metricLines := strings.Count(output, "[METRIC]")
	if metricLines != 1 {
		t.Errorf("Expected 1 metric log line with 0.01 sampling over 100 records, got %d", metricLines)
	}
}

func TestMetricOutputFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)
	logger.SetMetricCollector(NewInMemoryMetricCollector(), 1.0)

	logger.Metric("es.bulk_index.duration_ms", 150.0)

	output := buf.String()
	if !strings.Contains(output, "es.bulk_index.duration_ms=150.00") {
		t.Errorf("Expected metric name and value in output, got: %s", output)
	}
	if !strings.Contains(output, "count=1") {
		t.Errorf("Expected count in output, got: %s", output)
	}
	if !strings.Contains(output, "avg=150.00") {
		t.Errorf("Expected avg in output, got: %s", output)
	}
	if !strings.Contains(output, "min=150.00") {
		t.Errorf("Expected min in output, got: %s", output)
	}
	if !strings.Contains(output, "max=150.00") {
		t.Errorf("Expected max in output, got: %s", output)
	}
}

func TestMetricZeroSamplingRatio(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(true)
	logger.SetOutput(&buf)
	mc := NewInMemoryMetricCollector()
	logger.SetMetricCollector(mc, 0.0)

	logger.Metric("test.metric", 42.0)

	output := buf.String()
	if output != "" {
		t.Errorf("Expected no output with 0.0 sampling ratio, got: %s", output)
	}

	// But the collector should still record the value
	summary := mc.Summary("test.metric")
	if summary == nil {
		t.Fatal("Expected collector to still record values with 0.0 sampling")
	}
	if summary.Count != 1 {
		t.Errorf("Expected count 1, got %d", summary.Count)
	}
}
