package common

import (
	"bytes"
	"strings"
	"sync"
	"testing"
)

// mockMetricCollector is a simple test double for MetricCollector
type mockMetricCollector struct {
	mu      sync.Mutex
	records map[string][]float64
}

func newMockMetricCollector() *mockMetricCollector {
	return &mockMetricCollector{
		records: make(map[string][]float64),
	}
}

func (m *mockMetricCollector) Record(name string, value float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records[name] = append(m.records[name], value)
}

func (m *mockMetricCollector) getRecords(name string) []float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.records[name]
}

func TestNewLogger(t *testing.T) {
	logger := NewLogger(true)
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
	logger.SetDebugEnabled(true)
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
	logger := NewLogger(false)
	mc := newMockMetricCollector()
	logger.SetMetricCollector(mc)

	logger.Metric("test.metric", 42.0)

	if records := mc.getRecords("test.metric"); len(records) != 0 {
		t.Errorf("Expected no records when logger disabled, got %v", records)
	}
}

func TestMetricNoCollector(t *testing.T) {
	logger := NewLogger(true)
	// Should not panic when no collector is set
	logger.Metric("test.metric", 42.0)
}

func TestMetricDelegatesToCollector(t *testing.T) {
	logger := NewLogger(true)
	mc := newMockMetricCollector()
	logger.SetMetricCollector(mc)

	logger.Metric("test.metric", 42.0)
	logger.Metric("test.metric", 50.0)

	records := mc.getRecords("test.metric")
	if len(records) != 2 {
		t.Fatalf("Expected 2 records, got %d", len(records))
	}
	if records[0] != 42.0 {
		t.Errorf("Expected first record 42.0, got %f", records[0])
	}
	if records[1] != 50.0 {
		t.Errorf("Expected second record 50.0, got %f", records[1])
	}
}

func TestMetricConcurrentAccess(t *testing.T) {
	logger := NewLogger(true)
	mc := newMockMetricCollector()
	logger.SetMetricCollector(mc)

	var wg sync.WaitGroup
	numGoroutines := 10
	numIterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				logger.Metric("concurrent.metric", float64(j))
			}
		}()
	}

	wg.Wait()

	records := mc.getRecords("concurrent.metric")
	expectedCount := numGoroutines * numIterations
	if len(records) != expectedCount {
		t.Errorf("Expected count %d, got %d", expectedCount, len(records))
	}
}
