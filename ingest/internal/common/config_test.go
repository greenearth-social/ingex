package common

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig_Defaults(t *testing.T) {
	// Clear any existing environment variables
	clearEnvVars()

	config := LoadConfig()

	if config.WebSocketWorkers != 3 {
		t.Errorf("Expected default WebSocketWorkers to be 3, got %d", config.WebSocketWorkers)
	}

	if config.ElasticsearchWorkers != 5 {
		t.Errorf("Expected default ElasticsearchWorkers to be 5, got %d", config.ElasticsearchWorkers)
	}

	if config.WorkerTimeout != 30*time.Second {
		t.Errorf("Expected default WorkerTimeout to be 30s, got %v", config.WorkerTimeout)
	}

	if !config.LoggingEnabled {
		t.Error("Expected default LoggingEnabled to be true")
	}
}

func TestLoadConfig_FromEnvironment(t *testing.T) {
	// Set environment variables
	setEnvForTest(t, "GE_ELASTICSEARCH_URL", "http://test.example.com:9200")
	setEnvForTest(t, "GE_WEBSOCKET_WORKERS", "10")
	setEnvForTest(t, "GE_ELASTICSEARCH_WORKERS", "15")
	setEnvForTest(t, "GE_WORKER_TIMEOUT", "45s")
	setEnvForTest(t, "GE_LOGGING_ENABLED", "false")
	setEnvForTest(t, "PORT", "3000")

	defer clearEnvVars()

	config := LoadConfig()

	if config.ElasticsearchURL != "http://test.example.com:9200" {
		t.Errorf("Expected ElasticsearchURL from env, got %s", config.ElasticsearchURL)
	}

	if config.WebSocketWorkers != 10 {
		t.Errorf("Expected WebSocketWorkers from env to be 10, got %d", config.WebSocketWorkers)
	}

	if config.ElasticsearchWorkers != 15 {
		t.Errorf("Expected ElasticsearchWorkers from env to be 15, got %d", config.ElasticsearchWorkers)
	}

	if config.WorkerTimeout != 45*time.Second {
		t.Errorf("Expected WorkerTimeout from env to be 45s, got %v", config.WorkerTimeout)
	}

	if config.LoggingEnabled {
		t.Error("Expected LoggingEnabled from env to be false")
	}
}

func TestLoadConfig_InvalidValues(t *testing.T) {
	// Set invalid environment variables that should fall back to defaults
	setEnvForTest(t, "GE_WEBSOCKET_WORKERS", "invalid")
	setEnvForTest(t, "GE_ELASTICSEARCH_WORKERS", "invalid")
	setEnvForTest(t, "GE_WORKER_TIMEOUT", "invalid")
	setEnvForTest(t, "GE_LOGGING_ENABLED", "invalid")

	defer clearEnvVars()

	config := LoadConfig()

	// Should fall back to defaults for invalid values
	if config.WebSocketWorkers != 3 {
		t.Errorf("Expected default WebSocketWorkers for invalid value, got %d", config.WebSocketWorkers)
	}

	if config.ElasticsearchWorkers != 5 {
		t.Errorf("Expected default ElasticsearchWorkers for invalid value, got %d", config.ElasticsearchWorkers)
	}

	if config.WorkerTimeout != 30*time.Second {
		t.Errorf("Expected default WorkerTimeout for invalid value, got %v", config.WorkerTimeout)
	}

	if !config.LoggingEnabled {
		t.Error("Expected default LoggingEnabled for invalid value")
	}
}

func TestLoadConfig_MetricSamplingRatio_Default(t *testing.T) {
	clearEnvVars()
	config := LoadConfig()

	if config.MetricSamplingRatio != 0.01 {
		t.Errorf("Expected default MetricSamplingRatio 0.01, got %f", config.MetricSamplingRatio)
	}
}

func TestLoadConfig_MetricSamplingRatio_EnvOverride(t *testing.T) {
	clearEnvVars()
	setEnvForTest(t, "GE_METRIC_SAMPLING_RATIO", "0.5")
	defer clearEnvVars()

	config := LoadConfig()

	if config.MetricSamplingRatio != 0.5 {
		t.Errorf("Expected MetricSamplingRatio 0.5, got %f", config.MetricSamplingRatio)
	}
}

func TestLoadConfig_MetricSamplingRatio_InvalidFallback(t *testing.T) {
	clearEnvVars()
	setEnvForTest(t, "GE_METRIC_SAMPLING_RATIO", "invalid")
	defer clearEnvVars()

	config := LoadConfig()

	if config.MetricSamplingRatio != 0.01 {
		t.Errorf("Expected default MetricSamplingRatio 0.01 for invalid value, got %f", config.MetricSamplingRatio)
	}
}

func clearEnvVars() {
	envVars := []string{
		"GE_ELASTICSEARCH_URL",
		"GE_WEBSOCKET_WORKERS",
		"GE_ELASTICSEARCH_WORKERS",
		"GE_WORKER_TIMEOUT",
		"GE_LOGGING_ENABLED",
		"GE_METRIC_SAMPLING_RATIO",
		"PORT",
	}

	for _, env := range envVars {
		_ = os.Unsetenv(env) // Ignore errors in test cleanup
	}
}

func setEnvForTest(t *testing.T, key, value string) {
	t.Helper()
	if err := os.Setenv(key, value); err != nil {
		t.Fatalf("Failed to set environment variable %s: %v", key, err)
	}
}
