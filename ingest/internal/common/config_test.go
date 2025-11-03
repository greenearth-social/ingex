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
	os.Setenv("ELASTICSEARCH_URL", "http://test.example.com:9200")
	os.Setenv("WEBSOCKET_WORKERS", "10")
	os.Setenv("ELASTICSEARCH_WORKERS", "15")
	os.Setenv("WORKER_TIMEOUT", "45s")
	os.Setenv("LOGGING_ENABLED", "false")
	os.Setenv("PORT", "3000")

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
	os.Setenv("WEBSOCKET_WORKERS", "invalid")
	os.Setenv("ELASTICSEARCH_WORKERS", "invalid")
	os.Setenv("WORKER_TIMEOUT", "invalid")
	os.Setenv("LOGGING_ENABLED", "invalid")

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

func clearEnvVars() {
	envVars := []string{
		"ELASTICSEARCH_URL",
		"WEBSOCKET_WORKERS",
		"ELASTICSEARCH_WORKERS",
		"WORKER_TIMEOUT",
		"LOGGING_ENABLED",
		"PORT",
	}

	for _, env := range envVars {
		os.Unsetenv(env)
	}
}
