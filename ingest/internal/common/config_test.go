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

func TestLoadConfig_MetricExportIntervalSec_Default(t *testing.T) {
	clearEnvVars()
	config := LoadConfig()

	if config.MetricExportIntervalSec != 60 {
		t.Errorf("Expected default MetricExportIntervalSec 60, got %d", config.MetricExportIntervalSec)
	}
}

func TestLoadConfig_MetricExportIntervalSec_EnvOverride(t *testing.T) {
	clearEnvVars()
	setEnvForTest(t, "GE_METRIC_EXPORT_INTERVAL_SEC", "30")
	defer clearEnvVars()

	config := LoadConfig()

	if config.MetricExportIntervalSec != 30 {
		t.Errorf("Expected MetricExportIntervalSec 30, got %d", config.MetricExportIntervalSec)
	}
}

func TestLoadConfig_GCPFields(t *testing.T) {
	clearEnvVars()

	config := LoadConfig()
	if config.GCPProjectID != "" {
		t.Errorf("Expected default GCPProjectID empty, got %s", config.GCPProjectID)
	}
	if config.GCPRegion != "us-east1" {
		t.Errorf("Expected default GCPRegion us-east1, got %s", config.GCPRegion)
	}
	if config.Environment != "local" {
		t.Errorf("Expected default Environment local, got %s", config.Environment)
	}

	setEnvForTest(t, "GE_GCP_PROJECT_ID", "my-project")
	setEnvForTest(t, "GE_GCP_REGION", "us-west1")
	setEnvForTest(t, "GE_ENVIRONMENT", "prod")
	defer clearEnvVars()

	config = LoadConfig()
	if config.GCPProjectID != "my-project" {
		t.Errorf("Expected GCPProjectID my-project, got %s", config.GCPProjectID)
	}
	if config.GCPRegion != "us-west1" {
		t.Errorf("Expected GCPRegion us-west1, got %s", config.GCPRegion)
	}
	if config.Environment != "prod" {
		t.Errorf("Expected Environment prod, got %s", config.Environment)
	}
}

func clearEnvVars() {
	envVars := []string{
		"GE_ELASTICSEARCH_URL",
		"GE_WEBSOCKET_WORKERS",
		"GE_ELASTICSEARCH_WORKERS",
		"GE_WORKER_TIMEOUT",
		"GE_LOGGING_ENABLED",
		"GE_METRIC_EXPORT_INTERVAL_SEC",
		"GE_GCP_PROJECT_ID",
		"GE_GCP_REGION",
		"GE_ENVIRONMENT",
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
