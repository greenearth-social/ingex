package main

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration values for the ingest service
type Config struct {
	// SQLite configuration
	SQLiteDBPath string

	// WebSocket configuration (for future use)
	TurboStreamURL string

	// Elasticsearch configuration
	ElasticsearchURL    string
	ElasticsearchAPIKey string

	// Worker configuration (for future use)
	WebSocketWorkers     int
	ElasticsearchWorkers int
	WorkerTimeout        time.Duration

	// Spooler configuration
	LocalSQLiteDBPath string
	S3SQLiteDBBucket  string
	S3SQLiteDBPrefix  string
	SpoolIntervalSec  int
	SpoolStateFile    string
	AWSRegion         string

	// Logging configuration
	LoggingEnabled bool
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	return &Config{
		SQLiteDBPath:         getEnv("SQLITE_DB_PATH", ""),
		TurboStreamURL:       getEnv("TURBOSTREAM_URL", ""),
		WebSocketWorkers:     getEnvInt("WEBSOCKET_WORKERS", 3),
		ElasticsearchURL:     getEnv("ELASTICSEARCH_URL", ""),
		ElasticsearchAPIKey:  getEnv("ELASTICSEARCH_API_KEY", ""),
		ElasticsearchWorkers: getEnvInt("ELASTICSEARCH_WORKERS", 5),
		WorkerTimeout:        getEnvDuration("WORKER_TIMEOUT", 30*time.Second),
		LocalSQLiteDBPath:    getEnv("LOCAL_SQLITE_DB_PATH", ""),
		S3SQLiteDBBucket:     getEnv("S3_SQLITE_DB_BUCKET", ""),
		S3SQLiteDBPrefix:     getEnv("S3_SQLITE_DB_PREFIX", ""),
		SpoolIntervalSec:     getEnvInt("SPOOL_INTERVAL_SEC", 60),
		SpoolStateFile:       getEnv("SPOOL_STATE_FILE", ".processed_files.json"),
		AWSRegion:            getEnv("AWS_REGION", "us-east-1"),
		LoggingEnabled:       getEnvBool("LOGGING_ENABLED", true),
	}
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvInt returns the integer value of an environment variable or a default value
func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

// getEnvBool returns the boolean value of an environment variable or a default value
func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

// getEnvDuration returns the duration value of an environment variable or a default value
func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}