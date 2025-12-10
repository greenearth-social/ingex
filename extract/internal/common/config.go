package common

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration values for the export service
type Config struct {
	// Elasticsearch configuration
	ElasticsearchURL    string
	ElasticsearchAPIKey string

	// Parquet file configuration
	ParquetFilePath string

	// TODO: S3 configuration

	// Logging configuration
	LoggingEnabled bool
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	return &Config{
		ElasticsearchURL:    getEnv("ELASTICSEARCH_URL", ""),
		ElasticsearchAPIKey: getEnv("ELASTICSEARCH_API_KEY", ""),
		ParquetFilePath:     getEnv("PARQUET_FILE_PATH", "../"),
		LoggingEnabled:      getEnvBool("LOGGING_ENABLED", true),
	}
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// TODO:  some of this maybe should move to ingex shared level?

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
