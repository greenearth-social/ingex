package common

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration values for the ingest service
type Config struct {
	// SQLite configuration
	SQLiteDBPath string

	// WebSocket configuration
	JetstreamURL string

	// Elasticsearch configuration
	ElasticsearchURL           string
	ElasticsearchAPIKey        string
	ElasticsearchTLSSkipVerify bool

	// Worker configuration (for future use)
	WebSocketWorkers     int
	ElasticsearchWorkers int
	WorkerTimeout        time.Duration

	// Spooler configuration
	LocalSQLiteDBPath   string
	S3SQLiteDBBucket    string
	S3SQLiteDBPrefix    string
	SpoolIntervalSec    int
	JetstreamStateFile  string
	MegastreamStateFile string
	AWSRegion           string
	AWSS3AccessKey      string
	AWSS3SecretKey      string

	// Logging configuration
	LoggingEnabled bool

	// Extract/Export configuration
	ParquetDestination string // Supports local paths (./output) or GCS paths (gs://bucket/path)
	ParquetMaxRecords  int64
	ExtractFetchSize   int
	ExtractIndices     string
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	return &Config{
		SQLiteDBPath:               getEnv("SQLITE_DB_PATH", ""),
		JetstreamURL:               getEnv("JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe"),
		WebSocketWorkers:           getEnvInt("WEBSOCKET_WORKERS", 3),
		ElasticsearchURL:           getEnv("ELASTICSEARCH_URL", ""),
		ElasticsearchAPIKey:        getEnv("ELASTICSEARCH_API_KEY", ""),
		ElasticsearchTLSSkipVerify: getEnvBool("ELASTICSEARCH_TLS_SKIP_VERIFY", false),
		ElasticsearchWorkers:       getEnvInt("ELASTICSEARCH_WORKERS", 5),
		WorkerTimeout:              getEnvDuration("WORKER_TIMEOUT", 30*time.Second),
		LocalSQLiteDBPath:          getEnv("LOCAL_SQLITE_DB_PATH", ""),
		S3SQLiteDBBucket:           getEnv("S3_SQLITE_DB_BUCKET", ""),
		S3SQLiteDBPrefix:           getEnv("S3_SQLITE_DB_PREFIX", ""),
		SpoolIntervalSec:           getEnvInt("SPOOL_INTERVAL_SEC", 60),
		JetstreamStateFile:         getEnv("JETSTREAM_STATE_FILE", ".jetstream_state.json"),
		MegastreamStateFile:        getEnv("MEGASTREAM_STATE_FILE", ".megastream_state.json"),
		AWSRegion:                  getEnv("AWS_REGION", "us-east-1"),
		AWSS3AccessKey:             getEnv("AWS_S3_ACCESS_KEY", ""),
		AWSS3SecretKey:             getEnv("AWS_S3_SECRET_KEY", ""),
		LoggingEnabled:             getEnvBool("LOGGING_ENABLED", true),
		ParquetDestination:         getEnv("PARQUET_DESTINATION", ""),
		ParquetMaxRecords:          int64(getEnvInt("PARQUET_MAX_RECORDS", 100000)),
		ExtractFetchSize:           getEnvInt("EXTRACT_FETCH_SIZE", 1000),
		ExtractIndices:             getEnv("EXTRACT_INDICES", "posts"),
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
