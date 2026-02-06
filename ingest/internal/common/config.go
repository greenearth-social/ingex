package common

import (
	"os"
	"strconv"
	"time"
)

// Config holds all configuration values for the ingest service
type Config struct {
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

	// Metric configuration
	MetricExportIntervalSec int

	// GCP configuration
	GCPProjectID string
	GCPRegion    string
	Environment  string

	// Extract/Export configuration
	ParquetDestination string // Supports local paths (./output) or GCS paths (gs://bucket/path)
	ParquetMaxRecords  int64
	ExtractFetchSize   int
	ExtractIndices     string

	// Cache configuration
	PostRoutingCacheSize int
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	return &Config{
		JetstreamURL:               getEnv("GE_JETSTREAM_URL", "wss://jetstream2.us-east.bsky.network/subscribe"),
		WebSocketWorkers:           getEnvInt("GE_WEBSOCKET_WORKERS", 3),
		ElasticsearchURL:           getEnv("GE_ELASTICSEARCH_URL", ""),
		ElasticsearchAPIKey:        getEnv("GE_ELASTICSEARCH_API_KEY", ""),
		ElasticsearchTLSSkipVerify: getEnvBool("GE_ELASTICSEARCH_TLS_SKIP_VERIFY", false),
		ElasticsearchWorkers:       getEnvInt("GE_ELASTICSEARCH_WORKERS", 5),
		WorkerTimeout:              getEnvDuration("GE_WORKER_TIMEOUT", 30*time.Second),
		LocalSQLiteDBPath:          getEnv("GE_LOCAL_SQLITE_DB_PATH", ""),
		S3SQLiteDBBucket:           getEnv("GE_AWS_S3_BUCKET", ""),
		S3SQLiteDBPrefix:           getEnv("GE_AWS_S3_PREFIX", ""),
		SpoolIntervalSec:           getEnvInt("GE_SPOOL_INTERVAL_SEC", 60),
		JetstreamStateFile:         getEnv("GE_JETSTREAM_STATE_FILE", ".jetstream_state.json"),
		MegastreamStateFile:        getEnv("GE_MEGASTREAM_STATE_FILE", ".megastream_state.json"),
		AWSRegion:                  getEnv("GE_AWS_REGION", "us-east-1"),
		AWSS3AccessKey:             getEnv("GE_AWS_S3_ACCESS_KEY", ""),
		AWSS3SecretKey:             getEnv("GE_AWS_S3_SECRET_KEY", ""),
		LoggingEnabled:             getEnvBool("GE_LOGGING_ENABLED", true),
		MetricExportIntervalSec:    getEnvInt("GE_METRIC_EXPORT_INTERVAL_SEC", 60),
		GCPProjectID:               getEnv("GE_GCP_PROJECT_ID", ""),
		GCPRegion:                  getEnv("GE_GCP_REGION", "us-east1"),
		Environment:                getEnv("GE_ENVIRONMENT", "local"),
		ParquetDestination:         getEnv("GE_PARQUET_DESTINATION", ""),
		ParquetMaxRecords:          int64(getEnvInt("GE_PARQUET_MAX_RECORDS", 100000)),
		ExtractFetchSize:           getEnvInt("GE_EXTRACT_FETCH_SIZE", 1000),
		ExtractIndices:             getEnv("GE_EXTRACT_INDICES", "posts"),
		PostRoutingCacheSize:       getEnvInt("GE_POST_ROUTING_CACHE_SIZE", 500000),
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
