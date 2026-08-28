package common

import (
	"os"
	"strconv"
	"time"
)

// IndexPeriod controls how frequently a new time-based index is created.
// Valid values: "week", "hour", "10min".
const (
	IndexPeriodWeek  = "week"
	IndexPeriodHour  = "hour"
	IndexPeriod10Min = "10min"
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

	// Rate limiting / blocklist configuration
	BlocklistDestination       string // GE_BLOCKLIST_DESTINATION, e.g. gs://bucket/environment
	LikeRateLimitPerHour       int    // GE_LIKE_RATE_LIMIT_PER_HOUR, default 2000
	LikeRateLimitWindowMinutes int    // GE_LIKE_RATE_LIMIT_WINDOW_MIN, default 5
	LikeBlockDurationMinutes   int    // GE_LIKE_BLOCK_DURATION_MIN, default 60

	// Index period configuration
	IndexPeriod string // GE_INDEX_PERIOD: "week", "hour", or "10min"

	// Quality corpus configuration (greenearth-social/ingex#442). The quality
	// index is the lean two-week corpus two-tower kNN searches; posts join it
	// when they cross QualityLikeThreshold. Must stay in step with the api's
	// MIN_LIKE_COUNT and with posts_quality_ilm_delete_age in the ILM settings.
	QualityIndexEnabled  bool          // GE_QUALITY_INDEX_ENABLED, default true
	QualityLikeThreshold int           // GE_QUALITY_LIKE_THRESHOLD, default 10
	QualityRetentionAge  time.Duration // GE_QUALITY_RETENTION_AGE, default 336h (14d)

	// Inference service configuration
	InferenceBaseURL        string        // GE_INFERENCE_BASE_URL; empty disables post-tower embeddings
	InferenceAPIKey         string        // GE_INFERENCE_API_KEY
	InferenceTimeout        time.Duration // GE_INFERENCE_TIMEOUT, per-request HTTP timeout
	InferenceChunkSize      int           // GE_INFERENCE_CHUNK_SIZE, must be <= server GE_INFERENCE_MAX_BATCH
	InferenceMaxConcurrency int           // GE_INFERENCE_MAX_CONCURRENCY, concurrent inference requests
	InferenceRetryMax       int           // GE_INFERENCE_RETRY_MAX, retries beyond the first attempt

	// Perspective API configuration (api#368). Scoring posts at ingest keeps
	// the api off the serving path and builds the complete attribute-score
	// corpus we need to train a replacement before the API sunsets.
	//
	// The 36 000 requests/minute quota is shared with the api's serving path,
	// so PerspectiveQPS is ingest's *slice* of it, not the whole thing. The
	// default of 150 QPS is 9 000 RPM, against the api's 26 700
	// (GE_PERSPECTIVE_QPM there): 35 700 together, leaving a 300 RPM buffer.
	//
	// The buffer is deliberate. Neither limiter is exact — this one is a token
	// bucket in this process, the api's is a calendar-minute bucket in each of
	// several processes — so the true combined ceiling sits above the sum of
	// the two numbers. Raising either without lowering the other spends the
	// buffer.
	PerspectiveAPIKey         string        // GE_PERSPECTIVE_API_KEY; empty disables scoring entirely
	PerspectiveHost           string        // GE_PERSPECTIVE_HOST, overridable for the devenv stub
	PerspectiveQPS            int           // GE_PERSPECTIVE_QPS, this service's share of the shared quota
	PerspectiveOnQuota        string        // GE_PERSPECTIVE_ON_QUOTA, "wait" (throttle ingest) or "skip" (index unscored)
	PerspectiveTimeout        time.Duration // GE_PERSPECTIVE_TIMEOUT, per-request HTTP timeout
	PerspectiveMaxConcurrency int           // GE_PERSPECTIVE_MAX_CONCURRENCY, concurrent scoring requests
	PerspectiveRetryMax       int           // GE_PERSPECTIVE_RETRY_MAX, retries beyond the first attempt

	// Followed-users cache configuration (api#83). Jetstream is the only part
	// of the system that sees follow and unfollow events as they happen, so it
	// keeps the API's per-user follow cache current. Leaving FirestoreProject
	// empty disables the whole path; ingest keeps writing likes as before.
	FirestoreProject         string // GE_FIRESTORE_PROJECT; empty disables follow-delta writes
	FirestoreDatabase        string // GE_FIRESTORE_DATABASE, e.g. greenearth-prod
	FirestoreEmulatorHost    string // GE_FIRESTORE_EMULATOR_HOST, for devenv
	FollowsTrackedRefreshSec int    // GE_FOLLOWS_TRACKED_REFRESH_SEC, default 300
	FollowsWriteBuffer       int    // GE_FOLLOWS_WRITE_BUFFER, default 1024
}

// PerspectiveEnabled reports whether posts should be scored at ingest. An
// unset API key is the kill switch: no key, no scoring, and ingestion is
// otherwise unchanged.
func (c *Config) PerspectiveEnabled() bool {
	return c.PerspectiveAPIKey != ""
}

// FollowCacheEnabled reports whether follow deltas should be written.
func (c *Config) FollowCacheEnabled() bool {
	return c.FirestoreProject != ""
}

// LoadConfig loads configuration from environment variables with defaults
func LoadConfig() *Config {
	// The Google SDK reads FIRESTORE_EMULATOR_HOST natively; copy the
	// GE-prefixed variable into that standard name, as the API does.
	if host := getEnv("GE_FIRESTORE_EMULATOR_HOST", ""); host != "" {
		_ = os.Setenv("FIRESTORE_EMULATOR_HOST", host)
	}
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
		BlocklistDestination:       getEnv("GE_BLOCKLIST_DESTINATION", ""),
		LikeRateLimitPerHour:       getEnvInt("GE_LIKE_RATE_LIMIT_PER_HOUR", 2000),
		LikeRateLimitWindowMinutes: getEnvInt("GE_LIKE_RATE_LIMIT_WINDOW_MIN", 5),
		LikeBlockDurationMinutes:   getEnvInt("GE_LIKE_BLOCK_DURATION_MIN", 60),
		IndexPeriod:                getEnv("GE_INDEX_PERIOD", IndexPeriod10Min),
		QualityIndexEnabled:        getEnvBool("GE_QUALITY_INDEX_ENABLED", true),
		QualityLikeThreshold:       getEnvInt("GE_QUALITY_LIKE_THRESHOLD", 10),
		QualityRetentionAge:        getEnvDuration("GE_QUALITY_RETENTION_AGE", 14*24*time.Hour),
		InferenceBaseURL:           getEnv("GE_INFERENCE_BASE_URL", ""),
		InferenceAPIKey:            getEnv("GE_INFERENCE_API_KEY", ""),
		InferenceTimeout:           getEnvDuration("GE_INFERENCE_TIMEOUT", 10*time.Second),
		InferenceChunkSize:         getEnvInt("GE_INFERENCE_CHUNK_SIZE", 64),
		InferenceMaxConcurrency:    getEnvInt("GE_INFERENCE_MAX_CONCURRENCY", 8),
		InferenceRetryMax:          getEnvInt("GE_INFERENCE_RETRY_MAX", 3),
		PerspectiveAPIKey:          getEnv("GE_PERSPECTIVE_API_KEY", ""),
		PerspectiveHost:            getEnv("GE_PERSPECTIVE_HOST", ""),
		PerspectiveQPS:             getEnvInt("GE_PERSPECTIVE_QPS", 150),
		PerspectiveOnQuota:         getEnv("GE_PERSPECTIVE_ON_QUOTA", "wait"),
		PerspectiveTimeout:         getEnvDuration("GE_PERSPECTIVE_TIMEOUT", 2*time.Second),
		PerspectiveMaxConcurrency:  getEnvInt("GE_PERSPECTIVE_MAX_CONCURRENCY", 32),
		PerspectiveRetryMax:        getEnvInt("GE_PERSPECTIVE_RETRY_MAX", 2),
		FirestoreProject:           getEnv("GE_FIRESTORE_PROJECT", ""),
		FirestoreDatabase:          getEnv("GE_FIRESTORE_DATABASE", ""),
		FirestoreEmulatorHost:      getEnv("GE_FIRESTORE_EMULATOR_HOST", ""),
		FollowsTrackedRefreshSec:   getEnvInt("GE_FOLLOWS_TRACKED_REFRESH_SEC", 300),
		FollowsWriteBuffer:         getEnvInt("GE_FOLLOWS_WRITE_BUFFER", 1024),
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
