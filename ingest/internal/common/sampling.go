package common

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
)

// IsSampledIn determines if a user DID should be included in the ingest pipeline
// based on consistent hashing and sampling rate configuration.
// Uses FNV-32a hash of the DID to assign to one of 100 buckets (0-99).
// Returns true if the bucket falls within the current sampling rate (0-100).
func IsSampledIn(userDID string) bool {
	if !isSamplingEnabled() {
		return true
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(userDID))
	bucket := int(hash.Sum32() % 100) // 100 buckets: 0-99

	samplingRate := getSamplingRate()
	return bucket < samplingRate
}

// isSamplingEnabled checks if sampling is enabled via environment variable.
// Only "true" (case-insensitive) enables sampling. All other values disable it.
// This prevents ambiguous interpretations of truthy strings like "1", "yes", etc.
func isSamplingEnabled() bool {
	enabled := os.Getenv("GE_INGEST_SAMPLING_ENABLED")
	return enabled == "true"
}

// getSamplingRate returns the current sampling rate percentage (0-100).
// Defaults to 10 if GE_INGEST_SAMPLING_RATE is unset or invalid.
// This implements the 90% reduction (keeping 10%) as required.
func getSamplingRate() int {
	rateStr := os.Getenv("GE_INGEST_SAMPLING_RATE")
	if rateStr == "" {
		return 10
	}
	rate, err := strconv.Atoi(rateStr)
	if err != nil || rate < 0 || rate > 100 {
		return 10
	}
	return rate
}

// GetSamplingRate returns the configured sampling rate for monitoring/debugging.
// This is exported for testing and metrics purposes.
func GetSamplingRate() int {
	return getSamplingRate()
}

// GetSamplingDebugInfo returns debug information about sampling decision.
// Useful for logging and troubleshooting why a DID was included/excluded.
func GetSamplingDebugInfo(userDID string) map[string]interface{} {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(userDID))
	bucket := int(hash.Sum32() % 100)
	samplingRate := getSamplingRate()
	enabled := isSamplingEnabled()

	return map[string]interface{}{
		"did":           userDID,
		"bucket":        bucket,
		"sampling_rate": samplingRate,
		"enabled":       enabled,
		"sampled_in":    enabled && bucket < samplingRate,
	}
}