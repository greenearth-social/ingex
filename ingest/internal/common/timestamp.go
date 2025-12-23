package common

import (
	"time"
)

// NormalizeTimestampToUTC parses an RFC3339/ISO 8601 timestamp string and
// returns it normalized to UTC in RFC3339 format.
// Returns empty string and logs error if parsing fails.
func NormalizeTimestampToUTC(timestamp string, logger *IngestLogger) string {
	if timestamp == "" {
		return ""
	}

	// Parse the timestamp (handles RFC3339 with timezone offsets)
	parsedTime, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		// Try with nanoseconds variant (ISO 8601 with more precision)
		parsedTime, err = time.Parse(time.RFC3339Nano, timestamp)
		if err != nil {
			logger.Error("Failed to parse timestamp '%s': %v", timestamp, err)
			return ""
		}
	}

	// Convert to UTC and format back to RFC3339
	return parsedTime.UTC().Format(time.RFC3339)
}
