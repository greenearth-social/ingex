package common

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

var megastreamFilenameRegex = regexp.MustCompile(`^mega_jetstream_(\d{8})_(\d{6})\.db\.zip$`)

// ParseMegastreamFilenameTimestamp extracts a Unix timestamp in microseconds from a Megastream filename.
// Expected format: mega_jetstream_YYYYMMDD_HHMMSS.db.zip
func ParseMegastreamFilenameTimestamp(filename string) (int64, error) {
	base := filepath.Base(filename)
	matches := megastreamFilenameRegex.FindStringSubmatch(base)
	if matches == nil {
		return 0, fmt.Errorf("filename does not match expected format mega_jetstream_YYYYMMDD_hhmmss.db.zip: %s", base)
	}

	dateStr := matches[1]
	timeStr := matches[2]

	year, err := strconv.Atoi(dateStr[0:4])
	if err != nil {
		return 0, fmt.Errorf("failed to parse year: %w", err)
	}

	month, err := strconv.Atoi(dateStr[4:6])
	if err != nil {
		return 0, fmt.Errorf("failed to parse month: %w", err)
	}

	day, err := strconv.Atoi(dateStr[6:8])
	if err != nil {
		return 0, fmt.Errorf("failed to parse day: %w", err)
	}

	hour, err := strconv.Atoi(timeStr[0:2])
	if err != nil {
		return 0, fmt.Errorf("failed to parse hour: %w", err)
	}

	minute, err := strconv.Atoi(timeStr[2:4])
	if err != nil {
		return 0, fmt.Errorf("failed to parse minute: %w", err)
	}

	second, err := strconv.Atoi(timeStr[4:6])
	if err != nil {
		return 0, fmt.Errorf("failed to parse second: %w", err)
	}

	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	return t.UnixMicro(), nil
}

// TimestampToMegastreamFilename converts a Unix timestamp in microseconds to a Megastream filename format.
// Returns a string in the format: mega_jetstream_YYYYMMDD_HHMMSS.db.zip
func TimestampToMegastreamFilename(timeUs int64) string {
	t := time.UnixMicro(timeUs).UTC()
	return fmt.Sprintf("mega_jetstream_%s.db.zip", t.Format("20060102_150405"))
}
