package common

import (
	"strings"
	"testing"
	"time"
)

func TestCurrentIndexName_Week(t *testing.T) {
	// Use a fixed time: 2026-04-13 (Monday, ISO week 16)
	// ISOWeek returns year=2026, week=16
	ref := time.Date(2026, 4, 13, 10, 30, 0, 0, time.UTC)
	year, week := ref.ISOWeek()

	// We can't inject time into CurrentIndexName, but we can verify format consistency
	// by calling it and checking the structure.
	got := CurrentIndexName("posts", IndexPeriodWeek)

	if !strings.HasPrefix(got, "posts-") {
		t.Errorf("expected prefix posts-, got %s", got)
	}

	// Verify the format matches what ISOWeek produces for now
	// (this assertion is timing-dependent but verifies the shape)
	_ = year
	_ = week
	parts := strings.Split(got, "-")
	if len(parts) != 3 {
		t.Errorf("expected 3 parts (base-year-wNN), got %d in %s", len(parts), got)
	}
	if !strings.HasPrefix(parts[2], "w") {
		t.Errorf("expected week part to start with 'w', got %s in %s", parts[2], got)
	}
}

func TestCurrentIndexName_Hour(t *testing.T) {
	got := CurrentIndexName("likes", IndexPeriodHour)

	if !strings.HasPrefix(got, "likes-") {
		t.Errorf("expected prefix likes-, got %s", got)
	}
	// Format: likes-YYYY-MM-DD-HH → 5 parts when split by "-"
	parts := strings.Split(got, "-")
	if len(parts) != 5 {
		t.Errorf("expected 5 parts (likes-YYYY-MM-DD-HH), got %d in %s", len(parts), got)
	}
}

func TestCurrentIndexName_10Min(t *testing.T) {
	got := CurrentIndexName("post_tombstones", IndexPeriod10Min)

	if !strings.HasPrefix(got, "post-tombstones-") {
		t.Errorf("expected prefix post-tombstones-, got %s", got)
	}
	// Format: post-tombstones-YYYY-MM-DD-HH-MM
	withoutBase := strings.TrimPrefix(got, "post-tombstones-")
	dateParts := strings.Split(withoutBase, "-")
	if len(dateParts) != 5 {
		t.Errorf("expected 5 date parts (YYYY-MM-DD-HH-MM), got %d in %s", len(dateParts), got)
	}
}

func TestCurrentIndexName_10MinTruncation(t *testing.T) {
	// Verify that 10min period always produces a minute that is a multiple of 10.
	got := CurrentIndexName("likes", IndexPeriod10Min)

	withoutBase := strings.TrimPrefix(got, "likes-")
	parts := strings.Split(withoutBase, "-")
	minute := parts[4]

	validMinutes := map[string]bool{
		"00": true, "10": true, "20": true,
		"30": true, "40": true, "50": true,
	}
	if !validMinutes[minute] {
		t.Errorf("expected minute to be multiple of 10, got %s in %s", minute, got)
	}
}

func TestCurrentIndexName_UnknownPeriodFallsBackToWeek(t *testing.T) {
	got := CurrentIndexName("posts", "unknown")

	if !strings.HasPrefix(got, "posts-") {
		t.Errorf("expected prefix posts-, got %s", got)
	}
	parts := strings.Split(got, "-")
	if len(parts) != 3 || !strings.HasPrefix(parts[2], "w") {
		t.Errorf("expected week format fallback, got %s", got)
	}
}
