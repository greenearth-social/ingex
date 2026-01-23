package common

import (
	"testing"
	"time"
)

func TestExtractHashtags(t *testing.T) {
	// Test time truncation
	testTime := time.Date(2026, 1, 9, 14, 35, 22, 0, time.UTC)
	expectedHour := testTime.Truncate(time.Hour).Format(time.RFC3339)

	tests := []struct {
		name      string
		content   string
		createdAt string
		wantTags  []string
	}{
		{
			name:      "single hashtag",
			content:   "This is a #test post",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"test"},
		},
		{
			name:      "multiple hashtags",
			content:   "Check out #golang and #programming",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"golang", "programming"},
		},
		{
			name:      "hashtag at end",
			content:   "Learning #AI",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"ai"},
		},
		{
			name:      "hashtag with numbers and underscores",
			content:   "Welcome to #Web3_0 and #AI2024",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"web3_0", "ai2024"},
		},
		{
			name:      "duplicate hashtags",
			content:   "I love #coding and #coding is fun",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"coding"},
		},
		{
			name:      "no hashtags",
			content:   "This is a regular post",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{},
		},
		{
			name:      "empty content",
			content:   "",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{},
		},
		{
			name:      "hashtag with punctuation",
			content:   "Hello #world! How are you?",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"world"},
		},
		{
			name:      "consecutive hashtags",
			content:   "#first#second #third",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"first", "second", "third"},
		},
		{
			name:      "case insensitive - different cases same tag",
			content:   "I love #AI and #ai and #Ai",
			createdAt: testTime.Format(time.RFC3339),
			wantTags:  []string{"ai"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			updates := ExtractHashtags(tt.content, tt.createdAt)

			if len(updates) != len(tt.wantTags) {
				t.Errorf("ExtractHashtags() got %d hashtags, want %d", len(updates), len(tt.wantTags))
			}

			// Check that all expected tags are present
			foundTags := make(map[string]bool)
			for _, update := range updates {
				foundTags[update.Hashtag] = true

				// Verify hour is correct
				if update.Hour != expectedHour {
					t.Errorf("ExtractHashtags() hour = %v, want %v", update.Hour, expectedHour)
				}

				// Verify count is 1
				if update.Count != 1 {
					t.Errorf("ExtractHashtags() count = %v, want 1", update.Count)
				}
			}

			for _, tag := range tt.wantTags {
				if !foundTags[tag] {
					t.Errorf("ExtractHashtags() missing expected tag: %s", tag)
				}
			}
		})
	}
}

func TestExtractHashtags_HourTruncation(t *testing.T) {
	// Test that different times in the same hour produce the same hour value
	baseTime := time.Date(2026, 1, 9, 14, 0, 0, 0, time.UTC)
	expectedHour := baseTime.Format(time.RFC3339)

	times := []time.Time{
		time.Date(2026, 1, 9, 14, 0, 0, 0, time.UTC),
		time.Date(2026, 1, 9, 14, 15, 30, 0, time.UTC),
		time.Date(2026, 1, 9, 14, 45, 59, 0, time.UTC),
		time.Date(2026, 1, 9, 14, 59, 59, 999999999, time.UTC),
	}

	for _, testTime := range times {
		t.Run(testTime.Format(time.RFC3339), func(t *testing.T) {
			updates := ExtractHashtags("Test #hashtag", testTime.Format(time.RFC3339))

			if len(updates) != 1 {
				t.Fatalf("Expected 1 update, got %d", len(updates))
			}

			if updates[0].Hour != expectedHour {
				t.Errorf("Hour = %v, want %v (input time: %v)", updates[0].Hour, expectedHour, testTime)
			}
		})
	}
}
