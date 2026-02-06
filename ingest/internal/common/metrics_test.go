package common

import (
	"testing"
	"time"
)

func TestCalculateFreshness(t *testing.T) {
	tests := []struct {
		name     string
		timeUs   int64
		expected int64
	}{
		{
			name:     "zero timestamp returns zero",
			timeUs:   0,
			expected: 0,
		},
		{
			name:   "recent timestamp returns small freshness",
			timeUs: time.Now().Add(-5 * time.Second).UnixMicro(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateFreshness(tt.timeUs)
			if tt.timeUs == 0 {
				if result != 0 {
					t.Errorf("Expected 0 for zero timestamp, got %d", result)
				}
			} else {
				if result < 4 || result > 6 {
					t.Errorf("Expected freshness ~5 seconds, got %d", result)
				}
			}
		})
	}
}
