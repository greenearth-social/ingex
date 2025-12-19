package common

import (
	"testing"
)

func TestNormalizeTimestampToUTC(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name     string
		input    string
		expected string
		isError  bool
	}{
		{
			name:     "already UTC with Z suffix",
			input:    "2025-01-27T12:34:56Z",
			expected: "2025-01-27T12:34:56Z",
			isError:  false,
		},
		{
			name:     "timezone offset +05:00 converts to UTC",
			input:    "2025-01-27T17:34:56+05:00",
			expected: "2025-01-27T12:34:56Z",
			isError:  false,
		},
		{
			name:     "timezone offset -08:00 converts to UTC",
			input:    "2025-01-27T04:34:56-08:00",
			expected: "2025-01-27T12:34:56Z",
			isError:  false,
		},
		{
			name:     "nanosecond precision with Z",
			input:    "2025-01-27T12:34:56.789123Z",
			expected: "2025-01-27T12:34:56Z",
			isError:  false,
		},
		{
			name:     "nanosecond precision with offset",
			input:    "2025-01-27T17:34:56.789123+05:00",
			expected: "2025-01-27T12:34:56Z",
			isError:  false,
		},
		{
			name:     "empty string returns empty",
			input:    "",
			expected: "",
			isError:  false,
		},
		{
			name:     "invalid format returns empty and logs error",
			input:    "2025-01-27 12:34:56",
			expected: "",
			isError:  true,
		},
		{
			name:     "malformed timestamp returns empty",
			input:    "not-a-timestamp",
			expected: "",
			isError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeTimestampToUTC(tt.input, logger)
			if result != tt.expected {
				t.Errorf("NormalizeTimestampToUTC(%q) = %q, expected %q",
					tt.input, result, tt.expected)
			}
		})
	}
}
