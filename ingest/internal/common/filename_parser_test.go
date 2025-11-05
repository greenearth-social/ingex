package common

import (
	"testing"
	"time"
)

func TestParseMegastreamFilenameTimestamp_Valid(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		want     int64
	}{
		{
			name:     "basic filename",
			filename: "mega_jetstream_20250109_120000.db.zip",
			want:     time.Date(2025, 1, 9, 12, 0, 0, 0, time.UTC).UnixMicro(),
		},
		{
			name:     "midnight timestamp",
			filename: "mega_jetstream_20250101_000000.db.zip",
			want:     time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro(),
		},
		{
			name:     "end of day timestamp",
			filename: "mega_jetstream_20251231_235959.db.zip",
			want:     time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC).UnixMicro(),
		},
		{
			name:     "with full path",
			filename: "/path/to/mega_jetstream_20250605_153045.db.zip",
			want:     time.Date(2025, 6, 5, 15, 30, 45, 0, time.UTC).UnixMicro(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseMegastreamFilenameTimestamp(tt.filename)
			if err != nil {
				t.Fatalf("ParseMegastreamFilenameTimestamp() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("ParseMegastreamFilenameTimestamp() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseMegastreamFilenameTimestamp_Invalid(t *testing.T) {
	tests := []struct {
		name     string
		filename string
	}{
		{
			name:     "wrong prefix",
			filename: "jetstream_20250109_120000.db.zip",
		},
		{
			name:     "missing date",
			filename: "mega_jetstream_120000.db.zip",
		},
		{
			name:     "missing time",
			filename: "mega_jetstream_20250109.db.zip",
		},
		{
			name:     "wrong extension",
			filename: "mega_jetstream_20250109_120000.db",
		},
		{
			name:     "no extension",
			filename: "mega_jetstream_20250109_120000",
		},
		{
			name:     "invalid date format",
			filename: "mega_jetstream_2025-01-09_120000.db.zip",
		},
		{
			name:     "invalid time format",
			filename: "mega_jetstream_20250109_12:00:00.db.zip",
		},
		{
			name:     "empty filename",
			filename: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseMegastreamFilenameTimestamp(tt.filename)
			if err == nil {
				t.Errorf("ParseMegastreamFilenameTimestamp() expected error for invalid filename %q", tt.filename)
			}
		})
	}
}
