package main

import (
	"strings"
	"testing"

	"github.com/greenearth/ingest/internal/common"
)

func TestParseIndexType_replies(t *testing.T) {
	got, err := ParseIndexType("replies")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != IndexTypeReplies {
		t.Errorf("expected IndexTypeReplies, got %q", got)
	}
}

func TestParseIndexType_replies_periodName(t *testing.T) {
	got, err := ParseIndexType("replies-2026-w23")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != IndexTypeReplies {
		t.Errorf("expected IndexTypeReplies, got %q", got)
	}
}

func TestGenerateFilename_replies(t *testing.T) {
	logger := common.NewLogger(false)
	filename := generateFilename("replies", "2026-06-06T12:00:00Z", logger)
	if !strings.HasPrefix(filename, "bsky_replies_") {
		t.Errorf("expected bsky_replies_ prefix, got %s", filename)
	}
}
