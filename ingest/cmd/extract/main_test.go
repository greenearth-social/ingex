package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/greenearth/ingest/internal/common"
	"github.com/parquet-go/parquet-go"
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

func TestWritePostsParquetFilePostsAndReplies(t *testing.T) {
	for _, tc := range []struct {
		indexName string
		filename  string
	}{
		{"posts", "bsky_posts_20260606_120000.parquet"},
		{"posts-2026-w23", "bsky_posts_20260606_120000.parquet"},
		{"replies", "bsky_replies_20260606_120000.parquet"},
		{"replies-2026-w23", "bsky_replies_20260606_120000.parquet"},
	} {
		t.Run(tc.indexName, func(t *testing.T) {
			posts := []common.ExtractPost{{
				AtURI:           "at://did:plc:test/app.bsky.feed.post/media",
				RecordCreatedAt: "2026-06-06T12:00:00Z",
				ContainsImages:  true,
				ContainsVideo:   false,
			}}
			dir := t.TempDir()
			if err := writePostsParquetFile(context.Background(), dir, false, nil, "", "", tc.indexName, posts, common.NewLogger(false)); err != nil {
				t.Fatal(err)
			}
			files, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 1 || files[0].Name() != tc.filename {
				t.Fatalf("expected only %s, got %v", tc.filename, files)
			}
			rows, err := parquet.ReadFile[common.ExtractPost](filepath.Join(dir, tc.filename))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != 1 {
				t.Fatalf("read %d rows, want 1", len(rows))
			}
			if rows[0].AtURI != posts[0].AtURI || !rows[0].ContainsImages || rows[0].ContainsVideo {
				t.Errorf("exported row = %+v, want media flags (true, false) for %s", rows[0], posts[0].AtURI)
			}
		})
	}
}
