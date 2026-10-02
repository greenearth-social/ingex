package common

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestExtractPostMediaFlagsRoundTrip(t *testing.T) {
	// Decode real ES field names, including an older document without flags.
	const fixture = `[
		{"_source":{"at_uri":"at://images","contains_images":true,"contains_video":false}},
		{"_source":{"at_uri":"at://video","contains_images":false,"contains_video":true}},
		{"_source":{"at_uri":"at://neither","contains_images":false,"contains_video":false}},
		{"_source":{"at_uri":"at://both","contains_images":true,"contains_video":true}},
		{"_source":{"at_uri":"at://absent"}}
	]`
	var hits []Hit
	if err := json.Unmarshal([]byte(fixture), &hits); err != nil {
		t.Fatal(err)
	}
	posts := HitsToExtractPosts(hits)

	// Use an independent projection so a change to ExtractPost's field tags
	// cannot make both writing and reading silently agree on the wrong names.
	type mediaRow struct {
		AtURI          string `parquet:"at_uri"`
		ContainsImages bool   `parquet:"contains_images"`
		ContainsVideo  bool   `parquet:"contains_video"`
	}
	want := []mediaRow{
		{AtURI: "at://images", ContainsImages: true},
		{AtURI: "at://video", ContainsVideo: true},
		{AtURI: "at://neither"},
		{AtURI: "at://both", ContainsImages: true, ContainsVideo: true},
		{AtURI: "at://absent"},
	}

	// JSON must also retain explicit false values under the exported names.
	encoded, err := json.Marshal(posts)
	if err != nil {
		t.Fatal(err)
	}
	var jsonRows []map[string]any
	if err := json.Unmarshal(encoded, &jsonRows); err != nil {
		t.Fatal(err)
	}
	for i, row := range jsonRows {
		if row["contains_images"] != want[i].ContainsImages || row["contains_video"] != want[i].ContainsVideo {
			t.Errorf("JSON row %d: flags = (%v, %v), want (%v, %v)", i,
				row["contains_images"], row["contains_video"], want[i].ContainsImages, want[i].ContainsVideo)
		}
	}

	writers := []struct {
		name  string
		write func(*testing.T) []byte
	}{
		{
			name: "local_file",
			write: func(t *testing.T) []byte {
				path := filepath.Join(t.TempDir(), "posts.parquet")
				if err := parquet.WriteFile(path, posts); err != nil {
					t.Fatal(err)
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				return data
			},
		},
		{
			name: "streaming",
			write: func(t *testing.T) []byte {
				var buffer bytes.Buffer
				writer := parquet.NewGenericWriter[ExtractPost](&buffer)
				for _, batch := range [][]ExtractPost{posts[:2], posts[2:]} {
					if _, err := writer.Write(batch); err != nil {
						t.Fatal(err)
					}
				}
				if err := writer.Close(); err != nil {
					t.Fatal(err)
				}
				return buffer.Bytes()
			},
		},
	}
	for _, writer := range writers {
		t.Run(writer.name, func(t *testing.T) {
			data := writer.write(t)
			file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"contains_images", "contains_video"} {
				found := false
				for _, field := range file.Schema().Fields() {
					if field.Name() != name {
						continue
					}
					found = true
					if !field.Leaf() || field.Type().Kind() != parquet.Boolean {
						t.Errorf("%s must be a BOOLEAN leaf, got %s", name, field)
					}
					if !field.Required() {
						t.Errorf("%s must be required (non-null)", name)
					}
				}
				if !found {
					t.Errorf("missing column %q", name)
				}
			}
			rows, err := parquet.Read[mediaRow](bytes.NewReader(data), int64(len(data)))
			if err != nil {
				t.Fatal(err)
			}
			if len(rows) != len(want) {
				t.Fatalf("read %d rows, want %d", len(rows), len(want))
			}
			for i, row := range rows {
				if row != want[i] {
					t.Errorf("row %d = %+v, want %+v", i, row, want[i])
				}
			}
		})
	}
}
