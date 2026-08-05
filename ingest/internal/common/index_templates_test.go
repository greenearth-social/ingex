package common

import (
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// templatesDir locates index/deploy/k8s/base/templates relative to this package.
//
// Caveat: these YAML files are outside this Go package, so `go test` will serve
// a cached PASS after they change. Run with `-count=1` when editing a template.
// CI starts from a cold cache, so the guard is reliable there.
func templatesDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join("..", "..", "..", "index", "deploy", "k8s", "base", "templates")
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("index templates not found at %s: %v", dir, err)
	}
	return dir
}

var templateVarRE = regexp.MustCompile(`\$\([A-Z_]+\)`)

// loadTemplateJSON extracts the template JSON from a k8s ConfigMap YAML,
// substituting kustomize vars with a placeholder so it parses. The YAML here is
// a fixed two-level shape, so a full YAML parser is not needed.
func loadTemplateJSON(t *testing.T, path string) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}

	lines := strings.Split(string(raw), "\n")
	start := -1
	for i, line := range lines {
		if strings.HasSuffix(strings.TrimSpace(line), ".json: |") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		t.Fatalf("%s: no '<name>.json: |' block found", filepath.Base(path))
	}

	var body []string
	for _, line := range lines[start:] {
		if strings.TrimSpace(line) == "" {
			body = append(body, "")
			continue
		}
		if !strings.HasPrefix(line, "    ") {
			break
		}
		body = append(body, strings.TrimPrefix(line, "    "))
	}

	cleaned := templateVarRE.ReplaceAllString(strings.Join(body, "\n"), "1")
	var doc map[string]any
	if err := json.Unmarshal([]byte(cleaned), &doc); err != nil {
		t.Fatalf("%s: parse template JSON: %v", filepath.Base(path), err)
	}
	return doc
}

// TestIndexTemplatesDoNotExcludeEmbeddingsFromSource guards greenearth-social/ingex#444.
//
// Elasticsearch's update API rebuilds a document from its stored _source, so any
// field excluded from _source is silently dropped the next time the document is
// updated. Both like-count write paths (BulkUpdateLikeCounts here, and the
// devenv seed) are exactly such updates, so excluding "embeddings" from _source
// meant a post lost every embedding the first time it was liked — which made
// `like_count>=20` and "has a searchable vector" mutually exclusive and left
// two-tower kNN unable to return anything.
//
// "mode": "synthetic" is NOT the fix: it is an Enterprise feature, and on our
// basic licence Elasticsearch silently ignores it and stores _source normally.
// Measured on ES 9.0.0: a synthetic-mode index and a default index produced
// byte-identical stores, and the synthetic index's mapping did not echo the
// _source block back at all. Anything that relies on it would be a no-op that
// reads as a working optimisation, so this test rejects it too.
func TestIndexTemplatesDoNotExcludeEmbeddingsFromSource(t *testing.T) {
	dir := templatesDir(t)
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read templates dir: %v", err)
	}

	checked := 0
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), "-index-template.yaml") {
			continue
		}
		doc := loadTemplateJSON(t, filepath.Join(dir, entry.Name()))

		tmpl, ok := doc["template"].(map[string]any)
		if !ok {
			continue
		}
		mappings, ok := tmpl["mappings"].(map[string]any)
		if !ok {
			continue
		}
		checked++

		source, ok := mappings["_source"].(map[string]any)
		if !ok {
			continue
		}

		if mode, _ := source["mode"].(string); mode == "synthetic" {
			t.Errorf("%s sets _source.mode=synthetic: that is an Enterprise feature, "+
				"silently ignored on our basic licence (measured byte-identical to a "+
				"default index on ES 9.0.0). It looks like an optimisation but is a "+
				"no-op — see ingex#444", entry.Name())
		}

		excludes, ok := source["excludes"].([]any)
		if !ok {
			continue
		}
		for _, ex := range excludes {
			field, _ := ex.(string)
			if field == "embeddings" || strings.HasPrefix(field, "embeddings.") {
				t.Errorf("%s excludes %q from _source: any update to a document "+
					"(e.g. a like_count increment) will silently drop it, destroying "+
					"the vectors two-tower kNN and the extract/training pipeline rely "+
					"on — see ingex#444", entry.Name(), field)
			}
		}
	}

	if checked == 0 {
		t.Fatal("no index templates were parsed; the check did not run")
	}
}
