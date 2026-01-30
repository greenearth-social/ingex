package common

import (
	"testing"
)

func TestNewPostRoutingCache(t *testing.T) {
	cache, err := NewPostRoutingCache(100)
	if err != nil {
		t.Fatalf("Failed to create cache: %v", err)
	}

	if cache == nil {
		t.Fatal("Expected non-nil cache")
	}

	if cache.Len() != 0 {
		t.Errorf("Expected empty cache, got %d items", cache.Len())
	}
}

func TestNewPostRoutingCache_DefaultSize(t *testing.T) {
	cache, err := NewPostRoutingCache(0)
	if err != nil {
		t.Fatalf("Failed to create cache with default size: %v", err)
	}

	if cache == nil {
		t.Fatal("Expected non-nil cache")
	}
}

func TestPostRoutingCache_GetAndAdd(t *testing.T) {
	cache, _ := NewPostRoutingCache(100)

	authorDID, found := cache.Get("at://post1")
	if found {
		t.Error("Expected cache miss for non-existent key")
	}
	if authorDID != "" {
		t.Errorf("Expected empty authorDID for miss, got %s", authorDID)
	}

	cache.Add("at://post1", "did:plc:author1")

	authorDID, found = cache.Get("at://post1")
	if !found {
		t.Error("Expected cache hit after add")
	}
	if authorDID != "did:plc:author1" {
		t.Errorf("Expected did:plc:author1, got %s", authorDID)
	}
}

func TestPostRoutingCache_BulkGet(t *testing.T) {
	cache, _ := NewPostRoutingCache(100)

	cache.Add("at://post1", "did:plc:author1")
	cache.Add("at://post2", "did:plc:author2")

	uris := []string{"at://post1", "at://post2", "at://post3", "at://post4"}
	found, missing := cache.BulkGet(uris)

	if len(found) != 2 {
		t.Errorf("Expected 2 found, got %d", len(found))
	}

	if found["at://post1"] != "did:plc:author1" {
		t.Errorf("Expected did:plc:author1 for post1, got %s", found["at://post1"])
	}

	if found["at://post2"] != "did:plc:author2" {
		t.Errorf("Expected did:plc:author2 for post2, got %s", found["at://post2"])
	}

	if len(missing) != 2 {
		t.Errorf("Expected 2 missing, got %d", len(missing))
	}

	expectedMissing := map[string]bool{"at://post3": true, "at://post4": true}
	for _, uri := range missing {
		if !expectedMissing[uri] {
			t.Errorf("Unexpected missing URI: %s", uri)
		}
	}
}

func TestPostRoutingCache_BulkAdd(t *testing.T) {
	cache, _ := NewPostRoutingCache(100)

	entries := map[string]string{
		"at://post1": "did:plc:author1",
		"at://post2": "did:plc:author2",
		"at://post3": "did:plc:author3",
	}

	cache.BulkAdd(entries)

	if cache.Len() != 3 {
		t.Errorf("Expected 3 items in cache, got %d", cache.Len())
	}

	for uri, expectedDID := range entries {
		authorDID, found := cache.Get(uri)
		if !found {
			t.Errorf("Expected to find %s in cache", uri)
		}
		if authorDID != expectedDID {
			t.Errorf("Expected %s for %s, got %s", expectedDID, uri, authorDID)
		}
	}
}

func TestPostRoutingCache_NilSafe(t *testing.T) {
	var cache *PostRoutingCache

	authorDID, found := cache.Get("at://post1")
	if found {
		t.Error("Expected false for nil cache")
	}
	if authorDID != "" {
		t.Errorf("Expected empty string for nil cache, got %s", authorDID)
	}

	cache.Add("at://post1", "did:plc:author1")

	found2, missing := cache.BulkGet([]string{"at://post1"})
	if len(found2) != 0 {
		t.Errorf("Expected empty found map for nil cache, got %d", len(found2))
	}
	if len(missing) != 1 {
		t.Errorf("Expected all URIs in missing for nil cache, got %d", len(missing))
	}

	cache.BulkAdd(map[string]string{"at://post1": "did:plc:author1"})

	if cache.Len() != 0 {
		t.Errorf("Expected 0 for nil cache, got %d", cache.Len())
	}
}

func TestPostRoutingCache_Eviction(t *testing.T) {
	cache, _ := NewPostRoutingCache(3)

	cache.Add("at://post1", "did:plc:author1")
	cache.Add("at://post2", "did:plc:author2")
	cache.Add("at://post3", "did:plc:author3")

	if cache.Len() != 3 {
		t.Errorf("Expected 3 items, got %d", cache.Len())
	}

	cache.Add("at://post4", "did:plc:author4")

	if cache.Len() != 3 {
		t.Errorf("Expected 3 items after eviction, got %d", cache.Len())
	}

	_, found := cache.Get("at://post4")
	if !found {
		t.Error("Expected post4 to be in cache")
	}
}
