package common

import (
	lru "github.com/hashicorp/golang-lru/v2"
)

// PostRoutingCache caches post at_uri -> author_did mappings to reduce ES lookups
// This is used by jetstream ingest to avoid querying ES for routing info on every batch
type PostRoutingCache struct {
	cache *lru.Cache[string, string]
}

// NewPostRoutingCache creates a new cache with the specified maximum size
// Size represents the number of at_uri -> author_did mappings to cache
func NewPostRoutingCache(size int) (*PostRoutingCache, error) {
	if size <= 0 {
		size = 500000
	}

	cache, err := lru.New[string, string](size)
	if err != nil {
		return nil, err
	}

	return &PostRoutingCache{cache: cache}, nil
}

// Get retrieves the author_did for a given at_uri
// Returns the author_did and true if found, empty string and false otherwise
func (c *PostRoutingCache) Get(atURI string) (authorDID string, found bool) {
	if c == nil || c.cache == nil {
		return "", false
	}
	return c.cache.Get(atURI)
}

// Add adds an at_uri -> author_did mapping to the cache
func (c *PostRoutingCache) Add(atURI, authorDID string) {
	if c == nil || c.cache == nil {
		return
	}
	c.cache.Add(atURI, authorDID)
}

// BulkGet retrieves author_dids for multiple at_uris
// Returns a map of found at_uri -> author_did and a slice of missing at_uris
func (c *PostRoutingCache) BulkGet(atURIs []string) (found map[string]string, missing []string) {
	found = make(map[string]string)
	missing = make([]string, 0)

	if c == nil || c.cache == nil {
		return found, atURIs
	}

	for _, uri := range atURIs {
		if authorDID, ok := c.cache.Get(uri); ok {
			found[uri] = authorDID
		} else {
			missing = append(missing, uri)
		}
	}

	return found, missing
}

// BulkAdd adds multiple at_uri -> author_did mappings to the cache
func (c *PostRoutingCache) BulkAdd(entries map[string]string) {
	if c == nil || c.cache == nil {
		return
	}

	for uri, authorDID := range entries {
		c.cache.Add(uri, authorDID)
	}
}

// Len returns the number of items in the cache
func (c *PostRoutingCache) Len() int {
	if c == nil || c.cache == nil {
		return 0
	}
	return c.cache.Len()
}
