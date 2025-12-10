package common

// TODO: everything in this file is copied from ingest; should have just used by ref, heh, that's what the name implied, right?

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	//	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

// ElasticsearchDoc represents the (Post) document structure for indexing
type ElasticsearchDoc struct {
	AtURI            string               `json:"at_uri"`
	AuthorDID        string               `json:"author_did"`
	Content          string               `json:"content"`
	CreatedAt        string               `json:"created_at"`
	ThreadRootPost   string               `json:"thread_root_post,omitempty"`
	ThreadParentPost string               `json:"thread_parent_post,omitempty"`
	QuotePost        string               `json:"quote_post,omitempty"`
	Embeddings       map[string][]float32 `json:"embeddings,omitempty"`
	IndexedAt        string               `json:"indexed_at"`
}

// ElasticsearchConfig holds configuration for Elasticsearch connection
type ElasticsearchConfig struct {
	URL           string
	APIKey        string
	SkipTLSVerify bool
}

// NewElasticsearchClient creates and tests a new Elasticsearch client
func NewElasticsearchClient(config ElasticsearchConfig, logger *IngestLogger) (*elasticsearch.Client, error) {
	esConfig := elasticsearch.Config{
		Addresses: []string{config.URL},
		APIKey:    config.APIKey,
	}

	if config.SkipTLSVerify {
		logger.Info("TLS certificate verification disabled (local development mode)")
		esConfig.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	client, err := elasticsearch.NewClient(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Elasticsearch: %w", err)
	}
	res.Body.Close()

	logger.Info("Connected to Elasticsearch at %s", config.URL)
	return client, nil
}

// NewElasticsearchTypedClient creates and tests a new typed Elasticsearch client
func NewElasticsearchTypedClient(config ElasticsearchConfig, logger *IngestLogger) (*elasticsearch.TypedClient, error) {
	esConfig := elasticsearch.Config{
		Addresses: []string{config.URL},
		APIKey:    config.APIKey,
	}

	if config.SkipTLSVerify {
		logger.Info("TLS certificate verification disabled (local development mode)")
		esConfig.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	client, err := elasticsearch.NewTypedClient(esConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	logger.Info("Connected to Elasticsearch at %s", config.URL)
	return client, nil
}

// BulkIndex indexes a batch of documents to Elasticsearch
func BulkIndex(ctx context.Context, client *elasticsearch.Client, index string, docs []ElasticsearchDoc, dryRun bool, logger *IngestLogger) error {
	if len(docs) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk index of %d documents to index '%s'", len(docs), index)
		return nil
	}

	var buf bytes.Buffer
	validDocCount := 0

	for _, doc := range docs {
		if doc.AtURI == "" {
			logger.Error("Skipping document with empty at_uri (author_did: %s)", doc.AuthorDID)
			continue
		}

		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index": index,
				"_id":    doc.AtURI,
			},
		}

		validDocCount++

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')

		docJSON, err := json.Marshal(doc)
		if err != nil {
			return fmt.Errorf("failed to marshal document: %w", err)
		}

		buf.Write(docJSON)
		buf.WriteByte('\n')
	}

	if validDocCount == 0 {
		logger.Error("No valid documents to index (all had empty at_uri)")
		return fmt.Errorf("no valid documents in batch")
	}

	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("bulk request failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk request returned error: %s", res.String())
	}

	var bulkResponse struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Error *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("failed to parse bulk response: %w", err)
	}

	if bulkResponse.Errors {
		itemsJSON, _ := json.Marshal(bulkResponse.Items)
		logger.Error("Bulk indexing failed with errors. Response items: %s", string(itemsJSON))
		return fmt.Errorf("bulk indexing failed: some documents had errors (see logs for details)")
	}

	return nil
}

// SearchResponse is the top-level structure for the Elasticsearch search result.
type SearchResponse struct {
	Took     int        `json:"took"`
	TimedOut bool       `json:"timed_out"`
	Shards   ShardsInfo `json:"_shards"`
	Hits     Hits       `json:"hits"`
}

// ShardsInfo contains information about the shards that were part of the search.
type ShardsInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

// Hits contains the search hits.
type Hits struct {
	Total    TotalHits `json:"total"`
	MaxScore float64   `json:"max_score"`
	Hits     []Hit     `json:"hits"`
}

// TotalHits contains the total number of hits and their relation.
type TotalHits struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

// Hit represents a single search hit.
type Hit struct {
	Index  string   `json:"_index"`
	ID     string   `json:"_id"`
	Score  float64  `json:"_score"`
	Source PostData `json:"_source"`
}

// PostData represents the _source field of a search hit.
// This is identical to the 'ElasticsearchDoc' struct in your project.
type PostData struct {
	AtURI            string               `json:"at_uri"`
	AuthorDID        string               `json:"author_did"`
	Content          string               `json:"content"`
	CreatedAt        string               `json:"created_at"`
	ThreadRootPost   string               `json:"thread_root_post,omitempty"`
	ThreadParentPost string               `json:"thread_parent_post,omitempty"`
	Embeddings       map[string][]float32 `json:"embeddings,omitempty"`
	QuotePost        string               `json:"quote_post,omitempty"`
	IndexedAt        string               `json:"indexed_at"`
	EsID             string               `json:"es_id"`
}

/*
func foo() {
	jsonData := `{
	  "took": 1,
	  "timed_out": false,
	  "_shards": {
		"total": 1,
		"successful": 1,
		"skipped": 0,
		"failed": 0
	  },
	  "hits": {
		"total": {
		  "value": 1797,
		  "relation": "eq"
		},
		"max_score": 1,
		"hits": [
		  {
			"_index": "posts_v1",
			"_id": "at://did:plc:2szzeh6xi4qytejg6qezm7jp/app.bsky.feed.post/3lygln7l4el2v",
			"_score": 1,
			"_source": {
			  "at_uri": "at://did:plc:2szzeh6xi4qytejg6qezm7jp/app.bsky.feed.post/3lygln7l4el2v",
			  "author_did": "did:plc:2szzeh6xi4qytejg6qezm7jp",
			  "content": "I don't like being too specific about my posts on liberty's death I don't like bringing down the mood of the tl but I just want to put my thoughts out there sometimes\\n\\nthe recent news put everything into my mind again",
			  "created_at": "2025-09-09T20:46:34.988Z",
			  "thread_root_post": "at://did:plc:2szzeh6xi4qytejg6qezm7jp/app.bsky.feed.post/3lygln7kukk2v",
			  "thread_parent_post": "at://did:plc:2szzeh6xi4qytejg6qezm7jp/app.bsky.feed.post/3lygln7l4ek2v",
			  "indexed_at": "2025-11-04T00:34:04Z"
			}
		  }
		]
	  }
	}`
*/

// NOTE: both after params must be supplied if either is supplied; the ordering clause is always
// afterCreatedAt asc, afterIndexedAt asc - so afterIndexedAt is just a "tiebreaker" for posts with same CreatedAt
// also: note that only fields in the base record can be used for sorting/pagination; EsID cannot be used

func FetchPosts(esClient *elasticsearch.Client, loggerPtr *IngestLogger, afterCreatedAt string, afterIndexedAt string) (SearchResponse, error) {

	logger := *loggerPtr

	var response SearchResponse

	var query strings.Builder
	query.WriteString(`{ "query": { "match_all": {} } `)
	query.WriteString(`, "sort": [ { "created_at": "asc" }, { "indexed_at": "asc" } ] `)
	after := `, "search_after": [ "%s" , "%s" ] }`
	if afterCreatedAt != "" && afterIndexedAt != "" {
		query.WriteString(fmt.Sprintf(after, afterCreatedAt, afterIndexedAt))
	} else {
		query.WriteString(" }")
	}
	//	var query string
	//	query = `{ "query": { "match_all": {} } }`
	//	} else {
	//		// continuing to fetch after last record already retrieved
	//		query = `{ "query": { "match_all": {} } }`
	//	}

	q := query.String()
	logger.Info("%s", q) // this is BS, there has to be a simpler way to log the query

	// Execute the search query
	res, err := esClient.Search(
		esClient.Search.WithIndex("posts"),
		esClient.Search.WithSize(1000),
		esClient.Search.WithBody(strings.NewReader(query.String())))

	if err != nil {
		return response, err
	} else {
		logger.Info("Elasticsearch search query succeeded")
		//logger.Info("ES response: %v", res)
		//logger.Info("ES response body: %v", res.Body)
	}

	// Read the response body into a byte slice
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return response, err
	}
	res.Body.Close() // Always close the response body

	if err := json.Unmarshal(bodyBytes, &response); err != nil {
		logger.Error("Error unmarshalling JSON response from Elasticsearch: %v", err)
		return response, err
	}

	fmt.Printf("Successfully unmarshalled data.\n")
	fmt.Printf("Total hits: %d\n", response.Hits.Total.Value)
	if len(response.Hits.Hits) > 0 {
		fmt.Printf("Total number of hits: %d\n", response.Hits.Total.Value)
		fmt.Printf("Hits returned in this pull: %d\n", len(response.Hits.Hits))
		fmt.Printf("First hit content: %s\n", response.Hits.Hits[0].Source.Content)
	}

	return response, nil
}
