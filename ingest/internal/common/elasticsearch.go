package common

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/elastic/go-elasticsearch/v9"
)

// Float32Array is a wrapper for []float32 that ensures values are always marshaled as floats
type Float32Array []float32

// MarshalJSON implements custom JSON marshaling to ensure floats are serialized with decimals
func (f Float32Array) MarshalJSON() ([]byte, error) {
	if f == nil {
		return []byte("null"), nil
	}
	// Convert to []float64 which json.Marshal handles more reliably as floats
	float64Array := make([]float64, len(f))
	for i, v := range f {
		float64Array[i] = float64(v)
	}
	return json.Marshal(float64Array)
}

// ElasticsearchDoc represents the document structure for indexing
type ElasticsearchDoc struct {
	AtURI            string                  `json:"at_uri"`
	AuthorDID        string                  `json:"author_did"`
	Content          string                  `json:"content"`
	CreatedAt        string                  `json:"created_at"`
	ThreadRootPost   string                  `json:"thread_root_post,omitempty"`
	ThreadParentPost string                  `json:"thread_parent_post,omitempty"`
	QuotePost        string                  `json:"quote_post,omitempty"`
	Embeddings       map[string]Float32Array `json:"embeddings,omitempty"`
	IndexedAt        string                  `json:"indexed_at"`
}

// PostTombstoneDoc represents the document structure for post deletion tombstones
type PostTombstoneDoc struct {
	AtURI     string `json:"at_uri"`
	AuthorDID string `json:"author_did"`
	DeletedAt string `json:"deleted_at"`
	IndexedAt string `json:"indexed_at"`
}

// LikeDoc represents the document structure for indexing likes
type LikeDoc struct {
	AtURI      string `json:"at_uri"`
	SubjectURI string `json:"subject_uri"`
	AuthorDID  string `json:"author_did"`
	CreatedAt  string `json:"created_at"`
	IndexedAt  string `json:"indexed_at"`
}

// LikeIdentifier holds the at_uri and author_did pair for looking up likes
type LikeIdentifier struct {
	AtURI     string
	AuthorDID string
}

// LikeTombstoneDoc represents the document structure for like deletion tombstones
type LikeTombstoneDoc struct {
	AtURI      string `json:"at_uri"`
	AuthorDID  string `json:"author_did"`
	SubjectURI string `json:"subject_uri"`
	DeletedAt  string `json:"deleted_at"`
	IndexedAt  string `json:"indexed_at"`
}

// DeleteDoc represents a document to be deleted with routing information
type DeleteDoc struct {
	DocID     string
	AuthorDID string
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
				InsecureSkipVerify: true, // nolint:gosec // G402: Required for local development with self-signed certs
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
	if err := res.Body.Close(); err != nil {
		logger.Error("Failed to close response body: %v", err)
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
				"_index":  index,
				"_id":     doc.AtURI,
				"routing": doc.AuthorDID,
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
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

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

// BulkIndexPostTombstones indexes a batch of post tombstone documents to Elasticsearch
func BulkIndexPostTombstones(ctx context.Context, client *elasticsearch.Client, index string, docs []PostTombstoneDoc, dryRun bool, logger *IngestLogger) error {
	if len(docs) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk index of %d tombstones to index '%s'", len(docs), index)
		return nil
	}

	var buf bytes.Buffer
	validDocCount := 0

	for _, doc := range docs {
		if doc.AtURI == "" {
			logger.Error("Skipping tombstone with empty at_uri (author_did: %s)", doc.AuthorDID)
			continue
		}

		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index":  index,
				"_id":     doc.AtURI,
				"routing": doc.AuthorDID,
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
			return fmt.Errorf("failed to marshal tombstone document: %w", err)
		}

		buf.Write(docJSON)
		buf.WriteByte('\n')
	}

	if validDocCount == 0 {
		logger.Error("No valid tombstones to index (all had empty at_uri)")
		return fmt.Errorf("no valid tombstones in batch")
	}

	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("bulk tombstone request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk tombstone request returned error: %s", res.String())
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
		return fmt.Errorf("failed to parse bulk tombstone response: %w", err)
	}

	if bulkResponse.Errors {
		itemsJSON, _ := json.Marshal(bulkResponse.Items)
		logger.Error("Bulk tombstone indexing failed with errors. Response items: %s", string(itemsJSON))
		return fmt.Errorf("bulk tombstone indexing failed: some documents had errors (see logs for details)")
	}

	return nil
}

// BulkDelete deletes a batch of documents from Elasticsearch by their IDs with routing
func BulkDelete(ctx context.Context, client *elasticsearch.Client, index string, docs []DeleteDoc, dryRun bool, logger *IngestLogger) error {
	if len(docs) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk delete of %d documents from index '%s'", len(docs), index)
		return nil
	}

	var buf bytes.Buffer
	validDocCount := 0

	for _, doc := range docs {
		if doc.DocID == "" {
			logger.Error("Skipping delete with empty document ID")
			continue
		}

		meta := map[string]interface{}{
			"delete": map[string]interface{}{
				"_index":  index,
				"_id":     doc.DocID,
				"routing": doc.AuthorDID,
			},
		}

		validDocCount++

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal delete metadata: %w", err)
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')
	}

	if validDocCount == 0 {
		logger.Error("No valid document IDs to delete (all were empty)")
		return fmt.Errorf("no valid document IDs in batch")
	}

	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("bulk delete request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk delete request returned error: %s", res.String())
	}

	var bulkResponse struct {
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Error *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
			Status int `json:"status"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("failed to parse bulk delete response: %w", err)
	}

	if bulkResponse.Errors {
		hasRealErrors := false
		for _, item := range bulkResponse.Items {
			for _, details := range item {
				if details.Error != nil && details.Status != 404 {
					hasRealErrors = true
					break
				}
			}
		}

		if hasRealErrors {
			itemsJSON, _ := json.Marshal(bulkResponse.Items)
			logger.Error("Bulk delete failed with errors. Response items: %s", string(itemsJSON))
			return fmt.Errorf("bulk delete failed: some documents had errors (see logs for details)")
		}
	}

	return nil
}

// CreateElasticsearchDoc creates an ElasticsearchDoc from a MegaStreamMessage
func CreateElasticsearchDoc(msg MegaStreamMessage) ElasticsearchDoc {
	// Convert embeddings to Float32Array type for proper JSON marshaling
	var embeddings map[string]Float32Array
	rawEmbeddings := msg.GetEmbeddings()
	if rawEmbeddings != nil {
		embeddings = make(map[string]Float32Array, len(rawEmbeddings))
		for key, value := range rawEmbeddings {
			embeddings[key] = Float32Array(value)
		}
	}

	return ElasticsearchDoc{
		AtURI:            msg.GetAtURI(),
		AuthorDID:        msg.GetAuthorDID(),
		Content:          msg.GetContent(),
		CreatedAt:        msg.GetCreatedAt(),
		ThreadRootPost:   msg.GetThreadRootPost(),
		ThreadParentPost: msg.GetThreadParentPost(),
		QuotePost:        msg.GetQuotePost(),
		Embeddings:       embeddings,
		IndexedAt:        time.Now().UTC().Format(time.RFC3339),
	}
}

// CreatePostTombstoneDoc creates a PostTombstoneDoc from a MegaStreamMessage
func CreatePostTombstoneDoc(msg MegaStreamMessage) PostTombstoneDoc {
	now := time.Now().UTC()
	deletedAt := now

	if timeUs := msg.GetTimeUs(); timeUs > 0 {
		deletedAt = time.Unix(0, timeUs*1000)
	}

	return PostTombstoneDoc{
		AtURI:     msg.GetAtURI(),
		AuthorDID: msg.GetAuthorDID(),
		DeletedAt: deletedAt.Format(time.RFC3339),
		IndexedAt: now.Format(time.RFC3339),
	}
}

// CreateLikeDoc creates a LikeDoc from a JetstreamMessage
func CreateLikeDoc(msg JetstreamMessage) LikeDoc {
	return LikeDoc{
		AtURI:      msg.GetAtURI(),
		SubjectURI: msg.GetSubjectURI(),
		AuthorDID:  msg.GetAuthorDID(),
		CreatedAt:  msg.GetCreatedAt(),
		IndexedAt:  time.Now().UTC().Format(time.RFC3339),
	}
}

// CreateLikeTombstoneDoc creates a LikeTombstoneDoc from a JetstreamMessage and subject URI
func CreateLikeTombstoneDoc(msg JetstreamMessage, subjectURI string) LikeTombstoneDoc {
	now := time.Now().UTC()
	deletedAt := now

	if timeUs := msg.GetTimeUs(); timeUs > 0 {
		deletedAt = time.Unix(0, timeUs*1000)
	}

	return LikeTombstoneDoc{
		AtURI:      msg.GetAtURI(),
		AuthorDID:  msg.GetAuthorDID(),
		SubjectURI: subjectURI,
		DeletedAt:  deletedAt.Format(time.RFC3339),
		IndexedAt:  now.Format(time.RFC3339),
	}
}

// BulkIndexLikes indexes a batch of like documents to Elasticsearch
func BulkIndexLikes(ctx context.Context, client *elasticsearch.Client, index string, docs []LikeDoc, dryRun bool, logger *IngestLogger) error {
	if len(docs) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk index of %d likes to index '%s'", len(docs), index)
		return nil
	}

	var buf bytes.Buffer
	validDocCount := 0

	for _, doc := range docs {
		if doc.AtURI == "" {
			logger.Error("Skipping like with empty at_uri (author_did: %s)", doc.AuthorDID)
			continue
		}

		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index":  index,
				"_id":     doc.AtURI,
				"routing": doc.AuthorDID,
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
			return fmt.Errorf("failed to marshal like document: %w", err)
		}

		buf.Write(docJSON)
		buf.WriteByte('\n')
	}

	if validDocCount == 0 {
		logger.Error("No valid likes to index (all had empty at_uri)")
		return fmt.Errorf("no valid likes in batch")
	}

	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("bulk like request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk like request returned error: %s", res.String())
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
		return fmt.Errorf("failed to parse bulk like response: %w", err)
	}

	if bulkResponse.Errors {
		itemsJSON, _ := json.Marshal(bulkResponse.Items)
		logger.Error("Bulk like indexing failed with errors. Response items: %s", string(itemsJSON))
		return fmt.Errorf("bulk like indexing failed: some documents had errors (see logs for details)")
	}

	return nil
}

// BulkGetLikes fetches multiple like documents from Elasticsearch by at_uri with routing
func BulkGetLikes(ctx context.Context, client *elasticsearch.Client, index string, likeIDs []LikeIdentifier, logger *IngestLogger) (map[string]LikeDoc, error) {
	if len(likeIDs) == 0 {
		return make(map[string]LikeDoc), nil
	}

	// Build mget request with proper docs array structure
	docs := make([]map[string]interface{}, 0, len(likeIDs))
	for _, id := range likeIDs {
		if id.AtURI == "" {
			continue
		}

		doc := map[string]interface{}{
			"_index": index,
			"_id":    id.AtURI,
		}

		// Add routing if author_did is provided
		if id.AuthorDID != "" {
			doc["routing"] = id.AuthorDID
		}

		docs = append(docs, doc)
	}

	// Wrap docs in the required structure
	requestBody := map[string]interface{}{
		"docs": docs,
	}

	bodyJSON, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal mget request: %w", err)
	}

	// Execute mget request
	res, err := client.Mget(
		bytes.NewReader(bodyJSON),
		client.Mget.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("mget request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close mget response body: %v", err)
		}
	}()

	if res.IsError() {
		return nil, fmt.Errorf("mget request returned error: %s", res.String())
	}

	// Parse response
	var mgetResponse struct {
		Docs []struct {
			ID     string  `json:"_id"`
			Found  bool    `json:"found"`
			Source LikeDoc `json:"_source"`
		} `json:"docs"`
	}

	if err := json.NewDecoder(res.Body).Decode(&mgetResponse); err != nil {
		return nil, fmt.Errorf("failed to parse mget response: %w", err)
	}

	// Build result map
	result := make(map[string]LikeDoc)
	for _, doc := range mgetResponse.Docs {
		if doc.Found {
			result[doc.ID] = doc.Source
		} else {
			logger.Debug("Like document not found for deletion: at_uri=%s", doc.ID)
		}
	}

	return result, nil
}

// BulkIndexLikeTombstones indexes a batch of like tombstone documents to Elasticsearch
func BulkIndexLikeTombstones(ctx context.Context, client *elasticsearch.Client, index string, docs []LikeTombstoneDoc, dryRun bool, logger *IngestLogger) error {
	if len(docs) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk index of %d like tombstones to index '%s'", len(docs), index)
		return nil
	}

	var buf bytes.Buffer
	validDocCount := 0

	for _, doc := range docs {
		if doc.AtURI == "" {
			logger.Error("Skipping like tombstone with empty at_uri (author_did: %s)", doc.AuthorDID)
			continue
		}

		if doc.SubjectURI == "" {
			logger.Error("Skipping like tombstone with empty subject_uri (at_uri: %s)", doc.AtURI)
			continue
		}

		meta := map[string]interface{}{
			"index": map[string]interface{}{
				"_index":  index,
				"_id":     doc.AtURI,
				"routing": doc.AuthorDID,
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
			return fmt.Errorf("failed to marshal like tombstone document: %w", err)
		}

		buf.Write(docJSON)
		buf.WriteByte('\n')
	}

	if validDocCount == 0 {
		logger.Error("No valid like tombstones to index (all had empty at_uri or subject_uri)")
		return fmt.Errorf("no valid like tombstones in batch")
	}

	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("bulk like tombstone request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk like tombstone request returned error: %s", res.String())
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
		return fmt.Errorf("failed to parse bulk like tombstone response: %w", err)
	}

	if bulkResponse.Errors {
		itemsJSON, _ := json.Marshal(bulkResponse.Items)
		logger.Error("Bulk like tombstone indexing failed with errors. Response items: %s", string(itemsJSON))
		return fmt.Errorf("bulk like tombstone indexing failed: some documents had errors (see logs for details)")
	}

	return nil
}

// SearchResponse represents the response from an Elasticsearch search query
type SearchResponse struct {
	Took     int        `json:"took"`
	TimedOut bool       `json:"timed_out"`
	Shards   ShardsInfo `json:"_shards"`
	Hits     Hits       `json:"hits"`
}

// ShardsInfo contains information about the shards that were queried
type ShardsInfo struct {
	Total      int `json:"total"`
	Successful int `json:"successful"`
	Skipped    int `json:"skipped"`
	Failed     int `json:"failed"`
}

// Hits contains the search results
type Hits struct {
	Total    TotalHits `json:"total"`
	MaxScore float64   `json:"max_score"`
	Hits     []Hit     `json:"hits"`
}

// TotalHits contains the total number of hits and their relation
type TotalHits struct {
	Value    int    `json:"value"`
	Relation string `json:"relation"`
}

// Hit represents a single search hit
type Hit struct {
	Index  string        `json:"_index"`
	ID     string        `json:"_id"`
	Score  float64       `json:"_score"`
	Sort   []interface{} `json:"sort,omitempty"`
	Source PostData      `json:"_source"`
}

// PostData represents the _source field of a search hit
type PostData struct {
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

// LikeData represents the _source field of a like search hit
type LikeData struct {
	AtURI      string `json:"at_uri"`
	SubjectURI string `json:"subject_uri"`
	AuthorDID  string `json:"author_did"`
	CreatedAt  string `json:"created_at"`
	IndexedAt  string `json:"indexed_at"`
}

// LikeHit represents a single like search hit
type LikeHit struct {
	Index  string        `json:"_index"`
	ID     string        `json:"_id"`
	Score  float64       `json:"_score"`
	Sort   []interface{} `json:"sort,omitempty"`
	Source LikeData      `json:"_source"`
}

// LikeHits contains the like search results
type LikeHits struct {
	Total    TotalHits `json:"total"`
	MaxScore float64   `json:"max_score"`
	Hits     []LikeHit `json:"hits"`
}

// LikeSearchResponse represents the response from an Elasticsearch like search query
type LikeSearchResponse struct {
	Took     int        `json:"took"`
	TimedOut bool       `json:"timed_out"`
	Shards   ShardsInfo `json:"_shards"`
	Hits     LikeHits   `json:"hits"`
}

// FetchPosts queries Elasticsearch with pagination using search_after
// Parameters:
//   - client: Elasticsearch client
//   - logger: Logger for debug/error messages
//   - index: Index name to query
//   - startTime, endTime: optional time range filter on created_at field (RFC3339 format)
//   - afterCreatedAt, afterIndexedAt: pagination cursors (both required if either provided)
//   - size: number of results to fetch (defaults to 1000 if 0)
func FetchPosts(ctx context.Context, client *elasticsearch.Client, logger *IngestLogger, index string, startTime string, endTime string, afterCreatedAt string, afterIndexedAt string, size int) (SearchResponse, error) {
	var response SearchResponse

	if size <= 0 {
		size = 1000
	}

	// Build query based on whether time range is specified
	var queryClause map[string]interface{}
	if startTime != "" || endTime != "" {
		rangeQuery := map[string]interface{}{}
		if startTime != "" {
			rangeQuery["gte"] = startTime
		}
		if endTime != "" {
			rangeQuery["lte"] = endTime
		}
		queryClause = map[string]interface{}{
			"range": map[string]interface{}{
				"created_at": rangeQuery,
			},
		}
	} else {
		queryClause = map[string]interface{}{
			"match_all": map[string]interface{}{},
		}
	}

	query := map[string]interface{}{
		"query": queryClause,
		"sort": []interface{}{
			map[string]interface{}{"created_at": "asc"},
			map[string]interface{}{"indexed_at": "asc"},
		},
		"size": size,
	}

	if afterCreatedAt != "" && afterIndexedAt != "" {
		query["search_after"] = []interface{}{afterCreatedAt, afterIndexedAt}
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return response, fmt.Errorf("failed to marshal query: %w", err)
	}

	logger.Debug("Executing search query on index '%s': %s", index, string(queryJSON))

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	if err != nil {
		return response, fmt.Errorf("search request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close search response body: %v", err)
		}
	}()

	if res.IsError() {
		return response, fmt.Errorf("search request returned error: %s", res.String())
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("failed to parse search response: %w", err)
	}

	logger.Debug("Search returned %d hits (total: %d)", len(response.Hits.Hits), response.Hits.Total.Value)

	return response, nil
}

// FetchLikes queries Elasticsearch for likes with pagination using search_after
// Parameters mirror FetchPosts but return LikeSearchResponse
func FetchLikes(ctx context.Context, client *elasticsearch.Client, logger *IngestLogger, index string, startTime string, endTime string, afterCreatedAt string, afterIndexedAt string, size int) (LikeSearchResponse, error) {
	var response LikeSearchResponse

	if size <= 0 {
		size = 1000
	}

	// Build query based on whether time range is specified
	var queryClause map[string]interface{}
	if startTime != "" || endTime != "" {
		rangeQuery := map[string]interface{}{}
		if startTime != "" {
			rangeQuery["gte"] = startTime
		}
		if endTime != "" {
			rangeQuery["lte"] = endTime
		}
		queryClause = map[string]interface{}{
			"range": map[string]interface{}{
				"created_at": rangeQuery,
			},
		}
	} else {
		queryClause = map[string]interface{}{
			"match_all": map[string]interface{}{},
		}
	}

	query := map[string]interface{}{
		"query": queryClause,
		"sort": []interface{}{
			map[string]interface{}{"created_at": "asc"},
			map[string]interface{}{"indexed_at": "asc"},
		},
		"size": size,
	}

	if afterCreatedAt != "" && afterIndexedAt != "" {
		query["search_after"] = []interface{}{afterCreatedAt, afterIndexedAt}
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return response, fmt.Errorf("failed to marshal query: %w", err)
	}

	logger.Debug("Executing like search query on index '%s': %s", index, string(queryJSON))

	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	if err != nil {
		return response, fmt.Errorf("like search request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close like search response body: %v", err)
		}
	}()

	if res.IsError() {
		return response, fmt.Errorf("like search request returned error: %s", res.String())
	}

	if err := json.NewDecoder(res.Body).Decode(&response); err != nil {
		return response, fmt.Errorf("failed to parse like search response: %w", err)
	}

	logger.Debug("Like search returned %d hits (total: %d)", len(response.Hits.Hits), response.Hits.Total.Value)

	return response, nil
}

// QueryPostsByAuthorDID retrieves all post at_uris for a given author_did using scroll API
func QueryPostsByAuthorDID(ctx context.Context, client *elasticsearch.Client, index string, authorDID string, logger *IngestLogger) ([]string, error) {
	// Build search query
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"author_did": authorDID,
			},
		},
		"_source": []string{"at_uri"},
		"size":    1000,
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// Initial scroll request with routing
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
		client.Search.WithScroll(time.Minute*5),
		client.Search.WithRouting(authorDID),
	)
	if err != nil {
		return nil, fmt.Errorf("initial scroll search failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return nil, fmt.Errorf("scroll search returned error: %s", res.String())
	}

	var searchResponse struct {
		ScrollID string `json:"_scroll_id"`
		Hits     struct {
			Hits []struct {
				Source struct {
					AtURI string `json:"at_uri"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	// Collect all at_uris
	var atURIs []string
	for _, hit := range searchResponse.Hits.Hits {
		if hit.Source.AtURI != "" {
			atURIs = append(atURIs, hit.Source.AtURI)
		}
	}

	scrollID := searchResponse.ScrollID
	count := len(atURIs)

	// Continue scrolling until no more results
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Get next batch
		scrollRes, err := client.Scroll(
			client.Scroll.WithContext(ctx),
			client.Scroll.WithScrollID(scrollID),
			client.Scroll.WithScroll(time.Minute*5),
		)
		if err != nil {
			return nil, fmt.Errorf("scroll request failed: %w", err)
		}

		if scrollRes.IsError() {
			_ = scrollRes.Body.Close()
			return nil, fmt.Errorf("scroll request returned error: %s", scrollRes.String())
		}

		var scrollResponse struct {
			ScrollID string `json:"_scroll_id"`
			Hits     struct {
				Hits []struct {
					Source struct {
						AtURI string `json:"at_uri"`
					} `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}

		if err := json.NewDecoder(scrollRes.Body).Decode(&scrollResponse); err != nil {
			_ = scrollRes.Body.Close()
			return nil, fmt.Errorf("failed to parse scroll response: %w", err)
		}
		_ = scrollRes.Body.Close()

		// No more results
		if len(scrollResponse.Hits.Hits) == 0 {
			break
		}

		// Collect at_uris from this batch
		for _, hit := range scrollResponse.Hits.Hits {
			if hit.Source.AtURI != "" {
				atURIs = append(atURIs, hit.Source.AtURI)
			}
		}

		scrollID = scrollResponse.ScrollID
		count += len(scrollResponse.Hits.Hits)

		// Log progress every 1000 documents
		if count%1000 == 0 {
			logger.Info("QueryPostsByAuthorDID progress: %d posts found for DID %s", count, authorDID)
		}
	}

	// Clear scroll context
	_, _ = client.ClearScroll(client.ClearScroll.WithScrollID(scrollID))

	logger.Info("QueryPostsByAuthorDID complete: found %d posts for DID %s", len(atURIs), authorDID)
	return atURIs, nil
}

// QueryLikesByAuthorDID retrieves all likes for a given author_did using scroll API
// Returns map of at_uri -> subject_uri (subject_uri needed for tombstone creation)
func QueryLikesByAuthorDID(ctx context.Context, client *elasticsearch.Client, index string, authorDID string, logger *IngestLogger) (map[string]string, error) {
	// Build search query
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"author_did": authorDID,
			},
		},
		"_source": []string{"at_uri", "subject_uri"},
		"size":    1000,
	}

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// Initial scroll request with routing
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
		client.Search.WithScroll(time.Minute*5),
		client.Search.WithRouting(authorDID),
	)
	if err != nil {
		return nil, fmt.Errorf("initial scroll search failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return nil, fmt.Errorf("scroll search returned error: %s", res.String())
	}

	var searchResponse struct {
		ScrollID string `json:"_scroll_id"`
		Hits     struct {
			Hits []struct {
				Source struct {
					AtURI      string `json:"at_uri"`
					SubjectURI string `json:"subject_uri"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	// Collect all likes
	likes := make(map[string]string)
	for _, hit := range searchResponse.Hits.Hits {
		if hit.Source.AtURI != "" && hit.Source.SubjectURI != "" {
			likes[hit.Source.AtURI] = hit.Source.SubjectURI
		}
	}

	scrollID := searchResponse.ScrollID
	count := len(likes)

	// Continue scrolling until no more results
	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Get next batch
		scrollRes, err := client.Scroll(
			client.Scroll.WithContext(ctx),
			client.Scroll.WithScrollID(scrollID),
			client.Scroll.WithScroll(time.Minute*5),
		)
		if err != nil {
			return nil, fmt.Errorf("scroll request failed: %w", err)
		}

		if scrollRes.IsError() {
			_ = scrollRes.Body.Close()
			return nil, fmt.Errorf("scroll request returned error: %s", scrollRes.String())
		}

		var scrollResponse struct {
			ScrollID string `json:"_scroll_id"`
			Hits     struct {
				Hits []struct {
					Source struct {
						AtURI      string `json:"at_uri"`
						SubjectURI string `json:"subject_uri"`
					} `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		}

		if err := json.NewDecoder(scrollRes.Body).Decode(&scrollResponse); err != nil {
			_ = scrollRes.Body.Close()
			return nil, fmt.Errorf("failed to parse scroll response: %w", err)
		}
		_ = scrollRes.Body.Close()

		// No more results
		if len(scrollResponse.Hits.Hits) == 0 {
			break
		}

		// Collect likes from this batch
		for _, hit := range scrollResponse.Hits.Hits {
			if hit.Source.AtURI != "" && hit.Source.SubjectURI != "" {
				likes[hit.Source.AtURI] = hit.Source.SubjectURI
			}
		}

		scrollID = scrollResponse.ScrollID
		count += len(scrollResponse.Hits.Hits)

		// Log progress every 1000 documents
		if count%1000 == 0 {
			logger.Info("QueryLikesByAuthorDID progress: %d likes found for DID %s", count, authorDID)
		}
	}

	// Clear scroll context
	_, _ = client.ClearScroll(client.ClearScroll.WithScrollID(scrollID))

	logger.Info("QueryLikesByAuthorDID complete: found %d likes for DID %s", len(likes), authorDID)
	return likes, nil
}
