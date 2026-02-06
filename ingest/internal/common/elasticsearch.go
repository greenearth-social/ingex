package common

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
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

// MediaItem represents an embedded media item (image or video) in a post
type MediaItem struct {
	ID          string  `json:"id"`
	MediaType   string  `json:"media_type"`
	MimeType    string  `json:"mime_type"`
	Size        int64   `json:"size"`
	AspectRatio float64 `json:"aspect_ratio"`
	Width       int     `json:"width"`
	Height      int     `json:"height"`
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
	LikeCount        int                     `json:"like_count"`
	Media            []MediaItem             `json:"media,omitempty"`
	ContainsImages   bool                    `json:"contains_images"`
	ContainsVideo    bool                    `json:"contains_video"`
	ImageCount       int                     `json:"image_count"`
	VideoCount       int                     `json:"video_count"`
	MediaCount       int                     `json:"media_count"`
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

// HashtagUpdate represents a hashtag count update for a specific hour
type HashtagUpdate struct {
	Hashtag string
	Hour    string // ISO8601 timestamp truncated to hour
	Count   int    // Amount to increment by
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

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.bulk_index_posts.duration_ms", float64(time.Since(start).Milliseconds()))
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
		Took   int  `json:"took"`
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

	logger.Metric("es.bulk_index_posts.took_ms", float64(bulkResponse.Took))

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

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.bulk_index_tombstones.duration_ms", float64(time.Since(start).Milliseconds()))
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
		Took   int  `json:"took"`
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

	logger.Metric("es.bulk_index_tombstones.took_ms", float64(bulkResponse.Took))

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

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.bulk_delete.duration_ms", float64(time.Since(start).Milliseconds()))
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
		Took   int  `json:"took"`
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

	logger.Metric("es.bulk_delete.took_ms", float64(bulkResponse.Took))

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
func CreateElasticsearchDoc(msg MegaStreamMessage, likeCount int) ElasticsearchDoc {
	// Convert embeddings to Float32Array type for proper JSON marshaling
	var embeddings map[string]Float32Array
	rawEmbeddings := msg.GetEmbeddings()
	if rawEmbeddings != nil {
		embeddings = make(map[string]Float32Array, len(rawEmbeddings))
		for key, value := range rawEmbeddings {
			embeddings[key] = Float32Array(value)
		}
	}

	// Extract media and compute summary fields
	media := msg.GetMedia()
	var imageCount, videoCount int
	for _, item := range media {
		switch item.MediaType {
		case "image":
			imageCount++
		case "video":
			videoCount++
		}
	}
	mediaCount := len(media)
	containsImages := imageCount > 0
	containsVideo := videoCount > 0

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
		LikeCount:        likeCount,
		Media:            media,
		ContainsImages:   containsImages,
		ContainsVideo:    containsVideo,
		ImageCount:       imageCount,
		VideoCount:       videoCount,
		MediaCount:       mediaCount,
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

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.bulk_index_likes.duration_ms", float64(time.Since(start).Milliseconds()))
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
		Took   int  `json:"took"`
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

	logger.Metric("es.bulk_index_likes.took_ms", float64(bulkResponse.Took))

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
	start := time.Now()
	res, err := client.Mget(
		bytes.NewReader(bodyJSON),
		client.Mget.WithContext(ctx),
	)
	logger.Metric("es.bulk_get_likes.duration_ms", float64(time.Since(start).Milliseconds()))
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

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.bulk_index_like_tombstones.duration_ms", float64(time.Since(start).Milliseconds()))
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
		Took   int  `json:"took"`
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

	logger.Metric("es.bulk_index_like_tombstones.took_ms", float64(bulkResponse.Took))

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
	Media            []MediaItem          `json:"media,omitempty"`
	ContainsImages   bool                 `json:"contains_images"`
	ContainsVideo    bool                 `json:"contains_video"`
	ImageCount       int                  `json:"image_count"`
	VideoCount       int                  `json:"video_count"`
	MediaCount       int                  `json:"media_count"`
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

// HashtagHit represents a hashtag search hit from Elasticsearch
type HashtagHit struct {
	ID     string        `json:"_id"`
	Sort   []interface{} `json:"sort,omitempty"`
	Source HashtagSource `json:"_source"`
}

// HashtagSource represents the _source field of a Hashtag document in Elasticsearch
type HashtagSource struct {
	Hashtag string `json:"hashtag"`
	Hour    string `json:"hour"`
	Count   int    `json:"count"`
}

// HashtagHits contains the hashtag search results
type HashtagHits struct {
	Total    TotalHits    `json:"total"`
	MaxScore float64      `json:"max_score"`
	Hits     []HashtagHit `json:"hits"`
}

// HashtagSearchResponse represents the response from an Elasticsearch hashtag search query
type HashtagSearchResponse struct {
	Took     int         `json:"took"`
	TimedOut bool        `json:"timed_out"`
	Shards   ShardsInfo  `json:"_shards"`
	Hits     HashtagHits `json:"hits"`
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

	start := time.Now()
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	logger.Metric("es.fetch_posts.duration_ms", float64(time.Since(start).Milliseconds()))
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

	logger.Metric("es.fetch_posts.took_ms", float64(response.Took))
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

	start := time.Now()
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	logger.Metric("es.fetch_likes.duration_ms", float64(time.Since(start).Milliseconds()))
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

	logger.Metric("es.fetch_likes.took_ms", float64(response.Took))
	logger.Debug("Like search returned %d hits (total: %d)", len(response.Hits.Hits), response.Hits.Total.Value)

	return response, nil
}

// BulkGetPosts fetches multiple post documents from Elasticsearch by at_uri
// Returns a map of at_uri -> author_did for routing purposes
// Note: Uses search API instead of mget because mget requires routing
func BulkGetPosts(ctx context.Context, client *elasticsearch.Client, index string, atURIs []string, logger *IngestLogger) (map[string]string, error) {
	if len(atURIs) == 0 {
		return make(map[string]string), nil
	}

	// Filter empty URIs
	validURIs := make([]string, 0, len(atURIs))
	for _, uri := range atURIs {
		if uri != "" {
			validURIs = append(validURIs, uri)
		}
	}

	if len(validURIs) == 0 {
		return make(map[string]string), nil
	}

	// Build search request with terms query on _id field
	searchBody := map[string]interface{}{
		"query": map[string]interface{}{
			"terms": map[string]interface{}{
				"_id": validURIs, // Query by document IDs (at_uri values)
			},
		},
		"_source": []string{"author_did"}, // Only fetch author_did field
		"size":    len(validURIs),         // Return up to batch size (100)
	}

	bodyJSON, err := json.Marshal(searchBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal search request: %w", err)
	}

	// Execute search request
	start := time.Now()
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(index),
		client.Search.WithBody(bytes.NewReader(bodyJSON)),
	)
	logger.Metric("es.bulk_get_posts.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		return nil, fmt.Errorf("search request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close search response body: %v", err)
		}
	}()

	if res.IsError() {
		return nil, fmt.Errorf("search request returned error: %s", res.String())
	}

	// Parse search response
	var searchResponse struct {
		Hits struct {
			Hits []struct {
				ID     string `json:"_id"`
				Source struct {
					AuthorDID string `json:"author_did"`
				} `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}

	if err := json.NewDecoder(res.Body).Decode(&searchResponse); err != nil {
		return nil, fmt.Errorf("failed to parse search response: %w", err)
	}

	// Build result map: at_uri -> author_did
	result := make(map[string]string)
	for _, hit := range searchResponse.Hits.Hits {
		if hit.Source.AuthorDID != "" {
			result[hit.ID] = hit.Source.AuthorDID
		}
	}

	return result, nil
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

// LikeCountUpdate represents a like count change for a post
type LikeCountUpdate struct {
	SubjectURI string
	Increment  int // Positive for like creation, negative for deletion
}

// aggregateLikeCountUpdates aggregates multiple updates to the same post
// Returns a map of subject_uri -> total increment
func aggregateLikeCountUpdates(updates []LikeCountUpdate) map[string]int {
	aggregated := make(map[string]int)
	for _, update := range updates {
		if update.SubjectURI != "" {
			aggregated[update.SubjectURI] += update.Increment
		}
	}
	return aggregated
}

// BulkUpdatePostLikeCounts updates like_count fields on posts using the ES update API
// Uses cache to reduce ES lookups for routing info (author_did)
// cache parameter is optional (nil-safe) - if nil, all lookups go to ES
func BulkUpdatePostLikeCounts(ctx context.Context, client *elasticsearch.Client, index string, updates []LikeCountUpdate, cache *PostRoutingCache, dryRun bool, logger *IngestLogger) error {
	if len(updates) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk update of %d post like counts", len(updates))
		return nil
	}

	// Aggregate updates by subject_uri (in case same post appears multiple times)
	aggregated := aggregateLikeCountUpdates(updates)

	// Extract unique subject URIs for routing lookup
	subjectURIs := make([]string, 0, len(aggregated))
	for uri := range aggregated {
		subjectURIs = append(subjectURIs, uri)
	}

	if len(subjectURIs) == 0 {
		return fmt.Errorf("no valid updates in batch")
	}

	// Check cache first for routing info, then fetch remaining from ES
	var routingMap map[string]string
	var missingURIs []string
	if cache != nil {
		routingMap, missingURIs = cache.BulkGet(subjectURIs)
	} else {
		routingMap = make(map[string]string)
		missingURIs = subjectURIs
	}

	// Log cache hit rate
	if len(subjectURIs) > 0 {
		hitRate := float64(len(routingMap)) / float64(len(subjectURIs))
		logger.Metric("cache.post_routing.hit_rate", hitRate)
	}

	// Only query ES for cache misses
	if len(missingURIs) > 0 {
		esResults, err := BulkGetPosts(ctx, client, index, missingURIs, logger)
		if err != nil {
			return fmt.Errorf("failed to fetch posts for routing: %w", err)
		}
		// Merge ES results into routing map and populate cache
		for uri, did := range esResults {
			routingMap[uri] = did
		}
		if cache != nil {
			cache.BulkAdd(esResults)
		}
	}

	var buf bytes.Buffer
	validUpdateCount := 0
	skippedNoRouting := 0

	for subjectURI, increment := range aggregated {
		// Get routing value (author_did) for this post
		authorDID, found := routingMap[subjectURI]
		if !found || authorDID == "" {
			// Post doesn't exist or wasn't found - skip update
			// This is expected for likes that arrive before posts are indexed
			skippedNoRouting++
			continue
		}
		validUpdateCount++

		// Elasticsearch update action metadata with shard routing
		meta := map[string]interface{}{
			"update": map[string]interface{}{
				"_index":  index,
				"_id":     subjectURI,
				"routing": authorDID,
			},
		}

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal update metadata: %w", err)
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')

		// Update body with painless script
		updateBody := map[string]interface{}{
			"script": map[string]interface{}{
				"source": "if (ctx._source.like_count == null) { ctx._source.like_count = 0; } ctx._source.like_count = ctx._source.like_count + params.increment;",
				"params": map[string]interface{}{
					"increment": increment,
				},
				"lang": "painless",
			},
			"_source": true, // Return the updated document
		}

		updateJSON, err := json.Marshal(updateBody)
		if err != nil {
			return fmt.Errorf("failed to marshal update body: %w", err)
		}

		buf.Write(updateJSON)
		buf.WriteByte('\n')
	}

	if validUpdateCount == 0 {
		logger.Debug("No like-count updates to perform (no corresponding posts found)")
		return nil
	}
	// Log if we skipped some updates due to missing posts
	if skippedNoRouting > 0 {
		logger.Debug("Skipped %d post like-count updates while looking for routing info due to missing posts", skippedNoRouting)
	}

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.update_like_counts.duration_ms", float64(time.Since(start).Milliseconds()))
	if err != nil {
		return fmt.Errorf("bulk update request failed: %w", err)
	}
	defer func() {
		if err := res.Body.Close(); err != nil {
			logger.Error("Failed to close response body: %v", err)
		}
	}()

	if res.IsError() {
		return fmt.Errorf("bulk update request returned error: %s", res.String())
	}

	var bulkResponse struct {
		Took   int  `json:"took"`
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("failed to parse bulk update response: %w", err)
	}

	logger.Metric("es.update_like_counts.took_ms", float64(bulkResponse.Took))

	if bulkResponse.Errors {
		hasRealErrors := false
		notFoundCount := 0

		for _, item := range bulkResponse.Items {
			for _, details := range item {
				if details.Error != nil {
					// It's possible (though unlikely) a post is deleted
					// before we increment likes. Ignore those race situations.
					if details.Status == 404 {
						notFoundCount++
					} else {
						hasRealErrors = true
						logger.Error("Update error (status %d): %s - %s",
							details.Status, details.Error.Type, details.Error.Reason)
					}
				}
			}
		}

		if notFoundCount > 0 {
			logger.Debug("Skipped %d like-count updates due to missing posts", notFoundCount)
		}

		if hasRealErrors {
			itemsJSON, _ := json.Marshal(bulkResponse.Items)
			logger.Error("Bulk like-count update failed with errors")
			logger.Debug("Response items with errors: %s", string(itemsJSON))
			return fmt.Errorf("bulk update failed: some updates had errors")
		}
	}

	logger.Debug("Successfully updated like counts for %d posts", validUpdateCount)
	return nil
}

// ExtractHashtags extracts hashtags from post content and returns them with hour bucket and count
// The hour is derived from the post's createdAt timestamp, truncated to the hour
func ExtractHashtags(content, createdAt string) []HashtagUpdate {
	if content == "" {
		return nil
	}

	// Parse the created_at timestamp to determine the hour bucket
	var hour string
	if t, err := time.Parse(time.RFC3339, createdAt); err == nil {
		// Truncate to hour
		hour = t.Truncate(time.Hour).Format(time.RFC3339)
	} else {
		// Fallback to current hour if parsing fails (shouldn't happen)
		hour = time.Now().UTC().Truncate(time.Hour).Format(time.RFC3339)
	}

	// Extract unique hashtags from content
	hashtags := make(map[string]bool)
	words := []rune(content)
	inHashtag := false
	var currentTag []rune

	for i := 0; i < len(words); i++ {
		char := words[i]

		if char == '#' {
			// Start of a hashtag
			if len(currentTag) > 0 {
				// Save previous hashtag
				tag := string(currentTag)
				if len(tag) > 0 {
					hashtags[tag] = true
				}
				currentTag = currentTag[:0]
			}
			inHashtag = true
		} else if inHashtag {
			// Continue hashtag if alphanumeric or underscore
			if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
				(char >= '0' && char <= '9') || char == '_' {
				currentTag = append(currentTag, char)
			} else {
				// End of hashtag
				if len(currentTag) > 0 {
					tag := strings.ToLower(string(currentTag))
					hashtags[tag] = true
				}
				currentTag = currentTag[:0]
				inHashtag = false
			}
		}
	}

	// Don't forget the last hashtag if content ends with one
	if len(currentTag) > 0 {
		tag := strings.ToLower(string(currentTag))
		if len(tag) > 0 {
			hashtags[tag] = true
		}
	}

	// Convert to updates (already lowercase from extraction)
	updates := make([]HashtagUpdate, 0, len(hashtags))
	for tag := range hashtags {
		updates = append(updates, HashtagUpdate{
			Hashtag: tag,
			Hour:    hour,
			Count:   1, // Each post counts as 1 for each unique hashtag
		})
	}

	return updates
}

// BulkUpdateHashtagCounts updates hashtag counts in Elasticsearch using the _update API with scripted upserts
// This increments the count for each hashtag-hour combination
func BulkUpdateHashtagCounts(ctx context.Context, client *elasticsearch.Client, index string, updates []HashtagUpdate, dryRun bool, logger *IngestLogger) error {
	if len(updates) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk update of %d hashtag counts to index '%s'", len(updates), index)
		return nil
	}

	var buf bytes.Buffer
	validUpdateCount := 0

	for _, update := range updates {
		if update.Hashtag == "" || update.Hour == "" {
			logger.Error("Skipping hashtag update with empty hashtag or hour")
			continue
		}

		// Create a document ID that combines hashtag and hour for uniqueness
		// Use lowercase for case-insensitive counting
		docID := fmt.Sprintf("%s_%s", update.Hashtag, update.Hour)

		meta := map[string]interface{}{
			"update": map[string]interface{}{
				"_index": index,
				"_id":    docID,
			},
		}

		validUpdateCount++

		metaJSON, err := json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("failed to marshal metadata: %w", err)
		}

		buf.Write(metaJSON)
		buf.WriteByte('\n')

		// Use scripted upsert to increment count or create new document
		updateDoc := map[string]interface{}{
			"script": map[string]interface{}{
				"source": "ctx._source.count += params.increment",
				"params": map[string]interface{}{
					"increment": update.Count,
				},
				"lang": "painless",
			},
			"upsert": map[string]interface{}{
				"hashtag": update.Hashtag,
				"hour":    update.Hour,
				"count":   update.Count,
			},
			"scripted_upsert": true,
		}

		updateJSON, err := json.Marshal(updateDoc)
		if err != nil {
			return fmt.Errorf("failed to marshal update document: %w", err)
		}

		buf.Write(updateJSON)
		buf.WriteByte('\n')
	}

	if validUpdateCount == 0 {
		logger.Error("No valid hashtag updates to perform")
		return fmt.Errorf("no valid updates in batch")
	}

	start := time.Now()
	res, err := client.Bulk(
		bytes.NewReader(buf.Bytes()),
		client.Bulk.WithContext(ctx),
	)
	logger.Metric("es.update_hashtags.duration_ms", float64(time.Since(start).Milliseconds()))
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
		Took   int  `json:"took"`
		Errors bool `json:"errors"`
		Items  []map[string]struct {
			Status int `json:"status"`
			Error  *struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
			} `json:"error"`
		} `json:"items"`
	}

	if err := json.NewDecoder(res.Body).Decode(&bulkResponse); err != nil {
		return fmt.Errorf("failed to parse bulk response: %w", err)
	}

	logger.Metric("es.update_hashtags.took_ms", float64(bulkResponse.Took))

	if bulkResponse.Errors {
		itemsJSON, _ := json.Marshal(bulkResponse.Items)
		logger.Error("Bulk hashtag update failed with errors. Response items: %s", string(itemsJSON))
		return fmt.Errorf("bulk hashtag update failed: some updates had errors (see logs for details)")
	}

	logger.Debug("Successfully updated %d hashtag counts", validUpdateCount)
	return nil
}

// FetchHashtags fetches hashtags from Elasticsearch within a time window
// Uses the 'hour' field for filtering since hashtags are bucketed by hour
func FetchHashtags(ctx context.Context, client *elasticsearch.Client, logger *IngestLogger,
	indexName, startTime, endTime, afterHour string, fetchSize int) (HashtagSearchResponse, error) {

	var response HashtagSearchResponse

	if fetchSize <= 0 {
		fetchSize = 1000
	}

	var query map[string]interface{}

	// Build range query for 'hour' field if time window specified
	if startTime != "" || endTime != "" {
		rangeQuery := make(map[string]interface{})
		if startTime != "" {
			rangeQuery["gte"] = startTime
		}
		if endTime != "" {
			rangeQuery["lte"] = endTime
		}

		query = map[string]interface{}{
			"query": map[string]interface{}{
				"range": map[string]interface{}{
					"hour": rangeQuery,
				},
			},
		}
	} else {
		// Fetch all if no time filter
		query = map[string]interface{}{
			"query": map[string]interface{}{
				"match_all": map[string]interface{}{},
			},
		}
	}

	// Add pagination using search_after if provided
	if afterHour != "" {
		query["search_after"] = []interface{}{afterHour}
	}

	// Sort by hour ascending for pagination
	query["sort"] = []map[string]interface{}{
		{"hour": map[string]string{"order": "asc"}},
	}
	query["size"] = fetchSize

	queryJSON, err := json.Marshal(query)
	if err != nil {
		return response, fmt.Errorf("failed to marshal query: %w", err)
	}

	logger.Debug("Executing hashtag search query on index '%s': %s", indexName, string(queryJSON))

	start := time.Now()
	res, err := client.Search(
		client.Search.WithContext(ctx),
		client.Search.WithIndex(indexName),
		client.Search.WithBody(bytes.NewReader(queryJSON)),
	)
	logger.Metric("es.fetch_hashtags.duration_ms", float64(time.Since(start).Milliseconds()))
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

	logger.Metric("es.fetch_hashtags.took_ms", float64(response.Took))
	logger.Debug("Hashtag search returned %d hits (total: %d)", len(response.Hits.Hits), response.Hits.Total.Value)

	return response, nil
}
