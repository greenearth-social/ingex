package main

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

// ElasticsearchDoc represents the document structure for indexing
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

// TombstoneDoc represents the document structure for post deletion tombstones
type TombstoneDoc struct {
	AtURI     string `json:"at_uri"`
	AuthorDID string `json:"author_did"`
	DeletedAt string `json:"deleted_at"`
	IndexedAt string `json:"indexed_at"`
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

// bulkIndex indexes a batch of documents to Elasticsearch
func bulkIndex(ctx context.Context, client *elasticsearch.Client, index string, docs []ElasticsearchDoc, dryRun bool, logger *IngestLogger) error {
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

// bulkIndexTombstones indexes a batch of tombstone documents to Elasticsearch
func bulkIndexTombstones(ctx context.Context, client *elasticsearch.Client, index string, docs []TombstoneDoc, dryRun bool, logger *IngestLogger) error {
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
	defer res.Body.Close()

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

// bulkDelete deletes a batch of documents from Elasticsearch by their IDs
func bulkDelete(ctx context.Context, client *elasticsearch.Client, index string, docIDs []string, dryRun bool, logger *IngestLogger) error {
	if len(docIDs) == 0 {
		return nil
	}

	if dryRun {
		logger.Debug("Dry-run: Skipping bulk delete of %d documents from index '%s'", len(docIDs), index)
		return nil
	}

	var buf bytes.Buffer
	validDocCount := 0

	for _, docID := range docIDs {
		if docID == "" {
			logger.Error("Skipping delete with empty document ID")
			continue
		}

		meta := map[string]interface{}{
			"delete": map[string]interface{}{
				"_index": index,
				"_id":    docID,
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
	defer res.Body.Close()

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
	return ElasticsearchDoc{
		AtURI:            msg.GetAtURI(),
		AuthorDID:        msg.GetAuthorDID(),
		Content:          msg.GetContent(),
		CreatedAt:        msg.GetCreatedAt(),
		ThreadRootPost:   msg.GetThreadRootPost(),
		ThreadParentPost: msg.GetThreadParentPost(),
		QuotePost:        msg.GetQuotePost(),
		Embeddings:       msg.GetEmbeddings(),
		IndexedAt:        time.Now().UTC().Format(time.RFC3339),
	}
}

// CreateTombstoneDoc creates a TombstoneDoc from a MegaStreamMessage
func CreateTombstoneDoc(msg MegaStreamMessage) TombstoneDoc {
	now := time.Now().UTC()
	deletedAt := now

	if timeUs := msg.GetTimeUs(); timeUs > 0 {
		deletedAt = time.Unix(0, timeUs*1000)
	}

	return TombstoneDoc{
		AtURI:     msg.GetAtURI(),
		AuthorDID: msg.GetAuthorDID(),
		DeletedAt: deletedAt.Format(time.RFC3339),
		IndexedAt: now.Format(time.RFC3339),
	}
}
