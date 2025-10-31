package common

import (
	"context"
	"io"
)

// TODO: Abstract WebSocketClient to a general DataSource interface
// The new DataSource interface should support multiple implementations:
// - WebSocketDataSource (real-time streams)
// - LocalSQLiteDataSource (local file ingestion)
// - S3SQLiteDataSource (remote S3-hosted files)

// WebSocketClient defines the interface for WebSocket connections
type WebSocketClient interface {
	// Connect establishes a WebSocket connection to the given URL
	Connect(ctx context.Context, url string) error

	// ReadMessage reads the next message from the WebSocket connection
	ReadMessage(ctx context.Context) ([]byte, error)

	// Close closes the WebSocket connection
	Close() error
}

// ElasticsearchClient defines the interface for Elasticsearch operations
type ElasticsearchClient interface {
	// IndexDocument indexes a document in the specified index
	IndexDocument(ctx context.Context, index string, document interface{}) error

	// BulkIndex performs bulk indexing of multiple documents
	BulkIndex(ctx context.Context, index string, documents []interface{}) error

	// Close closes the Elasticsearch client connection
	Close() error
}

// Logger defines the interface for logging operations
type Logger interface {
	// Info logs an informational message
	Info(msg string, args ...interface{})

	// Error logs an error message
	Error(msg string, args ...interface{})

	// Debug logs a debug message
	Debug(msg string, args ...interface{})

	// SetOutput sets the output destination for logs
	SetOutput(w io.Writer)
}

// Message represents a processed BlueSky message
type Message struct {
	ID        string                 `json:"id"`
	Type      string                 `json:"type"`
	Data      map[string]interface{} `json:"data"`
	Timestamp int64                  `json:"timestamp"`
}
