package jetstream_ingest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/greenearth/ingest/internal/common"
)

// Client represents a Jetstream WebSocket client
type Client struct {
	url       string
	cursor    *int64 // Optional cursor for rewinding to specific timestamp
	conn      *websocket.Conn
	msgChan   chan string
	logger    *common.IngestLogger
	reconnect bool
	mu        sync.RWMutex // Protects conn and reconnect fields
}

// NewClient creates a new Jetstream WebSocket client
func NewClient(url string, logger *common.IngestLogger) *Client {
	return &Client{
		url:       url,
		msgChan:   make(chan string, 10000), // Buffer for 10000 messages
		logger:    logger,
		reconnect: true,
	}
}

// SetCursor sets the cursor for rewinding to a specific timestamp
func (c *Client) SetCursor(timeUs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor = &timeUs
}

// Connect establishes a WebSocket connection to Jetstream
func (c *Client) Connect(ctx context.Context) error {
	url := c.url

	// Read cursor under lock since it may be updated by UpdateCursor
	c.mu.RLock()
	cursor := c.cursor
	c.mu.RUnlock()

	// Add cursor parameter if set
	if cursor != nil {
		url = fmt.Sprintf("%s?cursor=%d", c.url, *cursor)
		c.logger.Info("Connecting to Jetstream at %s with cursor (rewinding to timestamp %d)", c.url, *cursor)
	} else {
		c.logger.Info("Connecting to Jetstream at %s", c.url)
	}

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 30 * time.Second

	conn, resp, err := dialer.DialContext(ctx, url, nil)
	if resp != nil && resp.Body != nil {
		// Close the body on the HTTP upgrade response
		if closeErr := resp.Body.Close(); closeErr != nil {
			c.logger.Error("Failed to close HTTP response body: %v", closeErr)
		}
	}
	if err != nil {
		return fmt.Errorf("failed to connect to Jetstream: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()
	c.logger.Info("Successfully connected to Jetstream")

	return nil
}

// Start begins reading messages from the WebSocket connection
func (c *Client) Start(ctx context.Context) error {
	if err := c.Connect(ctx); err != nil {
		return err
	}

	go c.readLoop(ctx)

	return nil
}

// readLoop continuously reads messages from the WebSocket connection
func (c *Client) readLoop(ctx context.Context) {
	defer close(c.msgChan)

	// Close the active connection when ctx is cancelled so ReadMessage unblocks.
	go func() {
		<-ctx.Done()
		c.mu.Lock()
		c.reconnect = false
		if c.conn != nil {
			if err := c.conn.Close(); err != nil {
				c.logger.Error("Failed to close WebSocket connection on shutdown: %v", err)
			}
		}
		c.mu.Unlock()
	}()

	for {
		c.mu.RLock()
		conn := c.conn
		shouldReconnect := c.reconnect
		c.mu.RUnlock()

		if conn == nil {
			if !shouldReconnect {
				return
			}
			c.logger.Info("Attempting to reconnect...")
			if err := c.Connect(ctx); err != nil {
				c.logger.Error("Reconnection failed: %v, retrying in 5 seconds", err)
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
				continue
			}
			c.mu.RLock()
			conn = c.conn
			c.mu.RUnlock()
		}

		_, message, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return // ctx cancelled — the shutdown goroutine closed the conn
			}
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.logger.Info("WebSocket connection closed normally")
			} else {
				c.logger.Error("Error reading from WebSocket: %v", err)
			}
			c.mu.Lock()
			c.conn = nil
			shouldReconnect = c.reconnect
			c.mu.Unlock()
			if shouldReconnect {
				c.logger.Info("Reconnecting in 5 seconds...")
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				}
			}
			continue
		}

		select {
		case c.msgChan <- string(message):
		case <-time.After(5 * time.Second):
			c.logger.Error("Message channel full for 5 seconds, dropping message")
		case <-ctx.Done():
			return
		}
	}
}

// UpdateCursor updates the cursor used for reconnections to the latest processed timestamp.
// This should be called periodically as messages are processed to avoid replaying
// stale data on WebSocket reconnection.
func (c *Client) UpdateCursor(timeUs int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cursor = &timeUs
}

// GetMessageChannel returns the channel that receives raw JSON messages
func (c *Client) GetMessageChannel() <-chan string {
	return c.msgChan
}

// Close closes the WebSocket connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reconnect = false
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
