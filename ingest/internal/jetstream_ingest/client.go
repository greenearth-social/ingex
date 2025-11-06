package jetstream_ingest

import (
	"context"
	"fmt"
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
	c.cursor = &timeUs
}

// Connect establishes a WebSocket connection to Jetstream
func (c *Client) Connect(ctx context.Context) error {
	url := c.url

	// Add cursor parameter if set
	if c.cursor != nil {
		url = fmt.Sprintf("%s?cursor=%d", c.url, *c.cursor)
		c.logger.Info("Connecting to Jetstream at %s with cursor (rewinding to timestamp %d)", c.url, *c.cursor)
	} else {
		c.logger.Info("Connecting to Jetstream at %s", c.url)
	}

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 30 * time.Second

	conn, resp, err := dialer.DialContext(ctx, url, nil)
	if resp != nil && resp.Body != nil {
		// Close the body on the HTTP upgrade response
		resp.Body.Close()
	}
	if err != nil {
		return fmt.Errorf("failed to connect to Jetstream: %w", err)
	}

	c.conn = conn
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

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context cancelled, closing WebSocket connection")
			c.reconnect = false
			if c.conn != nil {
				c.conn.Close()
			}
			return
		default:
			if c.conn == nil {
				if !c.reconnect {
					return
				}
				c.logger.Info("Attempting to reconnect...")
				if err := c.Connect(ctx); err != nil {
					c.logger.Error("Reconnection failed: %v, retrying in 5 seconds", err)
					time.Sleep(5 * time.Second)
					continue
				}
			}

			// Set read deadline to allow periodic context checking
			c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))

			_, message, err := c.conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					c.logger.Info("WebSocket connection closed normally")
					c.conn = nil
					if c.reconnect {
						c.logger.Info("Reconnecting in 5 seconds...")
						time.Sleep(5 * time.Second)
						continue
					}
					return
				}

				// Check if it's a timeout (which is expected due to read deadline)
				if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
					continue
				}

				c.logger.Error("Error reading from WebSocket: %v", err)
				c.conn = nil
				if c.reconnect {
					c.logger.Info("Reconnecting in 5 seconds...")
					time.Sleep(5 * time.Second)
					continue
				}
				return
			}

			// Send message to channel with blocking and timeout
			// This applies backpressure when the consumer is slower than the producer
			select {
			case c.msgChan <- string(message):
				// Message sent successfully
			case <-time.After(5 * time.Second):
				// If we can't send within 5 seconds, log and drop
				c.logger.Error("Message channel full for 5 seconds, dropping message")
			case <-ctx.Done():
				// Context cancelled while trying to send
				return
			}
		}
	}
}

// GetMessageChannel returns the channel that receives raw JSON messages
func (c *Client) GetMessageChannel() <-chan string {
	return c.msgChan
}

// Close closes the WebSocket connection
func (c *Client) Close() error {
	c.reconnect = false
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}
