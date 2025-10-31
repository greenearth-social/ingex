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
	conn      *websocket.Conn
	msgChan   chan string
	logger    *common.IngestLogger
	reconnect bool
}

// NewClient creates a new Jetstream WebSocket client
func NewClient(url string, logger *common.IngestLogger) *Client {
	return &Client{
		url:       url,
		msgChan:   make(chan string, 1000), // Buffer for 1000 messages
		logger:    logger,
		reconnect: true,
	}
}

// Connect establishes a WebSocket connection to Jetstream
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Info("Connecting to Jetstream at %s", c.url)

	dialer := websocket.DefaultDialer
	dialer.HandshakeTimeout = 30 * time.Second

	conn, _, err := dialer.DialContext(ctx, c.url, nil)
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

			// Send message to channel (non-blocking)
			select {
			case c.msgChan <- string(message):
			default:
				c.logger.Error("Message channel full, dropping message")
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
