package jetstream_ingest

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/greenearth/ingest/internal/common"
)

// Mock WebSocket server for testing
func newMockWebSocketServer(t *testing.T, handler func(*websocket.Conn)) *httptest.Server {
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatalf("Failed to upgrade connection: %v", err)
			return
		}
		defer conn.Close()
		handler(conn)
	}))

	return server
}

func TestClientConnect(t *testing.T) {
	logger := common.NewLogger(false)

	// Create a mock WebSocket server that accepts connections
	server := newMockWebSocketServer(t, func(conn *websocket.Conn) {
		// Keep connection open
		time.Sleep(100 * time.Millisecond)
	})
	defer server.Close()

	// Convert http:// to ws://
	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	client := NewClient(wsURL, logger)
	ctx := context.Background()

	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	if client.conn == nil {
		t.Fatal("Connection not established")
	}

	client.Close()
}

func TestClientConnectFailure(t *testing.T) {
	logger := common.NewLogger(false)

	// Use an invalid URL that will fail to connect
	client := NewClient("ws://invalid.local:9999/subscribe", logger)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := client.Connect(ctx)
	if err == nil {
		t.Fatal("Expected connection to fail, but it succeeded")
	}
}

func TestClientReceiveMessages(t *testing.T) {
	logger := common.NewLogger(false)

	messages := []string{
		`{"did":"did:plc:test1","kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like"}}`,
		`{"did":"did:plc:test2","kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like"}}`,
		`{"did":"did:plc:test3","kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.like"}}`,
	}

	// Create a mock server that sends test messages
	server := newMockWebSocketServer(t, func(conn *websocket.Conn) {
		for _, msg := range messages {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				t.Logf("Failed to write message: %v", err)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		// Keep connection open briefly
		time.Sleep(100 * time.Millisecond)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(wsURL, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Collect messages from the channel
	receivedMessages := []string{}
	msgChan := client.GetMessageChannel()

	timeout := time.After(1 * time.Second)
	for i := 0; i < len(messages); i++ {
		select {
		case msg, ok := <-msgChan:
			if !ok {
				t.Fatal("Message channel closed unexpectedly")
			}
			receivedMessages = append(receivedMessages, msg)
		case <-timeout:
			t.Fatalf("Timeout waiting for messages. Received %d of %d", len(receivedMessages), len(messages))
		}
	}

	if len(receivedMessages) != len(messages) {
		t.Errorf("Expected %d messages, got %d", len(messages), len(receivedMessages))
	}

	for i, expected := range messages {
		if receivedMessages[i] != expected {
			t.Errorf("Message %d mismatch.\nExpected: %s\nGot: %s", i, expected, receivedMessages[i])
		}
	}

	client.Close()
}

func TestClientGracefulShutdown(t *testing.T) {
	logger := common.NewLogger(false)

	// Create a mock server that keeps sending messages
	server := newMockWebSocketServer(t, func(conn *websocket.Conn) {
		for i := 0; i < 100; i++ {
			msg := `{"did":"did:plc:test","kind":"commit"}`
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(wsURL, logger)

	ctx, cancel := context.WithCancel(context.Background())

	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Let some messages come through
	time.Sleep(100 * time.Millisecond)

	// Cancel context to trigger shutdown
	cancel()

	// Wait for channel to close
	timeout := time.After(2 * time.Second)
	select {
	case _, ok := <-client.GetMessageChannel():
		if ok {
			// Channel should eventually close
			time.Sleep(100 * time.Millisecond)
		}
	case <-timeout:
		t.Fatal("Channel did not close after context cancellation")
	}

	client.Close()
}

func TestClientClose(t *testing.T) {
	logger := common.NewLogger(false)

	server := newMockWebSocketServer(t, func(conn *websocket.Conn) {
		time.Sleep(100 * time.Millisecond)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(wsURL, logger)

	ctx := context.Background()
	err := client.Connect(ctx)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	err = client.Close()
	if err != nil {
		t.Errorf("Close returned error: %v", err)
	}

	// Verify reconnect flag is set to false
	if client.reconnect {
		t.Error("Expected reconnect to be false after Close()")
	}
}

func TestClientCloseWithoutConnection(t *testing.T) {
	logger := common.NewLogger(false)
	client := NewClient("ws://example.com/subscribe", logger)

	// Close without ever connecting
	err := client.Close()
	if err != nil {
		t.Errorf("Close on unconnected client returned error: %v", err)
	}
}

func TestClientMessageChannelBufferFull(t *testing.T) {
	logger := common.NewLogger(false)

	// Create a server that sends many messages quickly
	messageCount := 1500 // More than buffer capacity (1000)
	server := newMockWebSocketServer(t, func(conn *websocket.Conn) {
		for i := 0; i < messageCount; i++ {
			msg := `{"did":"did:plc:test","kind":"commit"}`
			if err := conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
				return
			}
		}
		// Keep connection open
		time.Sleep(500 * time.Millisecond)
	})
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")
	client := NewClient(wsURL, logger)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := client.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start client: %v", err)
	}

	// Don't read from the channel, let it fill up
	time.Sleep(300 * time.Millisecond)

	// The client should handle buffer full gracefully (dropping messages)
	// and not crash

	client.Close()
}

func TestGetMessageChannel(t *testing.T) {
	logger := common.NewLogger(false)
	client := NewClient("ws://example.com/subscribe", logger)

	msgChan := client.GetMessageChannel()
	if msgChan == nil {
		t.Fatal("GetMessageChannel returned nil")
	}

	// Verify it's the same channel
	msgChan2 := client.GetMessageChannel()
	if msgChan != msgChan2 {
		t.Error("GetMessageChannel returned different channels")
	}
}
