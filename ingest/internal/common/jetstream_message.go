package common

import (
	"encoding/json"
	"fmt"
	"time"
)

// JetstreamMessage defines the interface for processing messages from the Bluesky Jetstream
type JetstreamMessage interface {
	GetURI() string
	GetSubjectURI() string
	GetAuthorDID() string
	GetCreatedAt() string
	GetTimeUs() int64
	IsLike() bool
}

// jetstreamMessage is the implementation of JetstreamMessage
type jetstreamMessage struct {
	uri        string
	subjectURI string
	authorDID  string
	createdAt  string
	timeUs     int64
	isLike     bool
	parseError error
}

// JetstreamEventData represents the raw Jetstream event structure
type JetstreamEventData struct {
	Did    string `json:"did"`
	TimeUs int64  `json:"time_us"`
	Kind   string `json:"kind"`
	Commit struct {
		Operation  string                 `json:"operation"`
		Collection string                 `json:"collection"`
		RKey       string                 `json:"rkey"`
		Record     map[string]interface{} `json:"record"`
		CID        string                 `json:"cid"`
	} `json:"commit"`
}

// NewJetstreamMessage creates a new JetstreamMessage from raw Jetstream JSON data
func NewJetstreamMessage(rawJSON string, logger *IngestLogger) JetstreamMessage {
	msg := &jetstreamMessage{}
	msg.parseRawEvent(rawJSON, logger)
	return msg
}

// parseRawEvent parses the raw Jetstream JSON and extracts relevant fields
func (m *jetstreamMessage) parseRawEvent(rawJSON string, logger *IngestLogger) {
	var event JetstreamEventData
	if err := json.Unmarshal([]byte(rawJSON), &event); err != nil {
		m.parseError = fmt.Errorf("failed to parse Jetstream JSON: %w", err)
		logger.Error("Failed to parse Jetstream JSON: %v", err)
		return
	}

	m.authorDID = event.Did
	m.timeUs = event.TimeUs

	// Check if this is a like event
	if event.Kind == "commit" &&
		event.Commit.Collection == "app.bsky.feed.like" &&
		event.Commit.Operation == "create" {
		m.isLike = true

		// Construct the URI for this like
		m.uri = fmt.Sprintf("at://%s/%s/%s", event.Did, event.Commit.Collection, event.Commit.RKey)

		// Extract the subject URI (the post being liked)
		if subject, ok := event.Commit.Record["subject"].(map[string]interface{}); ok {
			if subjectURI, ok := subject["uri"].(string); ok {
				m.subjectURI = subjectURI
			}
		}

		// Extract created_at timestamp
		if createdAt, ok := event.Commit.Record["createdAt"].(string); ok {
			m.createdAt = createdAt
		} else if m.timeUs > 0 {
			// Fallback to time_us if createdAt not present
			m.createdAt = time.Unix(0, m.timeUs*1000).UTC().Format(time.RFC3339)
		}
	}
}

// Interface method implementations

func (m *jetstreamMessage) GetURI() string {
	return m.uri
}

func (m *jetstreamMessage) GetSubjectURI() string {
	return m.subjectURI
}

func (m *jetstreamMessage) GetAuthorDID() string {
	return m.authorDID
}

func (m *jetstreamMessage) GetCreatedAt() string {
	return m.createdAt
}

func (m *jetstreamMessage) GetTimeUs() int64 {
	return m.timeUs
}

func (m *jetstreamMessage) IsLike() bool {
	return m.isLike
}
