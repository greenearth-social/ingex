package common

import (
	"encoding/json"
	"fmt"
)

// JetstreamMessage defines the interface for processing messages from the Bluesky Jetstream
type JetstreamMessage interface {
	GetAtURI() string
	GetSubjectURI() string
	GetAuthorDID() string
	GetCreatedAt() string
	GetTimeUs() int64
	IsLike() bool
	IsLikeDelete() bool
	IsFollow() bool
	IsFollowDelete() bool
	GetFollowSubjectDID() string
}

// jetstreamMessage is the implementation of JetstreamMessage
type jetstreamMessage struct {
	uri              string
	subjectURI       string
	authorDID        string
	createdAt        string
	timeUs           int64
	isLike           bool
	isLikeDelete     bool
	isFollow         bool
	isFollowDelete   bool
	followSubjectDID string
	parseError       error
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

	// Check if this is a like-related event
	if event.Kind == "commit" && event.Commit.Collection == "app.bsky.feed.like" {
		// Construct the URI for this like (works for both create and delete)
		m.uri = fmt.Sprintf("at://%s/%s/%s", event.Did, event.Commit.Collection, event.Commit.RKey)

		switch event.Commit.Operation {
		case "create":
			m.isLike = true

			// Extract the subject URI (the post being liked)
			if subject, ok := event.Commit.Record["subject"].(map[string]interface{}); ok {
				if subjectURI, ok := subject["uri"].(string); ok {
					m.subjectURI = subjectURI
				}
			}

			// Extract and normalize created_at timestamp to UTC
			if rawCreatedAt, ok := event.Commit.Record["createdAt"].(string); ok {
				m.createdAt = NormalizeTimestampToUTC(rawCreatedAt, logger)
				if m.createdAt == "" {
					logger.Error("Failed to normalize createdAt timestamp for at_uri: %s (raw value: %s)", m.uri, rawCreatedAt)
					return
				}
			} else {
				logger.Error("Failed to extract createdAt from Jetstream JSON (at_uri: %s)", m.uri)
				return
			}
		case "delete":
			m.isLikeDelete = true
			// For delete events, we only have did, collection, and rkey
			// URI is already constructed above
			// subject_uri will be fetched from Elasticsearch
		}
	}

	// Follow events feed the API's per-user followed-users cache (api#83).
	// The firehose is already unfiltered here for likes, so these cost no
	// extra bandwidth; the follow writer drops everything but our own users.
	if event.Kind == "commit" && event.Commit.Collection == "app.bsky.graph.follow" {
		m.uri = fmt.Sprintf("at://%s/%s/%s", event.Did, event.Commit.Collection, event.Commit.RKey)

		switch event.Commit.Operation {
		case "create":
			// In app.bsky.graph.follow the subject is a bare DID string, not
			// the {"uri": ...} object a like's subject uses.
			subjectDID, ok := event.Commit.Record["subject"].(string)
			if !ok || subjectDID == "" {
				logger.Error("Failed to extract follow subject DID (at_uri: %s)", m.uri)
				return
			}
			m.isFollow = true
			m.followSubjectDID = subjectDID
		case "delete":
			// Deletes carry only did, collection and rkey — there is no way
			// to tell *who* was unfollowed without a record of the rkey. The
			// API handles this by invalidating the user's entry instead.
			m.isFollowDelete = true
		}
	}
}

// Interface method implementations

func (m *jetstreamMessage) GetAtURI() string {
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

func (m *jetstreamMessage) IsLikeDelete() bool {
	return m.isLikeDelete
}

func (m *jetstreamMessage) IsFollow() bool {
	return m.isFollow
}

func (m *jetstreamMessage) IsFollowDelete() bool {
	return m.isFollowDelete
}

func (m *jetstreamMessage) GetFollowSubjectDID() string {
	return m.followSubjectDID
}
