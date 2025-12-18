package common

import (
	"encoding/json"
	"fmt"
)

// MegaStreamMessage defines the interface for processing messages from the MegaStream database
type MegaStreamMessage interface {
	GetAtURI() string
	GetAuthorDID() string
	GetContent() string
	GetCreatedAt() string
	GetThreadRootPost() string
	GetThreadParentPost() string
	GetQuotePost() string
	GetEmbeddings() map[string][]float32
	GetTimeUs() int64
	IsDelete() bool
	IsAccountDeletion() bool
	GetAccountStatus() string
}

// megaStreamMessage is the implementation of MegaStreamMessage
type megaStreamMessage struct {
	atURI            string
	did              string
	content          string
	createdAt        string
	threadRootPost   string
	threadParentPost string
	quotePost        string
	embeddings       map[string][]float32
	timeUs           int64
	isDelete         bool
	accountStatus    string
	parseError       error
}

// NewMegaStreamMessage creates a new MegaStreamMessage from raw SQLite data
func NewMegaStreamMessage(atURI, did, rawPostJSON, inferencesJSON string, logger *IngestLogger) MegaStreamMessage {
	msg := &megaStreamMessage{
		atURI:      atURI,
		did:        did,
		embeddings: make(map[string][]float32),
	}

	msg.parseRawPost(rawPostJSON, logger)
	msg.parseInferences(inferencesJSON, logger)

	return msg
}

// parseRawPost parses the raw_post JSON and extracts relevant fields
func (m *megaStreamMessage) parseRawPost(rawPostJSON string, logger *IngestLogger) {
	var rawPost map[string]interface{}
	if err := json.Unmarshal([]byte(rawPostJSON), &rawPost); err != nil {
		m.parseError = fmt.Errorf("failed to parse raw_post JSON: %w", err)
		logger.Error("Failed to parse raw_post JSON for %s: %v", m.atURI, err)
		return
	}

	message, ok := rawPost["message"].(map[string]interface{})
	if !ok {
		logger.Debug("No message field in raw_post for %s", m.atURI)
		return
	}

	if timeUs, ok := message["time_us"].(float64); ok {
		m.timeUs = int64(timeUs)
	}

	// Check for account deletion event FIRST (before checking commit field)
	if kind, ok := message["kind"].(string); ok && kind == "account" {
		if account, ok := message["account"].(map[string]interface{}); ok {
			if active, ok := account["active"].(bool); ok && !active {
				if status, ok := account["status"].(string); ok {
					m.accountStatus = status
					logger.Debug("Account event detected for DID %s: status=%s", m.did, status)
					return
				}
			}
		}
	}

	if m.atURI == "" {
		logger.Debug("Empty atURI in message for DID %s", m.did)
		return
	}

	commit, ok := message["commit"].(map[string]interface{})
	if !ok {
		logger.Debug("No commit field in message for %v", m.atURI)
		return
	}

	operation, _ := commit["operation"].(string)
	if operation == "delete" {
		m.isDelete = true
		return
	}

	record, ok := commit["record"].(map[string]interface{})
	if !ok {
		logger.Debug("No record field in commit for %s", m.atURI)
		return
	}

	m.content, _ = record["text"].(string) // This is blank on image posts

	m.createdAt, _ = record["createdAt"].(string)
	if m.createdAt == "" {
		logger.Debug("Empty createdAt in record for %s", m.atURI)
		return
	}

	hydratedMetadata, _ := rawPost["hydrated_metadata"].(map[string]interface{})
	if hydratedMetadata != nil {
		if replyPost, ok := hydratedMetadata["reply_post"].(map[string]interface{}); ok {
			m.threadRootPost, _ = replyPost["uri"].(string)
		}

		if parentPost, ok := hydratedMetadata["parent_post"].(map[string]interface{}); ok {
			m.threadParentPost, _ = parentPost["uri"].(string)
		}

		if qPost, ok := hydratedMetadata["quote_post"].(map[string]interface{}); ok {
			m.quotePost, _ = qPost["uri"].(string)
		}
	}
}

// parseInferences parses the inferences JSON and extracts embeddings
func (m *megaStreamMessage) parseInferences(inferencesJSON string, logger *IngestLogger) {
	var inferences map[string]interface{}
	if err := json.Unmarshal([]byte(inferencesJSON), &inferences); err != nil {
		logger.Debug("Failed to parse inferences JSON for %s: %v", m.atURI, err)
		return
	}

	textEmbeddings, ok := inferences["text_embeddings"].(map[string]interface{})
	if !ok {
		return
	}

	if embL12, ok := textEmbeddings["all-MiniLM-L12-v2"].(string); ok {
		if decoded, err := decodeEmbedding(embL12); err == nil {
			m.embeddings["all_MiniLM_L12_v2"] = decoded
		} else {
			logger.Debug("Failed to decode L12 embedding for %s: %v", m.atURI, err)
		}
	}

	if embL6, ok := textEmbeddings["all-MiniLM-L6-v2"].(string); ok {
		if decoded, err := decodeEmbedding(embL6); err == nil {
			m.embeddings["all_MiniLM_L6_v2"] = decoded
		} else {
			logger.Debug("Failed to decode L6 embedding for %s: %v", m.atURI, err)
		}
	}
}

// Interface method implementations

func (m *megaStreamMessage) GetAtURI() string {
	return m.atURI
}

func (m *megaStreamMessage) GetAuthorDID() string {
	return m.did
}

func (m *megaStreamMessage) GetContent() string {
	return m.content
}

func (m *megaStreamMessage) GetCreatedAt() string {
	return m.createdAt
}

func (m *megaStreamMessage) GetThreadRootPost() string {
	return m.threadRootPost
}

func (m *megaStreamMessage) GetThreadParentPost() string {
	return m.threadParentPost
}

func (m *megaStreamMessage) GetQuotePost() string {
	return m.quotePost
}

func (m *megaStreamMessage) GetEmbeddings() map[string][]float32 {
	return m.embeddings
}

func (m *megaStreamMessage) GetTimeUs() int64 {
	return m.timeUs
}

func (m *megaStreamMessage) IsDelete() bool {
	return m.isDelete
}

func (m *megaStreamMessage) IsAccountDeletion() bool {
	return m.accountStatus == "deleted"
}

func (m *megaStreamMessage) GetAccountStatus() string {
	return m.accountStatus
}
