package common

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
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

	commit, ok := message["commit"].(map[string]interface{})
	if !ok {
		logger.Debug("No commit field in message for %s", m.atURI)
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

	m.content, _ = record["text"].(string)
	m.createdAt, _ = record["createdAt"].(string)

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

// decodeBase85RFC1924 decodes RFC 1924 base85 encoded data (used by Python's base64.b85decode)
func decodeBase85RFC1924(encoded string) ([]byte, error) {
	const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~"

	var decodeMap [256]int
	for i := range decodeMap {
		decodeMap[i] = -1
	}
	for i, c := range alphabet {
		decodeMap[c] = i
	}

	input := []byte(encoded)
	padding := (-len(input)) % 5
	if padding < 0 {
		padding += 5
	}

	paddedInput := make([]byte, len(input)+padding)
	copy(paddedInput, input)
	for i := len(input); i < len(paddedInput); i++ {
		paddedInput[i] = '~'
	}

	output := make([]byte, 0, len(paddedInput)*4/5)

	for i := 0; i < len(paddedInput); i += 5 {
		var value uint32
		for j := 0; j < 5; j++ {
			digit := decodeMap[paddedInput[i+j]]
			if digit == -1 {
				return nil, fmt.Errorf("illegal base85 data at input byte %d", i+j)
			}
			value = value*85 + uint32(digit)
		}

		output = append(output, byte(value>>24))
		output = append(output, byte(value>>16))
		output = append(output, byte(value>>8))
		output = append(output, byte(value))
	}

	if padding > 0 {
		output = output[:len(output)-padding]
	}

	return output, nil
}

// decodeEmbedding decodes a base85-encoded, zlib-compressed embedding string to float32 array
func decodeEmbedding(encoded string) ([]float32, error) {
	decoded, err := decodeBase85RFC1924(encoded)
	if err != nil {
		return nil, fmt.Errorf("base85 decode failed: %w", err)
	}

	reader, err := zlib.NewReader(bytes.NewReader(decoded))
	if err != nil {
		return nil, fmt.Errorf("zlib decompression failed: %w", err)
	}
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read decompressed data: %w", err)
	}

	floatCount := len(decompressed) / 4
	floats := make([]float32, floatCount)

	for i := range floatCount {
		bits := binary.LittleEndian.Uint32(decompressed[i*4 : (i+1)*4])
		floats[i] = math.Float32frombits(bits)
	}

	return floats, nil
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
