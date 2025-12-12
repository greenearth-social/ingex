package common

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"strings"
)

// Embedding encoding/decoding utilities for converting between float32 arrays
// and base85-encoded, zlib-compressed strings. This matches the Python
// implementation using base64.b85encode/decode and zlib compression.
//
// The encoding process:
// 1. Pack float32 values as little-endian binary data
// 2. Compress with zlib
// 3. Encode with RFC 1924 base85 (matching Python's base64.b85encode)
//
// The decoding process is the reverse:
// 1. Decode base85 using RFC 1924 alphabet
// 2. Decompress with zlib
// 3. Unpack little-endian binary to float32 values

const base85Alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&()*+-;<=>?@^_`{|}~"

// decodeBase85RFC1924 decodes RFC 1924 base85 encoded data (used by Python's base64.b85decode)
func decodeBase85RFC1924(encoded string) ([]byte, error) {
	var decodeMap [256]int
	for i := range decodeMap {
		decodeMap[i] = -1
	}
	for i, c := range base85Alphabet {
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
			// Safely convert and check for overflow
			if digit < 0 || digit > 84 {
				return nil, fmt.Errorf("invalid base85 digit value: %d", digit)
			}
			digitValue := uint32(digit)
			if value > (math.MaxUint32-digitValue)/85 {
				return nil, fmt.Errorf("base85 decode overflow at input byte %d", i+j)
			}
			value = value*85 + digitValue
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

// encodeBase85RFC1924 encodes data using RFC 1924 base85 encoding (matches Python's base64.b85encode)
// Returns the base85-encoded string
func encodeBase85RFC1924(data []byte) (string, error) {
	if len(data) == 0 {
		return "", nil
	}

	// Calculate padding needed to make length divisible by 4
	padding := (4 - len(data)%4) % 4
	paddedData := data
	if padding > 0 {
		paddedData = make([]byte, len(data)+padding)
		copy(paddedData, data)
		// Remaining bytes are zero-padded
	}

	var result strings.Builder
	result.Grow(len(paddedData) * 5 / 4)

	// Process 4 bytes at a time
	for i := 0; i < len(paddedData); i += 4 {
		// Read 4 bytes as big-endian uint32
		value := uint32(paddedData[i])<<24 |
			uint32(paddedData[i+1])<<16 |
			uint32(paddedData[i+2])<<8 |
			uint32(paddedData[i+3])

		// Convert to 5 base85 digits
		// Decode expects most significant digit first, so collect and reverse
		var digits [5]byte
		for j := 4; j >= 0; j-- {
			digits[j] = base85Alphabet[value%85]
			value /= 85
		}

		result.Write(digits[:])
	}

	// Remove padding characters from result
	output := result.String()
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
	defer func() {
		_ = reader.Close() // Ignore error in cleanup
	}()

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

// encodeEmbedding encodes a float32 array to a base85-encoded, zlib-compressed string
// This is the reverse of decodeEmbedding
// Process: pack little-endian float32s → zlib compress → base85 encode
func encodeEmbedding(floats []float32) (string, error) {
	if len(floats) == 0 {
		return "", nil
	}

	// Pack floats to bytes (little-endian)
	byteData := make([]byte, len(floats)*4)
	for i, f := range floats {
		bits := math.Float32bits(f)
		binary.LittleEndian.PutUint32(byteData[i*4:(i+1)*4], bits)
	}

	// Compress with zlib
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	if _, err := writer.Write(byteData); err != nil {
		return "", fmt.Errorf("zlib compression failed: %w", err)
	}
	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("zlib writer close failed: %w", err)
	}

	// Encode with base85
	encoded, err := encodeBase85RFC1924(compressed.Bytes())
	if err != nil {
		return "", fmt.Errorf("base85 encode failed: %w", err)
	}

	return encoded, nil
}
