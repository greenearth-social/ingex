package common

import (
	"bytes"
	"math"
	"testing"
)

// TestDecodeBase85RFC1924 tests the base85 decoding function
// Test data moved from megastream_message_test.go
func TestDecodeBase85RFC1924(t *testing.T) {
	tests := []struct {
		name     string
		encoded  string
		expected []byte
	}{
		{
			name:     "empty string",
			encoded:  "",
			expected: []byte(""),
		},
		{
			name:     "long string",
			encoded:  "RA^-&adl~9Yan8BZ+C7WW^Z^PYISXJb0BYaWpW^NXk{R5VS0HWWN&9KAXI2&AZBTHWgud2cxi5DAa`kcVRB@1AZm4Oa3FDYX=7__c`hJOVPk6`ZFwMKZ+IYgX>@2HW@&b1AY^ZPWo{sBX>oOLav*ATXLBwfNN;x_c4c^JZf9(HAaQkRV{0H}VP<q7dSzmAVRImAb!~7V",
			expected: []byte("The quick brown fox jumps over the lazy dog. The five boxing wizards jump quickly. Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump!"),
		},
		{
			name:     "weird input characters",
			encoded:  "AwVM}C0-^fC@EhnEj?RZdwqO6IwCACJU%}!eqa",
			expected: []byte("!@#$%^&*()_+-=[]{}|;:\",.<>?/~`"),
		},
		{
			name:     "basic sentence with punctuation",
			encoded:  "NM&qnZ!92JZ*pv8As|R^cOYSMWgvNPbs%(aWMO$f",
			expected: []byte("Hello, World! How are you today?"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := decodeBase85RFC1924(tt.encoded)
			if err != nil {
				t.Fatalf("decodeBase85RFC1924() error = %v, expected nil", err)
			}
			if !bytes.Equal(decoded, tt.expected) {
				t.Errorf("decodeBase85RFC1924() = %q, expected %q", decoded, tt.expected)
			}
		})
	}
}

// TestEncodeBase85RFC1924 tests the base85 encoding function
func TestEncodeBase85RFC1924(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected string
	}{
		{
			name:     "empty bytes",
			data:     []byte(""),
			expected: "",
		},
		{
			name:     "long string",
			data:     []byte("The quick brown fox jumps over the lazy dog. The five boxing wizards jump quickly. Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump!"),
			expected: "RA^-&adl~9Yan8BZ+C7WW^Z^PYISXJb0BYaWpW^NXk{R5VS0HWWN&9KAXI2&AZBTHWgud2cxi5DAa`kcVRB@1AZm4Oa3FDYX=7__c`hJOVPk6`ZFwMKZ+IYgX>@2HW@&b1AY^ZPWo{sBX>oOLav*ATXLBwfNN;x_c4c^JZf9(HAaQkRV{0H}VP<q7dSzmAVRImAb!~7V",
		},
		{
			name:     "weird characters",
			data:     []byte("!@#$%^&*()_+-=[]{}|;:\",.<>?/~`"),
			expected: "AwVM}C0-^fC@EhnEj?RZdwqO6IwCACJU%}!eqa",
		},
		{
			name:     "basic sentence with punctuation",
			data:     []byte("Hello, World! How are you today?"),
			expected: "NM&qnZ!92JZ*pv8As|R^cOYSMWgvNPbs%(aWMO$f",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeBase85RFC1924(tt.data)
			if err != nil {
				t.Fatalf("encodeBase85RFC1924() error = %v, expected nil", err)
			}
			if encoded != tt.expected {
				t.Errorf("encodeBase85RFC1924() = %q, expected %q", encoded, tt.expected)
			}
		})
	}
}

// TestBase85RFC1924RoundTrip tests that encoding and decoding are inverse operations
func TestBase85RFC1924RoundTrip(t *testing.T) {
	tests := []struct {
		name string
		data []byte
	}{
		{"empty", []byte("")},
		{"single byte", []byte("a")},
		{"two bytes", []byte("ab")},
		{"three bytes", []byte("abc")},
		{"four bytes", []byte("abcd")},
		{"five bytes", []byte("abcde")},
		{"binary data", []byte{0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC}},
		{"all zeros", []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{"all ones", []byte{255, 255, 255, 255, 255, 255, 255, 255}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeBase85RFC1924(tt.data)
			if err != nil {
				t.Fatalf("encodeBase85RFC1924() error = %v", err)
			}

			decoded, err := decodeBase85RFC1924(encoded)
			if err != nil {
				t.Fatalf("decodeBase85RFC1924() error = %v", err)
			}

			if !bytes.Equal(decoded, tt.data) {
				t.Errorf("round-trip failed: got %v, want %v", decoded, tt.data)
			}
		})
	}
}

// TestDecodeEmbedding tests the embedding decoding function
func TestDecodeEmbedding(t *testing.T) {
	tests := []struct {
		name     string
		encoded  string
		expected []float32
	}{
		{
			name:     "small embedding",
			encoded:  "c${NkXs~BsU~m8;2LK5}0e}",
			expected: []float32{1.0, 2.0, 3.0},
		},
		{
			name:     "single value",
			encoded:  "c${Nk&~O3(0G0r2",
			expected: []float32{42.0},
		},
		{
			name:     "zeros",
			encoded:  "c${NkKmY&$3;+Q",
			expected: []float32{0.0, 0.0, 0.0},
		},
		{
			name:     "edge cases",
			encoded:  "c${NkIIy1q3=Y_zJ#*X%hz|h(J<<q6",
			expected: []float32{-1.5, 0.0, 1.5, 99.9, -99.9},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := decodeEmbedding(tt.encoded)
			if err != nil {
				t.Fatalf("decodeEmbedding() error = %v, expected nil", err)
			}

			if len(decoded) != len(tt.expected) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.expected))
			}

			for i := range decoded {
				if !floatsAlmostEqual(decoded[i], tt.expected[i]) {
					t.Errorf("value mismatch at index %d: got %v, want %v", i, decoded[i], tt.expected[i])
				}
			}
		})
	}
}

// TestEncodeEmbedding tests the embedding encoding function
// Note: We don't test for exact match with Python output because different zlib
// implementations can produce different (but valid) compressed outputs
func TestEncodeEmbedding(t *testing.T) {
	tests := []struct {
		name   string
		floats []float32
	}{
		{
			name:   "small embedding",
			floats: []float32{1.0, 2.0, 3.0},
		},
		{
			name:   "single value",
			floats: []float32{42.0},
		},
		{
			name:   "zeros",
			floats: []float32{0.0, 0.0, 0.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeEmbedding(tt.floats)
			if err != nil {
				t.Fatalf("encodeEmbedding() error = %v, expected nil", err)
			}

			// Verify we can decode it back
			decoded, err := decodeEmbedding(encoded)
			if err != nil {
				t.Fatalf("decodeEmbedding() error = %v, expected nil", err)
			}

			if len(decoded) != len(tt.floats) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.floats))
			}

			for i := range decoded {
				if decoded[i] != tt.floats[i] {
					t.Errorf("value mismatch at index %d: got %v, want %v", i, decoded[i], tt.floats[i])
				}
			}
		})
	}
}

// TestEmbeddingRoundTrip tests that encoding and decoding embeddings are inverse operations
func TestEmbeddingRoundTrip(t *testing.T) {
	tests := []struct {
		name   string
		floats []float32
	}{
		{"single", []float32{1.0}},
		{"small", []float32{1.0, 2.0, 3.0}},
		{"zeros", []float32{0.0, 0.0, 0.0}},
		{"negative", []float32{-1.0, -2.0, -3.0}},
		{"mixed", []float32{-1.5, 0.0, 1.5, 42.0}},
		{"special values", []float32{0.0, 0.0, float32(math.Inf(1)), float32(math.Inf(-1))}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := encodeEmbedding(tt.floats)
			if err != nil {
				t.Fatalf("encodeEmbedding() error = %v", err)
			}

			decoded, err := decodeEmbedding(encoded)
			if err != nil {
				t.Fatalf("decodeEmbedding() error = %v", err)
			}

			if len(decoded) != len(tt.floats) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.floats))
			}

			for i := range decoded {
				// Handle special values
				if math.IsInf(float64(tt.floats[i]), 0) {
					if !math.IsInf(float64(decoded[i]), 0) || math.Signbit(float64(tt.floats[i])) != math.Signbit(float64(decoded[i])) {
						t.Errorf("infinity mismatch at index %d: got %v, want %v", i, decoded[i], tt.floats[i])
					}
					continue
				}
				if math.IsNaN(float64(tt.floats[i])) {
					if !math.IsNaN(float64(decoded[i])) {
						t.Errorf("NaN mismatch at index %d: got %v, want %v", i, decoded[i], tt.floats[i])
					}
					continue
				}
				// Regular values should match exactly (bit-level)
				if decoded[i] != tt.floats[i] {
					t.Errorf("value mismatch at index %d: got %v, want %v", i, decoded[i], tt.floats[i])
				}
			}
		})
	}
}

// TestEmbeddingRoundTripNaN tests NaN values separately since NaN != NaN
func TestEmbeddingRoundTripNaN(t *testing.T) {
	floats := []float32{float32(math.NaN()), 1.0, float32(math.NaN())}

	encoded, err := encodeEmbedding(floats)
	if err != nil {
		t.Fatalf("encodeEmbedding() error = %v", err)
	}

	decoded, err := decodeEmbedding(encoded)
	if err != nil {
		t.Fatalf("decodeEmbedding() error = %v", err)
	}

	if len(decoded) != len(floats) {
		t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(floats))
	}

	// Check NaN values
	if !math.IsNaN(float64(decoded[0])) {
		t.Errorf("expected NaN at index 0, got %v", decoded[0])
	}
	if decoded[1] != 1.0 {
		t.Errorf("expected 1.0 at index 1, got %v", decoded[1])
	}
	if !math.IsNaN(float64(decoded[2])) {
		t.Errorf("expected NaN at index 2, got %v", decoded[2])
	}
}

// floatsAlmostEqual compares two float32 values with tolerance for floating point precision
func floatsAlmostEqual(a, b float32) bool {
	if math.IsNaN(float64(a)) && math.IsNaN(float64(b)) {
		return true
	}
	if math.IsInf(float64(a), 0) && math.IsInf(float64(b), 0) {
		return math.Signbit(float64(a)) == math.Signbit(float64(b))
	}
	// For most values, require exact match (bit-level)
	// For values like 99.9 that can't be represented exactly, allow small tolerance
	const epsilon = 1e-5
	diff := a - b
	if diff < 0 {
		diff = -diff
	}
	return diff < epsilon
}
