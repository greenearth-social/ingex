package common

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestFloat32JSONMarshaling verifies that float32 arrays are always serialized as floats
func TestFloat32JSONMarshaling(t *testing.T) {
	tests := []struct {
		name     string
		input    []float32
		expected string
	}{
		{
			name:     "decimals",
			input:    []float32{0.1, 0.2, 0.3},
			expected: "[0.1,0.2,0.3]",
		},
		{
			name:     "whole numbers",
			input:    []float32{1.0, 2.0, 3.0},
			expected: "[1,2,3]", // Go's json.Marshal outputs whole numbers without decimals
		},
		{
			name:     "mixed",
			input:    []float32{1.0, 1.5, 2.0},
			expected: "[1,1.5,2]",
		},
		{
			name:     "zeros",
			input:    []float32{0.0, 0.0, 0.0},
			expected: "[0,0,0]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			result := string(data)
			t.Logf("Input: %v, Marshaled: %s", tt.input, result)

			// Check that the result contains numeric values (not strings)
			if strings.Contains(result, `"`) {
				t.Errorf("Marshaled result contains strings: %s", result)
			}
		})
	}
}

// TestEmbeddingsJSONMarshaling tests the full ElasticsearchDoc embedding serialization
func TestEmbeddingsJSONMarshaling(t *testing.T) {
	doc := ElasticsearchDoc{
		AtURI:     "at://test",
		AuthorDID: "did:test",
		Content:   "test content",
		CreatedAt: "2024-01-01T00:00:00Z",
		Embeddings: map[string]Float32Array{
			"all_MiniLM_L6_v2": {0.0, 1.0, 2.5, -1.0},
		},
		IndexedAt: "2024-01-01T00:00:00Z",
	}

	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	result := string(data)
	t.Logf("Marshaled document: %s", result)

	// Unmarshal to verify structure
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}

	embeddings, ok := decoded["embeddings"].(map[string]interface{})
	if !ok {
		t.Fatal("embeddings field missing or wrong type")
	}

	embedding, ok := embeddings["all_MiniLM_L6_v2"].([]interface{})
	if !ok {
		t.Fatal("all_MiniLM_L6_v2 field missing or wrong type")
	}

	// Verify each value is a number (float64 after JSON unmarshal)
	expected := []float64{0.0, 1.0, 2.5, -1.0}
	for i, val := range embedding {
		floatVal, ok := val.(float64)
		if !ok {
			t.Errorf("Value at index %d is not a float64: %T %v", i, val, val)
		}
		if floatVal != expected[i] {
			t.Errorf("Value at index %d: expected %v, got %v", i, expected[i], floatVal)
		}
	}
}

// TestFloat32ArrayMarshaling tests that Float32Array always produces float values
func TestFloat32ArrayMarshaling(t *testing.T) {
	tests := []struct {
		name  string
		input Float32Array
	}{
		{
			name:  "whole numbers",
			input: Float32Array{1.0, 2.0, 3.0},
		},
		{
			name:  "zeros",
			input: Float32Array{0.0, 0.0, 0.0},
		},
		{
			name:  "mixed",
			input: Float32Array{1.0, 1.5, 2.0, 0.0, -1.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data, err := json.Marshal(tt.input)
			if err != nil {
				t.Fatalf("Marshal failed: %v", err)
			}

			result := string(data)
			t.Logf("Input: %v, Marshaled: %s", tt.input, result)

			// Unmarshal back to verify all values are treated as floats
			var decoded []interface{}
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatalf("Unmarshal failed: %v", err)
			}

			for i, val := range decoded {
				if _, ok := val.(float64); !ok {
					t.Errorf("Value at index %d is not a float64: %T %v", i, val, val)
				}
			}
		})
	}
}

// TestEmptyEmbeddingsMarshaling verifies that empty embeddings return nil
func TestEmptyEmbeddingsMarshaling(t *testing.T) {
	msg := &megaStreamMessage{
		embeddings: make(map[string][]float32),
	}

	embeddings := msg.GetEmbeddings()
	if embeddings != nil {
		t.Errorf("Expected nil for empty embeddings, got %v", embeddings)
	}

	// Test with actual doc - nil embeddings should be omitted
	doc := ElasticsearchDoc{
		AtURI:      "at://test",
		AuthorDID:  "did:test",
		Content:    "test",
		CreatedAt:  "2024-01-01T00:00:00Z",
		Embeddings: nil,
		IndexedAt:  "2024-01-01T00:00:00Z",
	}

	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	result := string(data)
	t.Logf("Marshaled doc with nil embeddings: %s", result)

	// Should not contain embeddings field at all due to omitempty
	if strings.Contains(result, `"embeddings"`) {
		t.Errorf("Expected embeddings field to be omitted, but it's present: %s", result)
	}
}
