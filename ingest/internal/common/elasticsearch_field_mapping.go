package common

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/elastic/go-elasticsearch/v9"
)

// FieldMappingTypes returns the mapped type of each of fields in index, keyed
// by the full field name ("perspective_scores.toxicity" as well as
// "combined_perspective_score").
//
// A field that the index does not map is simply absent from the result rather
// than an error: Elasticsearch omits unmapped fields from the field-mapping
// API, and "not mapped" is a meaningful answer. That is the whole point of
// this helper — an index template applies only at index *creation*, so an
// index created before a template gained a field will not have it, and writing
// to it would let dynamic mapping infer a type instead. Callers that care use
// absence to gate the write.
func FieldMappingTypes(
	ctx context.Context,
	client *elasticsearch.Client,
	index string,
	fields []string,
) (map[string]string, error) {
	if len(fields) == 0 {
		return map[string]string{}, nil
	}

	res, err := client.Indices.GetFieldMapping(
		fields,
		client.Indices.GetFieldMapping.WithContext(ctx),
		client.Indices.GetFieldMapping.WithIndex(index),
	)
	if err != nil {
		return nil, fmt.Errorf("get field mapping for %s: %w", index, err)
	}
	defer func() {
		_ = res.Body.Close()
	}()

	if res.IsError() {
		return nil, fmt.Errorf("get field mapping for %s: %s", index, res.String())
	}

	// map[index]{ mappings: map[fullFieldName]{ mapping: map[leafName]{ type } } }
	var body map[string]struct {
		Mappings map[string]struct {
			Mapping map[string]struct {
				Type string `json:"type"`
			} `json:"mapping"`
		} `json:"mappings"`
	}
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("parse field mapping for %s: %w", index, err)
	}

	types := make(map[string]string, len(fields))
	for _, perIndex := range body {
		for fullName, field := range perIndex.Mappings {
			// The leaf mapping is keyed by the last path segment, which is the
			// only entry, so the name it is under does not matter here.
			for _, leaf := range field.Mapping {
				if leaf.Type != "" {
					types[fullName] = leaf.Type
				}
			}
		}
	}
	return types, nil
}
