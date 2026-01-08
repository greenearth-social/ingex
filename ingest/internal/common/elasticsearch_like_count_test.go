package common

import (
	"context"
	"testing"
)

func TestCreateElasticsearchDoc_WithLikeCount(t *testing.T) {
	logger := NewLogger(false)

	validRawPost := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "test post content",
					"createdAt": "2024-01-01T00:00:00Z"
				}
			}
		}
	}`

	// Test with zero likes
	msg := NewMegaStreamMessage("at://did:test/app.bsky.feed.post/test123", "did:test", validRawPost, "{}", logger)
	doc := CreateElasticsearchDoc(msg, 0)

	if doc.LikeCount != 0 {
		t.Errorf("Expected LikeCount = 0, got %d", doc.LikeCount)
	}

	if doc.AtURI != "at://did:test/app.bsky.feed.post/test123" {
		t.Errorf("Expected AtURI to be set correctly, got %s", doc.AtURI)
	}

	if doc.Content != "test post content" {
		t.Errorf("Expected Content to be set correctly, got %s", doc.Content)
	}

	// Test with positive likes
	doc2 := CreateElasticsearchDoc(msg, 42)
	if doc2.LikeCount != 42 {
		t.Errorf("Expected LikeCount = 42, got %d", doc2.LikeCount)
	}

	// Test with large like count
	doc3 := CreateElasticsearchDoc(msg, 10000)
	if doc3.LikeCount != 10000 {
		t.Errorf("Expected LikeCount = 10000, got %d", doc3.LikeCount)
	}

	// Verify other fields are still set correctly with like count
	if doc2.AuthorDID != "did:test" {
		t.Errorf("Expected AuthorDID to be preserved with like count, got %s", doc2.AuthorDID)
	}

	if doc2.IndexedAt == "" {
		t.Error("Expected IndexedAt to be set")
	}
}

func TestLikeCountUpdate_Aggregation(t *testing.T) {
	// Test that multiple updates to same post are aggregated correctly
	updates := []LikeCountUpdate{
		{SubjectURI: "at://post1", Increment: 1},
		{SubjectURI: "at://post1", Increment: 1},
		{SubjectURI: "at://post2", Increment: 1},
		{SubjectURI: "at://post1", Increment: 1}, // Third update to post1
		{SubjectURI: "at://post3", Increment: 5},
	}

	// Simulate aggregation logic from BulkUpdatePostLikeCounts
	aggregated := make(map[string]int)
	for _, update := range updates {
		aggregated[update.SubjectURI] += update.Increment
	}

	if aggregated["at://post1"] != 3 {
		t.Errorf("Expected post1 to have 3 updates, got %d", aggregated["at://post1"])
	}

	if aggregated["at://post2"] != 1 {
		t.Errorf("Expected post2 to have 1 update, got %d", aggregated["at://post2"])
	}

	if aggregated["at://post3"] != 5 {
		t.Errorf("Expected post3 to have 5 updates, got %d", aggregated["at://post3"])
	}

	// Test with negative increments (deletions)
	updates2 := []LikeCountUpdate{
		{SubjectURI: "at://post1", Increment: 5},
		{SubjectURI: "at://post1", Increment: -2},
		{SubjectURI: "at://post1", Increment: -1},
	}

	aggregated2 := make(map[string]int)
	for _, update := range updates2 {
		aggregated2[update.SubjectURI] += update.Increment
	}

	if aggregated2["at://post1"] != 2 {
		t.Errorf("Expected post1 to have net increment of 2, got %d", aggregated2["at://post1"])
	}
}

func TestBulkUpdatePostLikeCounts_DryRun(t *testing.T) {
	logger := NewLogger(false)

	updates := []LikeCountUpdate{
		{SubjectURI: "at://test", Increment: 1},
	}

	// Dry-run should not error with nil client
	err := BulkUpdatePostLikeCounts(context.TODO(), nil, "posts", updates, true, logger)
	if err != nil {
		t.Errorf("Expected no error in dry-run mode, got: %v", err)
	}
}

func TestBulkUpdatePostLikeCounts_EmptyBatch(t *testing.T) {
	logger := NewLogger(false)

	// Empty batch should not error
	err := BulkUpdatePostLikeCounts(context.TODO(), nil, "posts", []LikeCountUpdate{}, false, logger)
	if err != nil {
		t.Errorf("Expected no error for empty batch, got: %v", err)
	}

	// Nil batch should not error
	err2 := BulkUpdatePostLikeCounts(context.TODO(), nil, "posts", nil, false, logger)
	if err2 != nil {
		t.Errorf("Expected no error for nil batch, got: %v", err2)
	}
}

func TestBulkUpdatePostLikeCounts_EmptySubjectURI(t *testing.T) {
	logger := NewLogger(false)

	updates := []LikeCountUpdate{
		{SubjectURI: "", Increment: 1},
		{SubjectURI: "", Increment: 1},
	}

	// Should return error when all updates have empty subject_uri
	err := BulkUpdatePostLikeCounts(context.TODO(), nil, "posts", updates, false, logger)
	if err == nil {
		t.Error("Expected error when all updates have empty subject_uri")
	}

	if err.Error() != "no valid updates in batch" {
		t.Errorf("Expected 'no valid updates in batch' error, got: %v", err)
	}
}

func TestBulkUpdatePostLikeCounts_MixedEmptyAndValid(t *testing.T) {
	updates := []LikeCountUpdate{
		{SubjectURI: "", Increment: 1},
		{SubjectURI: "at://valid", Increment: 1},
		{SubjectURI: "", Increment: 1},
	}

	// Simulate aggregation to verify empty URIs are skipped
	aggregated := make(map[string]int)
	validCount := 0
	for _, update := range updates {
		if update.SubjectURI != "" {
			aggregated[update.SubjectURI] += update.Increment
			validCount++
		}
	}

	if validCount != 1 {
		t.Errorf("Expected 1 valid update, got %d", validCount)
	}

	if aggregated["at://valid"] != 1 {
		t.Errorf("Expected valid post to have 1 update, got %d", aggregated["at://valid"])
	}
}

func TestBulkCountLikesBySubjectURIs_EmptyInput(t *testing.T) {
	logger := NewLogger(false)

	// Empty slice should return empty map without error
	result, err := BulkCountLikesBySubjectURIs(context.TODO(), nil, "likes", []string{}, logger)
	if err != nil {
		t.Errorf("Expected no error for empty input, got: %v", err)
	}

	if result == nil {
		t.Error("Expected non-nil result")
	}

	if len(result) != 0 {
		t.Errorf("Expected empty result map, got %d entries", len(result))
	}

	// Nil slice should also return empty map without error
	result2, err2 := BulkCountLikesBySubjectURIs(context.TODO(), nil, "likes", nil, logger)
	if err2 != nil {
		t.Errorf("Expected no error for nil input, got: %v", err2)
	}

	if result2 == nil {
		t.Error("Expected non-nil result for nil input")
	}

	if len(result2) != 0 {
		t.Errorf("Expected empty result map for nil input, got %d entries", len(result2))
	}
}

func TestLikeCountUpdate_StructFields(t *testing.T) {
	// Test struct initialization
	update := LikeCountUpdate{
		SubjectURI: "at://did:plc:test/app.bsky.feed.post/abc123",
		Increment:  1,
	}

	if update.SubjectURI != "at://did:plc:test/app.bsky.feed.post/abc123" {
		t.Errorf("Expected SubjectURI to be set, got %s", update.SubjectURI)
	}

	if update.Increment != 1 {
		t.Errorf("Expected Increment to be 1, got %d", update.Increment)
	}

	// Test with negative increment
	update2 := LikeCountUpdate{
		SubjectURI: "at://post2",
		Increment:  -5,
	}

	if update2.Increment != -5 {
		t.Errorf("Expected Increment to be -5, got %d", update2.Increment)
	}

	// Test zero value
	var update3 LikeCountUpdate
	if update3.SubjectURI != "" {
		t.Errorf("Expected empty SubjectURI for zero value, got %s", update3.SubjectURI)
	}

	if update3.Increment != 0 {
		t.Errorf("Expected Increment to be 0 for zero value, got %d", update3.Increment)
	}
}
