package common

// ExtractPost represents the Post document structure for Parquet serialization
// Field names match the expected parquet output format
type ExtractPost struct {
	DID             string `json:"did"`
	EmbedQuoteURI   string `json:"embed_quote_uri,omitempty"`
	InsertedAt      string `json:"inserted_at"`
	RecordCreatedAt string `json:"record_created_at"`
	RecordText      string `json:"record_text"`
	ReplyParentURI  string `json:"reply_parent_uri,omitempty"`
	ReplyRootURI    string `json:"reply_root_uri,omitempty"`
	// TODO: Add embeddings as encoded values in the future
}

// HitToExtractPost converts an Elasticsearch Hit to an ExtractPost
func HitToExtractPost(hit Hit) ExtractPost {
	return ExtractPost{
		DID:             hit.Source.AuthorDID,
		EmbedQuoteURI:   hit.Source.QuotePost,
		InsertedAt:      hit.Source.IndexedAt,
		RecordCreatedAt: hit.Source.CreatedAt,
		RecordText:      hit.Source.Content,
		ReplyParentURI:  hit.Source.ThreadParentPost,
		ReplyRootURI:    hit.Source.ThreadRootPost,
	}
}

// HitsToExtractPosts converts multiple Elasticsearch Hits to ExtractPosts
func HitsToExtractPosts(hits []Hit) []ExtractPost {
	posts := make([]ExtractPost, len(hits))
	for i, hit := range hits {
		posts[i] = HitToExtractPost(hit)
	}
	return posts
}
