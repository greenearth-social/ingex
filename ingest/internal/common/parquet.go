package common

// ExtractPost represents the Post document structure for Parquet serialization
// Identical to ElasticsearchDoc but adds EsID field from Hit metadata
type ExtractPost struct {
	EsID             string               `json:"es_id"`
	AtURI            string               `json:"at_uri"`
	AuthorDID        string               `json:"author_did"`
	Content          string               `json:"content"`
	CreatedAt        string               `json:"created_at"`
	ThreadRootPost   string               `json:"thread_root_post,omitempty"`
	ThreadParentPost string               `json:"thread_parent_post,omitempty"`
	QuotePost        string               `json:"quote_post,omitempty"`
	Embeddings       map[string][]float32 `json:"embeddings,omitempty"`
	IndexedAt        string               `json:"indexed_at"`
}

// HitToExtractPost converts an Elasticsearch Hit to an ExtractPost
func HitToExtractPost(hit Hit) ExtractPost {
	return ExtractPost{
		EsID:             hit.ID,
		AtURI:            hit.Source.AtURI,
		AuthorDID:        hit.Source.AuthorDID,
		Content:          hit.Source.Content,
		CreatedAt:        hit.Source.CreatedAt,
		ThreadRootPost:   hit.Source.ThreadRootPost,
		ThreadParentPost: hit.Source.ThreadParentPost,
		QuotePost:        hit.Source.QuotePost,
		Embeddings:       hit.Source.Embeddings,
		IndexedAt:        hit.Source.IndexedAt,
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
