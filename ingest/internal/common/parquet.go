package common

// ExtractPost represents the Post document structure for Parquet serialization
// Field names match the expected parquet output format
type ExtractPost struct {
	DID             string            `json:"did" parquet:"did"`
	AtURI           string            `json:"at_uri" parquet:"at_uri"`
	EmbedQuoteURI   string            `json:"embed_quote_uri,omitempty" parquet:"embed_quote_uri,optional"`
	InsertedAt      string            `json:"inserted_at" parquet:"inserted_at"`
	RecordCreatedAt string            `json:"record_created_at" parquet:"record_created_at"`
	RecordText      string            `json:"record_text" parquet:"record_text"`
	ReplyParentURI  string            `json:"reply_parent_uri,omitempty" parquet:"reply_parent_uri,optional"`
	ReplyRootURI    string            `json:"reply_root_uri,omitempty" parquet:"reply_root_uri,optional"`
	Embeddings      map[string]string `json:"embeddings,omitempty" parquet:"embeddings,optional"` // model name -> base85-encoded embedding string
}

// HitToExtractPost converts an Elasticsearch Hit to an ExtractPost
func HitToExtractPost(hit Hit) ExtractPost {
	extractPost := ExtractPost{
		DID:             hit.Source.AuthorDID,
		AtURI:           hit.Source.AtURI,
		EmbedQuoteURI:   hit.Source.QuotePost,
		InsertedAt:      hit.Source.IndexedAt,
		RecordCreatedAt: hit.Source.CreatedAt,
		RecordText:      hit.Source.Content,
		ReplyParentURI:  hit.Source.ThreadParentPost,
		ReplyRootURI:    hit.Source.ThreadRootPost,
	}

	// Encode embeddings if present
	if len(hit.Source.Embeddings) > 0 {
		extractPost.Embeddings = make(map[string]string, len(hit.Source.Embeddings))
		for modelName, floatArray := range hit.Source.Embeddings {
			if encoded, err := encodeEmbedding(floatArray); err == nil {
				extractPost.Embeddings[modelName] = encoded
			}
			// Silently skip embeddings that fail to encode
		}
	}

	return extractPost
}

// HitsToExtractPosts converts multiple Elasticsearch Hits to ExtractPosts
func HitsToExtractPosts(hits []Hit) []ExtractPost {
	posts := make([]ExtractPost, len(hits))
	for i, hit := range hits {
		posts[i] = HitToExtractPost(hit)
	}
	return posts
}

// ExtractLike represents the Like document structure for Parquet serialization
type ExtractLike struct {
	DID             string `json:"did" parquet:"did"`
	SubjectURI      string `json:"subject_uri" parquet:"subject_uri"`
	InsertedAt      string `json:"inserted_at" parquet:"inserted_at"`
	RecordCreatedAt string `json:"record_created_at" parquet:"record_created_at"`
}

// LikeHitToExtractLike converts an Elasticsearch LikeHit to an ExtractLike
func LikeHitToExtractLike(hit LikeHit) ExtractLike {
	return ExtractLike{
		DID:             hit.Source.AuthorDID,
		SubjectURI:      hit.Source.SubjectURI,
		InsertedAt:      hit.Source.IndexedAt,
		RecordCreatedAt: hit.Source.CreatedAt,
	}
}

// LikeHitsToExtractLikes converts multiple Elasticsearch LikeHits to ExtractLikes
func LikeHitsToExtractLikes(hits []LikeHit) []ExtractLike {
	likes := make([]ExtractLike, len(hits))
	for i, hit := range hits {
		likes[i] = LikeHitToExtractLike(hit)
	}
	return likes
}
