package main

import (
	"testing"

	"github.com/greenearth/ingest/internal/common"
)

func TestPostAliasFromDoc_originalPost(t *testing.T) {
	doc := common.ElasticsearchDoc{ThreadParentPost: ""}
	if got := postAliasFromDoc(doc); got != "posts" {
		t.Errorf("expected posts, got %s", got)
	}
}

func TestPostAliasFromDoc_reply(t *testing.T) {
	doc := common.ElasticsearchDoc{ThreadParentPost: "at://did:plc:abc/app.bsky.feed.post/xyz"}
	if got := postAliasFromDoc(doc); got != "replies" {
		t.Errorf("expected replies, got %s", got)
	}
}
