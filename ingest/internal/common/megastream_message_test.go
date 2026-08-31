package common

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/greenearth/ingest/internal/embeddings"
)

func TestIsAccountDeletion(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name                      string
		rawPostJSON               string
		expectedIsAccountDeletion bool
		expectedAccountStatus     string
	}{
		{
			name: "account deletion event",
			rawPostJSON: `{
				"message": {
					"kind": "account",
					"account": {
						"active": false,
						"status": "deleted",
						"did": "did:plc:test123"
					},
					"time_us": 1757450926034794
				}
			}`,
			expectedIsAccountDeletion: true,
			expectedAccountStatus:     "deleted",
		},
		{
			name: "account deactivation event",
			rawPostJSON: `{
				"message": {
					"kind": "account",
					"account": {
						"active": false,
						"status": "deactivated",
						"did": "did:plc:test123"
					},
					"time_us": 1757450926034794
				}
			}`,
			expectedIsAccountDeletion: false,
			expectedAccountStatus:     "deactivated",
		},
		{
			name: "active account event",
			rawPostJSON: `{
				"message": {
					"kind": "account",
					"account": {
						"active": true,
						"status": "active",
						"did": "did:plc:test123"
					},
					"time_us": 1757450926034794
				}
			}`,
			expectedIsAccountDeletion: false,
			expectedAccountStatus:     "",
		},
		{
			name: "regular post creation event",
			rawPostJSON: `{
				"message": {
					"commit": {
						"operation": "create",
						"record": {
							"text": "Hello world",
							"createdAt": "2024-01-01T00:00:00Z"
						}
					},
					"time_us": 1757450926034794
				}
			}`,
			expectedIsAccountDeletion: false,
			expectedAccountStatus:     "",
		},
		{
			name: "regular post deletion event",
			rawPostJSON: `{
				"message": {
					"commit": {
						"operation": "delete"
					},
					"time_us": 1757450926034794
				}
			}`,
			expectedIsAccountDeletion: false,
			expectedAccountStatus:     "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMegaStreamMessage("at://test", "did:plc:test123", tt.rawPostJSON, "{}", logger)

			if got := msg.IsAccountDeletion(); got != tt.expectedIsAccountDeletion {
				t.Errorf("IsAccountDeletion() = %v, expected %v", got, tt.expectedIsAccountDeletion)
			}

			if got := msg.GetAccountStatus(); got != tt.expectedAccountStatus {
				t.Errorf("GetAccountStatus() = %q, expected %q", got, tt.expectedAccountStatus)
			}
		})
	}
}

func TestGetAccountStatus(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name           string
		rawPostJSON    string
		expectedStatus string
	}{
		{
			name: "deleted status",
			rawPostJSON: `{
				"message": {
					"kind": "account",
					"account": {
						"active": false,
						"status": "deleted"
					}
				}
			}`,
			expectedStatus: "deleted",
		},
		{
			name: "deactivated status",
			rawPostJSON: `{
				"message": {
					"kind": "account",
					"account": {
						"active": false,
						"status": "deactivated"
					}
				}
			}`,
			expectedStatus: "deactivated",
		},
		{
			name: "no account event",
			rawPostJSON: `{
				"message": {
					"commit": {
						"operation": "create"
					}
				}
			}`,
			expectedStatus: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMegaStreamMessage("at://test", "did:plc:test123", tt.rawPostJSON, "{}", logger)

			if got := msg.GetAccountStatus(); got != tt.expectedStatus {
				t.Errorf("GetAccountStatus() = %q, expected %q", got, tt.expectedStatus)
			}
		})
	}
}

func TestMegaStreamMessage_VideoEmbedParsing(t *testing.T) {
	logger := NewLogger(false)

	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "Check out this video!",
					"createdAt": "2025-01-27T12:00:00Z",
					"embed": {
						"$type": "app.bsky.embed.video",
						"video": {
							"$type": "blob",
							"ref": {
								"$link": "bafkreiexamplevideoid123"
							},
							"mimeType": "video/mp4",
							"size": 15000000
						},
						"aspectRatio": {
							"width": 1920,
							"height": 1080
						}
					}
				}
			}
		}
	}`

	msg := NewMegaStreamMessage("at://test", "did:plc:test123", rawPostJSON, "{}", logger)
	media := msg.GetMedia()

	if len(media) != 1 {
		t.Fatalf("Expected 1 media item, got %d", len(media))
	}

	item := media[0]
	if item.ID != "bafkreiexamplevideoid123" {
		t.Errorf("Expected ID 'bafkreiexamplevideoid123', got '%s'", item.ID)
	}
	if item.MediaType != "video" {
		t.Errorf("Expected MediaType 'video', got '%s'", item.MediaType)
	}
	if item.MimeType != "video/mp4" {
		t.Errorf("Expected MimeType 'video/mp4', got '%s'", item.MimeType)
	}
	if item.Size != 15000000 {
		t.Errorf("Expected Size 15000000, got %d", item.Size)
	}
	if item.Width != 1920 {
		t.Errorf("Expected Width 1920, got %d", item.Width)
	}
	if item.Height != 1080 {
		t.Errorf("Expected Height 1080, got %d", item.Height)
	}
	expectedRatio := 1920.0 / 1080.0
	if item.AspectRatio != expectedRatio {
		t.Errorf("Expected AspectRatio %f, got %f", expectedRatio, item.AspectRatio)
	}
}

func TestMegaStreamMessage_SingleImageEmbedParsing(t *testing.T) {
	logger := NewLogger(false)

	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "Check out this image!",
					"createdAt": "2025-01-27T12:00:00Z",
					"embed": {
						"$type": "app.bsky.embed.images",
						"images": [
							{
								"alt": "A beautiful sunset",
								"image": {
									"$type": "blob",
									"ref": {
										"$link": "bafkreiexampleimageid456"
									},
									"mimeType": "image/jpeg",
									"size": 500000
								},
								"aspectRatio": {
									"width": 1600,
									"height": 900
								}
							}
						]
					}
				}
			}
		}
	}`

	msg := NewMegaStreamMessage("at://test", "did:plc:test123", rawPostJSON, "{}", logger)
	media := msg.GetMedia()

	if len(media) != 1 {
		t.Fatalf("Expected 1 media item, got %d", len(media))
	}

	item := media[0]
	if item.ID != "bafkreiexampleimageid456" {
		t.Errorf("Expected ID 'bafkreiexampleimageid456', got '%s'", item.ID)
	}
	if item.MediaType != "image" {
		t.Errorf("Expected MediaType 'image', got '%s'", item.MediaType)
	}
	if item.MimeType != "image/jpeg" {
		t.Errorf("Expected MimeType 'image/jpeg', got '%s'", item.MimeType)
	}
	if item.Size != 500000 {
		t.Errorf("Expected Size 500000, got %d", item.Size)
	}
	if item.Width != 1600 {
		t.Errorf("Expected Width 1600, got %d", item.Width)
	}
	if item.Height != 900 {
		t.Errorf("Expected Height 900, got %d", item.Height)
	}
}

func TestMegaStreamMessage_MultipleImagesEmbedParsing(t *testing.T) {
	logger := NewLogger(false)

	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "Multiple images!",
					"createdAt": "2025-01-27T12:00:00Z",
					"embed": {
						"$type": "app.bsky.embed.images",
						"images": [
							{
								"alt": "Image 1",
								"image": {
									"$type": "blob",
									"ref": {"$link": "bafkreiimage1"},
									"mimeType": "image/jpeg",
									"size": 100000
								},
								"aspectRatio": {"width": 800, "height": 600}
							},
							{
								"alt": "Image 2",
								"image": {
									"$type": "blob",
									"ref": {"$link": "bafkreiimage2"},
									"mimeType": "image/png",
									"size": 200000
								},
								"aspectRatio": {"width": 1200, "height": 800}
							},
							{
								"alt": "Image 3",
								"image": {
									"$type": "blob",
									"ref": {"$link": "bafkreiimage3"},
									"mimeType": "image/webp",
									"size": 150000
								},
								"aspectRatio": {"width": 1000, "height": 1000}
							}
						]
					}
				}
			}
		}
	}`

	msg := NewMegaStreamMessage("at://test", "did:plc:test123", rawPostJSON, "{}", logger)
	media := msg.GetMedia()

	if len(media) != 3 {
		t.Fatalf("Expected 3 media items, got %d", len(media))
	}

	expectedIDs := []string{"bafkreiimage1", "bafkreiimage2", "bafkreiimage3"}
	expectedMimeTypes := []string{"image/jpeg", "image/png", "image/webp"}
	expectedSizes := []int64{100000, 200000, 150000}

	for i, item := range media {
		if item.ID != expectedIDs[i] {
			t.Errorf("Image %d: Expected ID '%s', got '%s'", i, expectedIDs[i], item.ID)
		}
		if item.MediaType != "image" {
			t.Errorf("Image %d: Expected MediaType 'image', got '%s'", i, item.MediaType)
		}
		if item.MimeType != expectedMimeTypes[i] {
			t.Errorf("Image %d: Expected MimeType '%s', got '%s'", i, expectedMimeTypes[i], item.MimeType)
		}
		if item.Size != expectedSizes[i] {
			t.Errorf("Image %d: Expected Size %d, got %d", i, expectedSizes[i], item.Size)
		}
	}
}

func TestMegaStreamMessage_RecordWithMediaParsing(t *testing.T) {
	logger := NewLogger(false)

	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "Quote post with media!",
					"createdAt": "2025-01-27T12:00:00Z",
					"embed": {
						"$type": "app.bsky.embed.recordWithMedia",
						"record": {
							"$type": "app.bsky.embed.record",
							"record": {
								"cid": "bafyreiquotedpost",
								"uri": "at://did:plc:quoted/app.bsky.feed.post/xyz"
							}
						},
						"media": {
							"$type": "app.bsky.embed.images",
							"images": [
								{
									"alt": "Attached image",
									"image": {
										"$type": "blob",
										"ref": {"$link": "bafkreirecordwithmedia"},
										"mimeType": "image/jpeg",
										"size": 300000
									},
									"aspectRatio": {"width": 1200, "height": 800}
								}
							]
						}
					}
				}
			}
		}
	}`

	msg := NewMegaStreamMessage("at://test", "did:plc:test123", rawPostJSON, "{}", logger)
	media := msg.GetMedia()

	if len(media) != 1 {
		t.Fatalf("Expected 1 media item from recordWithMedia, got %d", len(media))
	}

	item := media[0]
	if item.ID != "bafkreirecordwithmedia" {
		t.Errorf("Expected ID 'bafkreirecordwithmedia', got '%s'", item.ID)
	}
	if item.MediaType != "image" {
		t.Errorf("Expected MediaType 'image', got '%s'", item.MediaType)
	}
	if item.MimeType != "image/jpeg" {
		t.Errorf("Expected MimeType 'image/jpeg', got '%s'", item.MimeType)
	}
}

func TestMegaStreamMessage_NoEmbedParsing(t *testing.T) {
	logger := NewLogger(false)

	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "Just a text post with no media",
					"createdAt": "2025-01-27T12:00:00Z"
				}
			}
		}
	}`

	msg := NewMegaStreamMessage("at://test", "did:plc:test123", rawPostJSON, "{}", logger)
	media := msg.GetMedia()

	if media != nil {
		t.Errorf("Expected nil media for post without embed, got %v", media)
	}
}

func TestMegaStreamMessage_RecordWithMediaVideoEmbed(t *testing.T) {
	logger := NewLogger(false)

	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "Quote post with video!",
					"createdAt": "2025-01-27T12:00:00Z",
					"embed": {
						"$type": "app.bsky.embed.recordWithMedia",
						"record": {
							"$type": "app.bsky.embed.record",
							"record": {
								"cid": "bafyreiquotedpost",
								"uri": "at://did:plc:quoted/app.bsky.feed.post/xyz"
							}
						},
						"media": {
							"$type": "app.bsky.embed.video",
							"video": {
								"$type": "blob",
								"ref": {"$link": "bafkreivideorecordwithmedia"},
								"mimeType": "video/mp4",
								"size": 8000000
							},
							"aspectRatio": {"width": 1280, "height": 720}
						}
					}
				}
			}
		}
	}`

	msg := NewMegaStreamMessage("at://test", "did:plc:test123", rawPostJSON, "{}", logger)
	media := msg.GetMedia()

	if len(media) != 1 {
		t.Fatalf("Expected 1 media item from recordWithMedia video, got %d", len(media))
	}

	item := media[0]
	if item.ID != "bafkreivideorecordwithmedia" {
		t.Errorf("Expected ID 'bafkreivideorecordwithmedia', got '%s'", item.ID)
	}
	if item.MediaType != "video" {
		t.Errorf("Expected MediaType 'video', got '%s'", item.MediaType)
	}
}

func TestMegaStreamMessage_ImageAltTextParsing(t *testing.T) {
	logger := NewLogger(false)

	t.Run("single image with alt text", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "A photo",
						"createdAt": "2025-01-27T12:00:00Z",
						"embed": {
							"$type": "app.bsky.embed.images",
							"images": [
								{
									"alt": "Cape Forchu, NS",
									"image": {
										"$type": "blob",
										"ref": {"$link": "bafkreiimage1"},
										"mimeType": "image/jpeg",
										"size": 980954
									},
									"aspectRatio": {"width": 2000, "height": 1332}
								}
							]
						}
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		media := msg.GetMedia()

		if len(media) != 1 {
			t.Fatalf("Expected 1 media item, got %d", len(media))
		}
		if media[0].AltText != "Cape Forchu, NS" {
			t.Errorf("Expected AltText 'Cape Forchu, NS', got %q", media[0].AltText)
		}
	})

	t.Run("multiple images with mixed alt text", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Multiple images",
						"createdAt": "2025-01-27T12:00:00Z",
						"embed": {
							"$type": "app.bsky.embed.images",
							"images": [
								{
									"alt": "",
									"image": {
										"ref": {"$link": "bafkreiimage1"},
										"mimeType": "image/jpeg",
										"size": 500623
									},
									"aspectRatio": {"width": 1499, "height": 2000}
								},
								{
									"alt": "A beautiful painting",
									"image": {
										"ref": {"$link": "bafkreiimage2"},
										"mimeType": "image/jpeg",
										"size": 951096
									},
									"aspectRatio": {"width": 1499, "height": 2000}
								}
							]
						}
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		media := msg.GetMedia()

		if len(media) != 2 {
			t.Fatalf("Expected 2 media items, got %d", len(media))
		}
		if media[0].AltText != "" {
			t.Errorf("Expected empty AltText for first image, got %q", media[0].AltText)
		}
		if media[1].AltText != "A beautiful painting" {
			t.Errorf("Expected AltText 'A beautiful painting', got %q", media[1].AltText)
		}
	})

	t.Run("alt text in CreatePostDoc", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "A photo",
						"createdAt": "2025-01-27T12:00:00Z",
						"embed": {
							"$type": "app.bsky.embed.images",
							"images": [
								{
									"alt": "Cape Forchu, NS",
									"image": {
										"ref": {"$link": "bafkreiimage1"},
										"mimeType": "image/jpeg",
										"size": 980954
									},
									"aspectRatio": {"width": 2000, "height": 1332}
								}
							]
						}
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		doc := CreatePostDoc(msg, 0)

		if len(doc.Media) != 1 {
			t.Fatalf("Expected 1 media item in doc, got %d", len(doc.Media))
		}
		if doc.Media[0].AltText != "Cape Forchu, NS" {
			t.Errorf("Expected AltText 'Cape Forchu, NS' in doc, got %q", doc.Media[0].AltText)
		}
	})
}

func TestMegaStreamMessage_ExternalEmbedParsing(t *testing.T) {
	logger := NewLogger(false)

	t.Run("external embed with all fields", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Check this out",
						"createdAt": "2025-12-12T02:14:29.851Z",
						"embed": {
							"$type": "app.bsky.embed.external",
							"external": {
								"description": "A cartoon dog sitting at a table",
								"title": "Funny GIF",
								"uri": "https://media.tenor.com/example.gif"
							}
						}
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		embed := msg.GetExternalEmbed()

		if embed == nil {
			t.Fatal("Expected non-nil ExternalEmbed")
		}
		if embed.URI != "https://media.tenor.com/example.gif" {
			t.Errorf("Expected URI 'https://media.tenor.com/example.gif', got %q", embed.URI)
		}
		if embed.Title != "Funny GIF" {
			t.Errorf("Expected Title 'Funny GIF', got %q", embed.Title)
		}
		if embed.Description != "A cartoon dog sitting at a table" {
			t.Errorf("Expected Description 'A cartoon dog sitting at a table', got %q", embed.Description)
		}
	})

	t.Run("no external embed", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Just text",
						"createdAt": "2025-01-27T12:00:00Z"
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		embed := msg.GetExternalEmbed()

		if embed != nil {
			t.Errorf("Expected nil ExternalEmbed, got %+v", embed)
		}
	})

	t.Run("external embed in CreatePostDoc", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Link post",
						"createdAt": "2025-12-12T02:14:29.851Z",
						"embed": {
							"$type": "app.bsky.embed.external",
							"external": {
								"description": "Page description",
								"title": "Page Title",
								"uri": "https://example.com"
							}
						}
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		doc := CreatePostDoc(msg, 0)

		if doc.ExternalEmbed == nil {
			t.Fatal("Expected non-nil ExternalEmbed in doc")
		}
		if doc.ExternalEmbed.URI != "https://example.com" {
			t.Errorf("Expected URI 'https://example.com', got %q", doc.ExternalEmbed.URI)
		}
		if doc.ExternalEmbed.Title != "Page Title" {
			t.Errorf("Expected Title 'Page Title', got %q", doc.ExternalEmbed.Title)
		}
		if doc.ExternalEmbed.Description != "Page description" {
			t.Errorf("Expected Description 'Page description', got %q", doc.ExternalEmbed.Description)
		}
	})

	t.Run("external embed in recordWithMedia", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Quote with external link",
						"createdAt": "2025-01-27T12:00:00Z",
						"embed": {
							"$type": "app.bsky.embed.recordWithMedia",
							"record": {
								"$type": "app.bsky.embed.record",
								"record": {
									"cid": "bafyreiquotedpost",
									"uri": "at://did:plc:quoted/app.bsky.feed.post/xyz"
								}
							},
							"media": {
								"$type": "app.bsky.embed.external",
								"external": {
									"description": "Linked page desc",
									"title": "Linked Page",
									"uri": "https://example.com/page"
								}
							}
						}
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)
		embed := msg.GetExternalEmbed()

		if embed == nil {
			t.Fatal("Expected non-nil ExternalEmbed from recordWithMedia")
		}
		if embed.URI != "https://example.com/page" {
			t.Errorf("Expected URI 'https://example.com/page', got %q", embed.URI)
		}
	})
}

func TestMegaStreamMessage_VideoTranscriptParsing(t *testing.T) {
	logger := NewLogger(false)

	t.Run("video with transcript", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Check out this video!",
						"createdAt": "2025-12-12T02:14:25.876Z",
						"embed": {
							"$type": "app.bsky.embed.video",
							"video": {
								"ref": {"$link": "bafkreivideo1"},
								"mimeType": "video/mp4",
								"size": 8396837
							},
							"aspectRatio": {"width": 1920, "height": 1080}
						}
					}
				}
			}
		}`

		inferencesJSON := `{
			"text_embeddings": {
				"all-MiniLM-L12-v2": "abc",
				"all-MiniLM-L6-v2": "def"
			},
			"video": {
				"audio_transcription": {
					"text": "Hello world this is a transcript",
					"language": "en"
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, inferencesJSON, logger)

		if msg.GetVideoTranscript() != "Hello world this is a transcript" {
			t.Errorf("Expected transcript 'Hello world this is a transcript', got %q", msg.GetVideoTranscript())
		}
		if msg.GetVideoTranscriptLanguage() != "en" {
			t.Errorf("Expected language 'en', got %q", msg.GetVideoTranscriptLanguage())
		}
	})

	t.Run("no video transcript", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Just text",
						"createdAt": "2025-01-27T12:00:00Z"
					}
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, "{}", logger)

		if msg.GetVideoTranscript() != "" {
			t.Errorf("Expected empty transcript, got %q", msg.GetVideoTranscript())
		}
		if msg.GetVideoTranscriptLanguage() != "" {
			t.Errorf("Expected empty language, got %q", msg.GetVideoTranscriptLanguage())
		}
	})

	t.Run("video transcript in CreatePostDoc", func(t *testing.T) {
		rawPostJSON := `{
			"message": {
				"commit": {
					"operation": "create",
					"record": {
						"text": "Video post",
						"createdAt": "2025-12-12T02:14:25.876Z",
						"embed": {
							"$type": "app.bsky.embed.video",
							"video": {
								"ref": {"$link": "bafkreivideo1"},
								"mimeType": "video/mp4",
								"size": 8396837
							},
							"aspectRatio": {"width": 1920, "height": 1080}
						}
					}
				}
			}
		}`

		inferencesJSON := `{
			"video": {
				"audio_transcription": {
					"text": "Transcript text here",
					"language": "es"
				}
			}
		}`

		msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, inferencesJSON, logger)
		doc := CreatePostDoc(msg, 0)

		if doc.VideoTranscript != "Transcript text here" {
			t.Errorf("Expected VideoTranscript 'Transcript text here', got %q", doc.VideoTranscript)
		}
		if doc.VideoTranscriptLanguage != "es" {
			t.Errorf("Expected VideoTranscriptLanguage 'es', got %q", doc.VideoTranscriptLanguage)
		}
	})
}

func TestMegaStreamMessage_TopicScoresParsing(t *testing.T) {
	logger := NewLogger(false)
	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "A post about current events",
					"createdAt": "2025-01-27T12:00:00Z"
				}
			}
		}
	}`

	tests := []struct {
		name       string
		inferences string
		want       map[string]float32
	}{
		{
			name: "all valid post text topic scores",
			inferences: `{
				"text": {
					"message.commit.record.text": {
						"topic": {
							"News & Social Concern": 0.73779296875,
							"Sports": 0.00861358642578125,
							"Science & Technology": 0.6943359375
						}
					}
				}
			}`,
			want: map[string]float32{
				"News & Social Concern": float32(0.73779296875),
				"Sports":                float32(0.00861358642578125),
				"Science & Technology":  float32(0.6943359375),
			},
		},
		{
			name: "invalid entries are skipped while zero is retained",
			inferences: `{
				"text": {
					"message.commit.record.text": {
						"topic": {
							"Sports": 0,
							"String Score": "0.7",
							"Negative Score": -0.1,
							"Score Above One": 1.1
						}
					}
				}
			}`,
			want: map[string]float32{"Sports": 0},
		},
		{
			name: "empty topic object",
			inferences: `{
				"text": {
					"message.commit.record.text": {"topic": {}}
				}
			}`,
		},
		{
			name:       "missing text analyses",
			inferences: `{}`,
		},
		{
			name: "topics on another analyzed field are ignored",
			inferences: `{
				"text": {
					"message.commit.record.embed.external.title": {
						"topic": {"Sports": 0.9}
					}
				}
			}`,
		},
		{
			name: "all invalid scores produce no map",
			inferences: `{
				"text": {
					"message.commit.record.text": {
						"topic": {
							"String Score": "0.7",
							"Score Above One": 1.1
						}
					}
				}
			}`,
		},
		{
			name:       "malformed inference JSON",
			inferences: `{"text":`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, tt.inferences, logger)
			if got := msg.GetTopicScores(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetTopicScores() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCreatePostDoc_TopicScoresJSON(t *testing.T) {
	logger := NewLogger(false)
	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "A post",
					"createdAt": "2025-01-27T12:00:00Z"
				}
			}
		}
	}`

	withTopics := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, `{
		"text": {
			"message.commit.record.text": {
				"topic": {
					"News & Social Concern": 0.73779296875,
					"Sports": 0
				}
			}
		}
	}`, logger)
	want := map[string]float32{
		"News & Social Concern": float32(0.73779296875),
		"Sports":                0,
	}
	doc := CreatePostDoc(withTopics, 0)
	if !reflect.DeepEqual(doc.TopicScores, want) {
		t.Fatalf("CreatePostDoc topic scores = %v, want %v", doc.TopicScores, want)
	}
	encoded, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal post document: %v", err)
	}
	var encodedDoc map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &encodedDoc); err != nil {
		t.Fatalf("unmarshal post document JSON: %v", err)
	}
	var encodedTopics map[string]float32
	if err := json.Unmarshal(encodedDoc["topic_scores"], &encodedTopics); err != nil {
		t.Fatalf("unmarshal topic_scores: %v", err)
	}
	if !reflect.DeepEqual(encodedTopics, want) {
		t.Errorf("serialized topic_scores = %v, want %v", encodedTopics, want)
	}

	withoutTopics := CreatePostDoc(
		NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, `{}`, logger),
		0,
	)
	encoded, err = json.Marshal(withoutTopics)
	if err != nil {
		t.Fatalf("marshal post document without topics: %v", err)
	}
	encodedDoc = nil
	if err := json.Unmarshal(encoded, &encodedDoc); err != nil {
		t.Fatalf("unmarshal post document JSON without topics: %v", err)
	}
	if _, ok := encodedDoc["topic_scores"]; ok {
		t.Errorf("missing topic scores were not omitted from post JSON: %s", encoded)
	}
}

func TestMegaStreamMessage_CreatedAtNormalization(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name              string
		rawPostJSON       string
		expectedCreatedAt string
	}{
		{
			name: "UTC timestamp preserved",
			rawPostJSON: `{
				"message": {
					"commit": {
						"operation": "create",
						"record": {
							"text": "Hello",
							"createdAt": "2025-01-27T12:00:00Z"
						}
					}
				}
			}`,
			expectedCreatedAt: "2025-01-27T12:00:00Z",
		},
		{
			name: "timezone offset +05:00 normalized to UTC",
			rawPostJSON: `{
				"message": {
					"commit": {
						"operation": "create",
						"record": {
							"text": "Hello",
							"createdAt": "2025-01-27T17:00:00+05:00"
						}
					}
				}
			}`,
			expectedCreatedAt: "2025-01-27T12:00:00Z",
		},
		{
			name: "timezone offset -08:00 normalized to UTC",
			rawPostJSON: `{
				"message": {
					"commit": {
						"operation": "create",
						"record": {
							"text": "Hello",
							"createdAt": "2025-01-27T04:00:00-08:00"
						}
					}
				}
			}`,
			expectedCreatedAt: "2025-01-27T12:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewMegaStreamMessage("at://test", "did:plc:test", tt.rawPostJSON, "{}", logger)
			if got := msg.GetCreatedAt(); got != tt.expectedCreatedAt {
				t.Errorf("GetCreatedAt() = %q, expected %q", got, tt.expectedCreatedAt)
			}
		})
	}
}

// TestMegaStreamMessage_EmbeddingsParsing covers api#312 step 2 and ingex#444:
// only the families something actually reads are ingested. all_MiniLM_L6_v2 and
// google_embeddinggemma_300m are read by nothing, so parseInferences must not
// store them; all_MiniLM_L12_v2 must still be parsed, because serving reads it
// (MMR and the heavy ranker) and it is the post tower's input. Every field kept
// on a post document is paid for on the hydration path and again in _source.
func TestMegaStreamMessage_EmbeddingsParsing(t *testing.T) {
	logger := NewLogger(false)
	rawPostJSON := `{
		"message": {
			"commit": {
				"operation": "create",
				"record": {
					"text": "hello",
					"createdAt": "2025-12-12T02:14:25.876Z"
				}
			}
		}
	}`

	l12, err := embeddings.Encode([]float32{0.1, 0.2, 0.3})
	if err != nil {
		t.Fatalf("failed to encode L12 fixture: %v", err)
	}
	l6, err := embeddings.Encode([]float32{0.4, 0.5, 0.6})
	if err != nil {
		t.Fatalf("failed to encode L6 fixture: %v", err)
	}

	gemma, err := embeddings.Encode([]float32{0.7, 0.8, 0.9})
	if err != nil {
		t.Fatalf("failed to encode gemma fixture: %v", err)
	}

	inferencesJSON := fmt.Sprintf(`{
		"text_embeddings": {
			"all-MiniLM-L12-v2": %q,
			"all-MiniLM-L6-v2": %q
		},
		"video": {
			"audio_transcription": {
				"text": "a transcript",
				"language": "en",
				"embeddings": {"google/embeddinggemma-300m": %q}
			}
		}
	}`, l12, l6, gemma)

	msg := NewMegaStreamMessage("at://test", "did:plc:test", rawPostJSON, inferencesJSON, logger)
	got := msg.GetEmbeddings()

	if _, ok := got["all_MiniLM_L6_v2"]; ok {
		t.Errorf("expected all_MiniLM_L6_v2 to be dropped, but it was present: %v", got["all_MiniLM_L6_v2"])
	}
	if _, ok := got["google_embeddinggemma_300m"]; ok {
		t.Errorf("expected google_embeddinggemma_300m to be dropped, but it was present: %v", got["google_embeddinggemma_300m"])
	}
	// The transcript itself is still ingested; only its embedding is dropped.
	if msg.GetVideoTranscript() != "a transcript" {
		t.Errorf("video transcript should still be parsed, got %q", msg.GetVideoTranscript())
	}
	if _, ok := got["all_MiniLM_L12_v2"]; !ok {
		t.Errorf("expected all_MiniLM_L12_v2 to still be parsed, got %v", got)
	}
}
