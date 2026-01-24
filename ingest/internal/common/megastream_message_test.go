package common

import (
	"testing"
)

func TestIsAccountDeletion(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name                    string
		rawPostJSON             string
		expectedIsAccountDeletion bool
		expectedAccountStatus   string
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
			expectedAccountStatus:   "deleted",
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
			expectedAccountStatus:   "deactivated",
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
			expectedAccountStatus:   "",
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
			expectedAccountStatus:   "",
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
			expectedAccountStatus:   "",
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