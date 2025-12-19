package common

import (
	"testing"
)

func TestJetstreamMessage_IsLike(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name    string
		rawJSON string
		want    bool
	}{
		{
			name: "like create event",
			rawJSON: `{
				"did": "did:plc:test123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.like",
					"rkey": "3m4zb3vk46q26",
					"record": {
						"subject": {
							"uri": "at://did:plc:xyz/app.bsky.feed.post/abc123"
						},
						"createdAt": "2025-01-27T12:34:56.789Z"
					}
				}
			}`,
			want: true,
		},
		{
			name: "like delete event",
			rawJSON: `{
				"did": "did:plc:test123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "delete",
					"collection": "app.bsky.feed.like",
					"rkey": "3m4zb3vk46q26"
				}
			}`,
			want: false,
		},
		{
			name: "post create event",
			rawJSON: `{
				"did": "did:plc:test123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.post",
					"rkey": "abc123",
					"record": {
						"text": "Hello world"
					}
				}
			}`,
			want: false,
		},
		{
			name:    "empty json",
			rawJSON: `{}`,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewJetstreamMessage(tt.rawJSON, logger)
			if got := msg.IsLike(); got != tt.want {
				t.Errorf("IsLike() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJetstreamMessage_IsLikeDelete(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name    string
		rawJSON string
		want    bool
	}{
		{
			name: "like delete event",
			rawJSON: `{
				"did": "did:plc:z4biihj3t2au7jw7qhemibpq",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "delete",
					"collection": "app.bsky.feed.like",
					"rkey": "3m4zb3vk46q26"
				}
			}`,
			want: true,
		},
		{
			name: "like create event",
			rawJSON: `{
				"did": "did:plc:test123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.like",
					"rkey": "3m4zb3vk46q26",
					"record": {
						"subject": {
							"uri": "at://did:plc:xyz/app.bsky.feed.post/abc123"
						},
						"createdAt": "2025-01-27T12:34:56.789Z"
					}
				}
			}`,
			want: false,
		},
		{
			name: "post delete event",
			rawJSON: `{
				"did": "did:plc:test123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "delete",
					"collection": "app.bsky.feed.post",
					"rkey": "abc123"
				}
			}`,
			want: false,
		},
		{
			name:    "empty json",
			rawJSON: `{}`,
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewJetstreamMessage(tt.rawJSON, logger)
			if got := msg.IsLikeDelete(); got != tt.want {
				t.Errorf("IsLikeDelete() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJetstreamMessage_URIConstruction(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name        string
		rawJSON     string
		wantAtURI   string
		wantAuthor  string
		wantSubject string
	}{
		{
			name: "like create with subject",
			rawJSON: `{
				"did": "did:plc:author123",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.like",
					"rkey": "likekey456",
					"record": {
						"subject": {
							"uri": "at://did:plc:postauthor/app.bsky.feed.post/postkey789"
						},
						"createdAt": "2025-01-27T12:34:56.789Z"
					}
				}
			}`,
			wantAtURI:   "at://did:plc:author123/app.bsky.feed.like/likekey456",
			wantAuthor:  "did:plc:author123",
			wantSubject: "at://did:plc:postauthor/app.bsky.feed.post/postkey789",
		},
		{
			name: "like delete without subject",
			rawJSON: `{
				"did": "did:plc:author456",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "delete",
					"collection": "app.bsky.feed.like",
					"rkey": "deletekey789"
				}
			}`,
			wantAtURI:   "at://did:plc:author456/app.bsky.feed.like/deletekey789",
			wantAuthor:  "did:plc:author456",
			wantSubject: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewJetstreamMessage(tt.rawJSON, logger)

			if got := msg.GetAtURI(); got != tt.wantAtURI {
				t.Errorf("GetAtURI() = %v, want %v", got, tt.wantAtURI)
			}

			if got := msg.GetAuthorDID(); got != tt.wantAuthor {
				t.Errorf("GetAuthorDID() = %v, want %v", got, tt.wantAuthor)
			}

			if got := msg.GetSubjectURI(); got != tt.wantSubject {
				t.Errorf("GetSubjectURI() = %v, want %v", got, tt.wantSubject)
			}
		})
	}
}

func TestJetstreamMessage_TimeUs(t *testing.T) {
	logger := NewLogger(false)

	wantTimeUs := int64(1764183883593160)

	rawJSON := `{
		"did": "did:plc:test",
		"time_us": 1764183883593160,
		"kind": "commit",
		"commit": {
			"operation": "delete",
			"collection": "app.bsky.feed.like",
			"rkey": "testkey"
		}
	}`

	msg := NewJetstreamMessage(rawJSON, logger)

	if got := msg.GetTimeUs(); got != wantTimeUs {
		t.Errorf("GetTimeUs() = %v, want %v", got, wantTimeUs)
	}
}

func TestJetstreamMessage_CreatedAtNormalization(t *testing.T) {
	logger := NewLogger(false)

	tests := []struct {
		name              string
		rawJSON           string
		expectedCreatedAt string
	}{
		{
			name: "UTC timestamp preserved",
			rawJSON: `{
				"did": "did:plc:test",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.like",
					"rkey": "testkey",
					"record": {
						"subject": {"uri": "at://did:plc:xyz/app.bsky.feed.post/abc"},
						"createdAt": "2025-01-27T12:00:00Z"
					}
				}
			}`,
			expectedCreatedAt: "2025-01-27T12:00:00Z",
		},
		{
			name: "timezone offset +05:00 normalized to UTC",
			rawJSON: `{
				"did": "did:plc:test",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.like",
					"rkey": "testkey",
					"record": {
						"subject": {"uri": "at://did:plc:xyz/app.bsky.feed.post/abc"},
						"createdAt": "2025-01-27T17:00:00+05:00"
					}
				}
			}`,
			expectedCreatedAt: "2025-01-27T12:00:00Z",
		},
		{
			name: "nanosecond precision normalized",
			rawJSON: `{
				"did": "did:plc:test",
				"time_us": 1764183883593160,
				"kind": "commit",
				"commit": {
					"operation": "create",
					"collection": "app.bsky.feed.like",
					"rkey": "testkey",
					"record": {
						"subject": {"uri": "at://did:plc:xyz/app.bsky.feed.post/abc"},
						"createdAt": "2025-01-27T12:00:00.789123Z"
					}
				}
			}`,
			expectedCreatedAt: "2025-01-27T12:00:00Z",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := NewJetstreamMessage(tt.rawJSON, logger)
			if got := msg.GetCreatedAt(); got != tt.expectedCreatedAt {
				t.Errorf("GetCreatedAt() = %q, expected %q", got, tt.expectedCreatedAt)
			}
		})
	}
}
