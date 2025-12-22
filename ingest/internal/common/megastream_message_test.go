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