package common

import (
	"bytes"
	"testing"
)

func TestDecodeBase85RFC1924(t *testing.T) {
	tests := []struct {
		name     string
		encoded  string
		expected []byte
	}{
		{
			name:     "empty string",
			encoded:  "",
			expected: []byte(""),
		},
		{
			name:     "long string",
			encoded:  "RA^-&adl~9Yan8BZ+C7WW^Z^PYISXJb0BYaWpW^NXk{R5VS0HWWN&9KAXI2&AZBTHWgud2cxi5DAa`kcVRB@1AZm4Oa3FDYX=7__c`hJOVPk6`ZFwMKZ+IYgX>@2HW@&b1AY^ZPWo{sBX>oOLav*ATXLBwfNN;x_c4c^JZf9(HAaQkRV{0H}VP<q7dSzmAVRImAb!~7V",
			expected: []byte("The quick brown fox jumps over the lazy dog. The five boxing wizards jump quickly. Pack my box with five dozen liquor jugs. How vexingly quick daft zebras jump!"),
		},
		{
			name:     "weird input characters",
			encoded:  "AwVM}C0-^fC@EhnEj?RZdwqO6IwCACJU%}!eqa",
			expected: []byte("!@#$%^&*()_+-=[]{}|;:\",.<>?/~`"),
		},
		{
			name:     "basic sentence with punctuation",
			encoded:  "NM&qnZ!92JZ*pv8As|R^cOYSMWgvNPbs%(aWMO$f",
			expected: []byte("Hello, World! How are you today?"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			decoded, err := decodeBase85RFC1924(tt.encoded)
			if err != nil {
				t.Fatalf("decodeBase85RFC1924() error = %v, expected nil", err)
			}
			if !bytes.Equal(decoded, tt.expected) {
				t.Errorf("decodeBase85RFC1924() = %q, expected %q", decoded, tt.expected)
			}
		})
	}
}

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
