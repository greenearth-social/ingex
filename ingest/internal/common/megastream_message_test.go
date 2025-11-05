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
