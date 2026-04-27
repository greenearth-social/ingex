package common

import (
	"fmt"
	"testing"
)

func TestShouldSampleDID_Determinism(t *testing.T) {
	did := "did:plc:abc123xyz"
	first := ShouldSampleDID(did)
	for i := 0; i < 100; i++ {
		if ShouldSampleDID(did) != first {
			t.Fatalf("ShouldSampleDID returned different results for same DID")
		}
	}
}

func TestShouldSampleDID_Distribution(t *testing.T) {
	total := 10000
	sampled := 0
	for i := 0; i < total; i++ {
		did := fmt.Sprintf("did:plc:user%d", i)
		if ShouldSampleDID(did) {
			sampled++
		}
	}
	pct := float64(sampled) / float64(total) * 100
	if pct < 7 || pct > 13 {
		t.Fatalf("expected ~10%% sample rate, got %.1f%%", pct)
	}
}

func TestShouldSampleDID_EmptyDID(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ShouldSampleDID panicked on empty DID: %v", r)
		}
	}()
	ShouldSampleDID("")
}
