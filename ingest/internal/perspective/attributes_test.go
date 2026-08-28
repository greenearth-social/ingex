package perspective

import (
	"math"
	"testing"
)

// The expectations here are the Go half of a cross-language fixture: the same
// weight groups and the same three cases are asserted in the api, in
// api/src/app/lib/perspective_test.py (TestPrcScore). If one repo's numbers
// move and the other's do not, a post's combined score starts depending on
// which service computed it — the api reads this package's output out of
// Elasticsearch and computes its own only on a cache miss.
var (
	bridgingAttrs = []string{
		"reasoning_experimental", "personal_story_experimental", "affinity_experimental",
		"compassion_experimental", "respect_experimental", "curiosity_experimental",
	}
	outrageSixthAttrs      = []string{"fearmongering_experimental", "generalization_experimental"}
	outrageEighteenthAttrs = []string{"scapegoating_experimental", "moral_outrage_experimental", "alienation_experimental"}
	toxicEighthAttrs       = []string{"toxicity", "identity_attack", "insult", "threat"}
)

func zeroAttrs() map[string]float64 {
	attrs := make(map[string]float64, len(RequestedAttributes))
	for _, key := range StorageKeys() {
		attrs[key] = 0.0
	}
	return attrs
}

func withValues(attrs map[string]float64, keys []string, value float64) map[string]float64 {
	for _, key := range keys {
		attrs[key] = value
	}
	return attrs
}

func assertClose(t *testing.T, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestPRCScoreAllZerosIsMidpoint(t *testing.T) {
	assertClose(t, PRCScore(zeroAttrs()), 0.5)
}

func TestPRCScorePureBridgingIsMax(t *testing.T) {
	assertClose(t, PRCScore(withValues(zeroAttrs(), bridgingAttrs, 1.0)), 1.0)
}

func TestPRCScorePureNegativeIsMin(t *testing.T) {
	attrs := zeroAttrs()
	withValues(attrs, outrageSixthAttrs, 1.0)
	withValues(attrs, outrageEighteenthAttrs, 1.0)
	withValues(attrs, toxicEighthAttrs, 1.0)
	assertClose(t, PRCScore(attrs), 0.0)
}

// The mixed case from the api's test_known_mixed_inputs, with the same inputs
// and the same independently-derived expectation.
func TestPRCScoreKnownMixedInputs(t *testing.T) {
	attrs := zeroAttrs()
	withValues(attrs, bridgingAttrs, 0.6)
	withValues(attrs, outrageSixthAttrs, 0.3)
	withValues(attrs, outrageEighteenthAttrs, 0.9)
	withValues(attrs, toxicEighthAttrs, 0.4)

	raw := float64(len(bridgingAttrs))*(1.0/6)*0.6 +
		float64(len(outrageSixthAttrs))*(-1.0/6)*0.3 +
		float64(len(outrageEighteenthAttrs))*(-1.0/18)*0.9 +
		float64(len(toxicEighthAttrs))*(-1.0/8)*0.4
	assertClose(t, PRCScore(attrs), (raw+1.0)/2.0)

	// The same literal the api pins in
	// perspective_test.py::test_matches_the_ingest_implementation. Deriving
	// the expectation from the weights, as above, cannot catch the two repos
	// drifting together in the same direction; a shared constant can.
	assertClose(t, PRCScore(attrs), 0.575)
}

// The rescale from raw to [0, 1] assumes bounds of exactly (-1, +1). If a
// weight changes and that stops holding, every stored score shifts silently.
func TestRawScoreBoundsAreSymmetric(t *testing.T) {
	lo, hi := rawScoreBounds()
	assertClose(t, lo, -1.0)
	assertClose(t, hi, 1.0)
}

func TestRequestedAttributesCoverEveryWeight(t *testing.T) {
	if len(RequestedAttributes) != len(prcWeights) {
		t.Fatalf("requesting %d attributes for %d weights", len(RequestedAttributes), len(prcWeights))
	}
	for _, name := range RequestedAttributes {
		if _, ok := prcWeights[name]; !ok {
			t.Errorf("requested attribute %q has no weight", name)
		}
	}
	// Sorted, so a captured request body diffs cleanly between runs.
	for i := 1; i < len(RequestedAttributes); i++ {
		if RequestedAttributes[i-1] >= RequestedAttributes[i] {
			t.Fatalf("RequestedAttributes not sorted at %d: %q >= %q", i, RequestedAttributes[i-1], RequestedAttributes[i])
		}
	}
}

// SEVERE_TOXICITY is not part of the PRC formula. It is an easy attribute to
// add by reflex, and adding it would change every score.
func TestSevereToxicityIsNotRequested(t *testing.T) {
	if _, ok := prcWeights["SEVERE_TOXICITY"]; ok {
		t.Error("SEVERE_TOXICITY is not part of perspective_baseline_minus_outrage_toxic")
	}
}

func TestStorageKeysMatchRequestedAttributes(t *testing.T) {
	keys := StorageKeys()
	if len(keys) != len(RequestedAttributes) {
		t.Fatalf("got %d storage keys for %d attributes", len(keys), len(RequestedAttributes))
	}
	for i, key := range keys {
		if key != storageKey(RequestedAttributes[i]) {
			t.Errorf("storage key %d is %q, want %q", i, key, storageKey(RequestedAttributes[i]))
		}
	}
	if got := storageKey("MORAL_OUTRAGE_EXPERIMENTAL"); got != "moral_outrage_experimental" {
		t.Errorf("storageKey lowercases: got %q", got)
	}
}
