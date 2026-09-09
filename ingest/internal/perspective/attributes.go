// Package perspective scores post text with Google's Perspective API during
// ingestion, so the api does not have to score candidates on the serving path.
//
// It is laid out like internal/inference — client, batch, docs — because it
// solves the same shape of problem: an external HTTP service on the ingest hot
// path that must fan out, retry, and fail open without ever failing ingestion.
package perspective

import "strings"

// The PRC attribute weights below are duplicated, deliberately, in
// api/src/app/lib/perspective.py (_PRC_WEIGHTS). Both must agree: the api
// reads combined_perspective_score straight out of Elasticsearch for posts
// this package scored, and falls back to computing it itself for posts it did
// not. A divergence would make a post's score depend on which service
// happened to compute it. If you change one, change the other, and update the
// cross-language fixture test in each repo (attributes_test.go here,
// perspective_test.py there).
//
// This is `perspective_baseline_minus_outrage_toxic` from the PRC reference
// implementation (the PRC paper's "Uprank Bridging, Downrank Toxic"
// condition, the only one to reach statistical significance, p<0.05):
// https://github.com/HumanCompatibleAI/ranking-challenge-perspective/blob/main/perspective_ranker.py#L163-L179
//
// Note SEVERE_TOXICITY is not part of this formula and is intentionally
// omitted.
var prcWeights = map[string]float64{
	"REASONING_EXPERIMENTAL":      1.0 / 6,
	"PERSONAL_STORY_EXPERIMENTAL": 1.0 / 6,
	"AFFINITY_EXPERIMENTAL":       1.0 / 6,
	"COMPASSION_EXPERIMENTAL":     1.0 / 6,
	"RESPECT_EXPERIMENTAL":        1.0 / 6,
	"CURIOSITY_EXPERIMENTAL":      1.0 / 6,
	"FEARMONGERING_EXPERIMENTAL":  -1.0 / 6,
	"GENERALIZATION_EXPERIMENTAL": -1.0 / 6,
	"SCAPEGOATING_EXPERIMENTAL":   -1.0 / 18,
	"MORAL_OUTRAGE_EXPERIMENTAL":  -1.0 / 18,
	"ALIENATION_EXPERIMENTAL":     -1.0 / 18,
	"TOXICITY":                    -1.0 / 8,
	"IDENTITY_ATTACK":             -1.0 / 8,
	"INSULT":                      -1.0 / 8,
	"THREAT":                      -1.0 / 8,
}

// RequestedAttributes are the attribute names sent to the API, sorted so the
// request body is stable across runs (handy when diffing captured traffic).
var RequestedAttributes = sortedAttributeNames()

// prcWeightsByStorageKey is prcWeights re-keyed by storage key. Weights are
// declared under the API's names above so they diff cleanly against the api
// and the PRC reference implementation, but every consumer downstream of the
// HTTP response works in storage keys.
var prcWeightsByStorageKey = weightsByStorageKey()

// storageKey converts an API attribute name to the key the score is stored
// under in Elasticsearch and Parquet. Lower case only: SCREAMING_CASE field
// names are legal in ES but read badly everywhere they surface, and the
// mapping back is just strings.ToUpper.
func storageKey(attributeName string) string {
	return strings.ToLower(attributeName)
}

// StorageKeys are the stored score keys, in the same order as
// RequestedAttributes.
func StorageKeys() []string {
	keys := make([]string, len(RequestedAttributes))
	for i, name := range RequestedAttributes {
		keys[i] = storageKey(name)
	}
	return keys
}

func weightsByStorageKey() map[string]float64 {
	weights := make(map[string]float64, len(prcWeights))
	for name, weight := range prcWeights {
		weights[storageKey(name)] = weight
	}
	return weights
}

// ScoreBounds is the range the combined score is rescaled into before it is
// stored. Matches PERSPECTIVE_SCORE_BOUNDS in the api.
var ScoreBounds = [2]float64{0.0, 1.0}

func sortedAttributeNames() []string {
	names := make([]string, 0, len(prcWeights))
	for name := range prcWeights {
		names = append(names, name)
	}
	// Insertion sort: 15 elements, and avoiding a "sort" import keeps this
	// file free of anything but arithmetic.
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}

// rawScoreBounds returns the theoretical (min, max) of the weighted sum.
//
// Every attribute score is in [0, 1], so the sum is minimised when each
// negatively-weighted attribute is at 1.0 and each positively-weighted one at
// 0.0 — the sum of the negative weights — and maximised by the mirror image.
// For these weights that is exactly (-1.0, +1.0): the positive weights sum to
// 1.0 (6 * 1/6) and the negative to -1.0 (2*(-1/6) + 3*(-1/18) + 4*(-1/8)).
func rawScoreBounds() (float64, float64) {
	var lo, hi float64
	for _, w := range prcWeights {
		if w < 0 {
			lo += w
		} else {
			hi += w
		}
	}
	return lo, hi
}

// PRCScore combines raw attribute scores into a single score in ScoreBounds.
//
// attrs is keyed by storage key (lower case), which is what Client.Score
// returns and what is written to Elasticsearch. An attribute missing from
// attrs contributes zero — indistinguishable from a genuine 0.0, which for a
// negatively weighted attribute would read as "not toxic" — so Client.Score
// rejects partial responses rather than letting them reach here.
func PRCScore(attrs map[string]float64) float64 {
	var raw float64
	for key, weight := range prcWeightsByStorageKey {
		raw += weight * attrs[key]
	}
	lo, hi := rawScoreBounds()
	return ScoreBounds[0] + (raw-lo)*(ScoreBounds[1]-ScoreBounds[0])/(hi-lo)
}
