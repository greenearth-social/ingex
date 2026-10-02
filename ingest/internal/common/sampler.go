package common

import "hash/fnv"

var ingestSampleDenominator uint32 = 10

// ShouldSampleDID returns true if the DID should be ingested. In the stage
// environment, only ~10% of DIDs (by FNV-32a bucket) are retained to reduce
// cluster costs. In all other environments every DID is kept.
func ShouldSampleDID(did, environment string) bool {
	if environment != "stage" {
		return true
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(did))
	return h.Sum32()%ingestSampleDenominator == 0
}
