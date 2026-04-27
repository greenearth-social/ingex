package common

import "hash/fnv"

var ingestSampleDenominator uint32 = 10

// ShouldSampleDID returns true if the DID falls into the sampled bucket (hash % denominator == 0).
func ShouldSampleDID(did string) bool {
	h := fnv.New32a()
	_, _ = h.Write([]byte(did))
	return h.Sum32()%ingestSampleDenominator == 0
}
