package common

import (
	"time"
)

// MetricCollector records metric values
type MetricCollector interface {
	Record(name string, value float64)
}

// CalculateFreshness returns the lag in seconds between the given microsecond timestamp and now
func CalculateFreshness(timeUs int64) int64 {
	if timeUs == 0 {
		return 0
	}
	nowUs := time.Now().UnixMicro()
	lagUs := nowUs - timeUs
	return lagUs / 1_000_000
}
