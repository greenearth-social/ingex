package common

import (
	"math"
	"sync"
	"time"
)

// MetricCollector records metric values and provides summaries
type MetricCollector interface {
	Record(name string, value float64)
	Summary(name string) *MetricSummary
	Reset(name string)
}

// MetricSummary holds aggregated statistics for a named metric
type MetricSummary struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
	Avg   float64
	Last  float64
}

type metricState struct {
	count int64
	sum   float64
	min   float64
	max   float64
	last  float64
}

// InMemoryMetricCollector is a thread-safe in-memory implementation of MetricCollector
type InMemoryMetricCollector struct {
	mu      sync.RWMutex
	metrics map[string]*metricState
}

// NewInMemoryMetricCollector creates a new InMemoryMetricCollector
func NewInMemoryMetricCollector() *InMemoryMetricCollector {
	return &InMemoryMetricCollector{
		metrics: make(map[string]*metricState),
	}
}

// Record adds a value to the named metric
func (c *InMemoryMetricCollector) Record(name string, value float64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	s, ok := c.metrics[name]
	if !ok {
		c.metrics[name] = &metricState{
			count: 1,
			sum:   value,
			min:   value,
			max:   value,
			last:  value,
		}
		return
	}

	s.count++
	s.sum += value
	s.last = value
	if value < s.min {
		s.min = value
	}
	if value > s.max {
		s.max = value
	}
}

// Summary returns aggregated statistics for the named metric, or nil if not found
func (c *InMemoryMetricCollector) Summary(name string) *MetricSummary {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.metrics[name]
	if !ok {
		return nil
	}

	avg := 0.0
	if s.count > 0 {
		avg = s.sum / float64(s.count)
	}

	return &MetricSummary{
		Count: s.count,
		Sum:   s.sum,
		Min:   s.min,
		Max:   s.max,
		Avg:   math.Round(avg*100) / 100,
		Last:  s.last,
	}
}

// Reset removes the named metric from the collector
func (c *InMemoryMetricCollector) Reset(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.metrics, name)
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
