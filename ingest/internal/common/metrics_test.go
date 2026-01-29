package common

import (
	"sync"
	"testing"
	"time"
)

func TestInMemoryMetricCollector_SingleValue(t *testing.T) {
	mc := NewInMemoryMetricCollector()
	mc.Record("test", 42.0)

	summary := mc.Summary("test")
	if summary == nil {
		t.Fatal("Expected summary, got nil")
	}
	if summary.Count != 1 {
		t.Errorf("Expected count 1, got %d", summary.Count)
	}
	if summary.Sum != 42.0 {
		t.Errorf("Expected sum 42.0, got %f", summary.Sum)
	}
	if summary.Min != 42.0 {
		t.Errorf("Expected min 42.0, got %f", summary.Min)
	}
	if summary.Max != 42.0 {
		t.Errorf("Expected max 42.0, got %f", summary.Max)
	}
	if summary.Avg != 42.0 {
		t.Errorf("Expected avg 42.0, got %f", summary.Avg)
	}
	if summary.Last != 42.0 {
		t.Errorf("Expected last 42.0, got %f", summary.Last)
	}
}

func TestInMemoryMetricCollector_MultipleValues(t *testing.T) {
	mc := NewInMemoryMetricCollector()
	mc.Record("test", 10.0)
	mc.Record("test", 20.0)
	mc.Record("test", 30.0)

	summary := mc.Summary("test")
	if summary == nil {
		t.Fatal("Expected summary, got nil")
	}
	if summary.Count != 3 {
		t.Errorf("Expected count 3, got %d", summary.Count)
	}
	if summary.Sum != 60.0 {
		t.Errorf("Expected sum 60.0, got %f", summary.Sum)
	}
	if summary.Min != 10.0 {
		t.Errorf("Expected min 10.0, got %f", summary.Min)
	}
	if summary.Max != 30.0 {
		t.Errorf("Expected max 30.0, got %f", summary.Max)
	}
	if summary.Avg != 20.0 {
		t.Errorf("Expected avg 20.0, got %f", summary.Avg)
	}
	if summary.Last != 30.0 {
		t.Errorf("Expected last 30.0, got %f", summary.Last)
	}
}

func TestInMemoryMetricCollector_UnknownMetric(t *testing.T) {
	mc := NewInMemoryMetricCollector()
	summary := mc.Summary("nonexistent")
	if summary != nil {
		t.Errorf("Expected nil for unknown metric, got %+v", summary)
	}
}

func TestInMemoryMetricCollector_Reset(t *testing.T) {
	mc := NewInMemoryMetricCollector()
	mc.Record("test", 100.0)
	mc.Reset("test")

	summary := mc.Summary("test")
	if summary != nil {
		t.Errorf("Expected nil after reset, got %+v", summary)
	}
}

func TestInMemoryMetricCollector_MultipleIndependentMetrics(t *testing.T) {
	mc := NewInMemoryMetricCollector()
	mc.Record("a", 1.0)
	mc.Record("b", 2.0)
	mc.Record("a", 3.0)

	summaryA := mc.Summary("a")
	summaryB := mc.Summary("b")

	if summaryA == nil || summaryB == nil {
		t.Fatal("Expected both summaries to exist")
	}
	if summaryA.Count != 2 {
		t.Errorf("Expected metric a count 2, got %d", summaryA.Count)
	}
	if summaryB.Count != 1 {
		t.Errorf("Expected metric b count 1, got %d", summaryB.Count)
	}
	if summaryA.Sum != 4.0 {
		t.Errorf("Expected metric a sum 4.0, got %f", summaryA.Sum)
	}
}

func TestInMemoryMetricCollector_ConcurrentAccess(t *testing.T) {
	mc := NewInMemoryMetricCollector()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(val float64) {
			defer wg.Done()
			mc.Record("concurrent", val)
		}(float64(i))
	}

	wg.Wait()

	summary := mc.Summary("concurrent")
	if summary == nil {
		t.Fatal("Expected summary, got nil")
	}
	if summary.Count != 100 {
		t.Errorf("Expected count 100, got %d", summary.Count)
	}
}

func TestCalculateFreshness(t *testing.T) {
	tests := []struct {
		name     string
		timeUs   int64
		expected int64
	}{
		{
			name:     "zero timestamp returns zero",
			timeUs:   0,
			expected: 0,
		},
		{
			name:   "recent timestamp returns small freshness",
			timeUs: time.Now().Add(-5 * time.Second).UnixMicro(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CalculateFreshness(tt.timeUs)
			if tt.timeUs == 0 {
				if result != 0 {
					t.Errorf("Expected 0 for zero timestamp, got %d", result)
				}
			} else {
				// For recent timestamps, freshness should be approximately 5 seconds
				if result < 4 || result > 6 {
					t.Errorf("Expected freshness ~5 seconds, got %d", result)
				}
			}
		})
	}
}
