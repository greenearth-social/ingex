package common

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func collectMetrics(t *testing.T, reader *metric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}
	return rm
}

func requireMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("Expected to find metric %s", name)
	return metricdata.Metrics{} // unreachable
}

func TestOTelMetricCollector_HistogramForDurationMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("es.bulk_index.duration_ms", 150.0)

	rm := collectMetrics(t, reader)
	m := requireMetric(t, rm, "es.bulk_index.duration_ms")
	if _, ok := m.Data.(metricdata.Histogram[float64]); !ok {
		t.Errorf("Expected histogram for _ms metric, got %T", m.Data)
	}
}

func TestOTelMetricCollector_HistogramForSecMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("jetstream.freshness_sec", 5.0)

	rm := collectMetrics(t, reader)
	m := requireMetric(t, rm, "jetstream.freshness_sec")
	if _, ok := m.Data.(metricdata.Histogram[float64]); !ok {
		t.Errorf("Expected histogram for _sec metric, got %T", m.Data)
	}
}

func TestOTelMetricCollector_GaugeForHitRateMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("cache.hit_rate", 0.95)

	rm := collectMetrics(t, reader)
	m := requireMetric(t, rm, "cache.hit_rate")
	if _, ok := m.Data.(metricdata.Gauge[float64]); !ok {
		t.Errorf("Expected gauge for hit_rate metric, got %T", m.Data)
	}
}

func TestOTelMetricCollector_DefaultHistogram(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("some.other.metric", 42.0)

	rm := collectMetrics(t, reader)
	m := requireMetric(t, rm, "some.other.metric")
	if _, ok := m.Data.(metricdata.Histogram[float64]); !ok {
		t.Errorf("Expected histogram for default metric, got %T", m.Data)
	}
}

func TestOTelMetricCollector_RecordMultipleValues(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("test.duration_ms", 100.0)
	collector.Record("test.duration_ms", 200.0)
	collector.Record("test.duration_ms", 300.0)

	rm := collectMetrics(t, reader)
	m := requireMetric(t, rm, "test.duration_ms")
	hist, ok := m.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("Expected histogram, got %T", m.Data)
	}
	if len(hist.DataPoints) != 1 {
		t.Fatalf("Expected 1 data point, got %d", len(hist.DataPoints))
	}
	dp := hist.DataPoints[0]
	if dp.Count != 3 {
		t.Errorf("Expected count 3, got %d", dp.Count)
	}
	if dp.Sum != 600.0 {
		t.Errorf("Expected sum 600.0, got %f", dp.Sum)
	}
}

func TestOTelMetricCollector_Shutdown(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("test.metric", 1.0)

	err := collector.Shutdown(context.Background())
	if err != nil {
		t.Errorf("Expected no error on shutdown, got %v", err)
	}
}

func TestOTelMetricCollector_MultipleMetrics(t *testing.T) {
	reader := metric.NewManualReader()
	collector := newOTelMetricCollectorWithReader(reader, "test-service", "local")

	collector.Record("a.duration_ms", 10.0)
	collector.Record("b.hit_rate", 0.5)
	collector.Record("c.other", 99.0)

	rm := collectMetrics(t, reader)

	requireMetric(t, rm, "a.duration_ms")
	requireMetric(t, rm, "b.hit_rate")
	requireMetric(t, rm, "c.other")
}
