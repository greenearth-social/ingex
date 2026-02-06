package common

import (
	"context"
	"strings"
	"sync"
	"time"

	gcpmetric "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"

	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
)

// OTelMetricCollector exports metrics via OpenTelemetry to GCP Cloud Monitoring or stdout.
type OTelMetricCollector struct {
	meter      metric.Meter
	provider   *sdkmetric.MeterProvider
	mu         sync.RWMutex
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64Gauge
}

// NewOTelMetricCollector creates a new OTel-based metric collector.
// When projectID is non-empty, metrics are exported to GCP Cloud Monitoring.
// When projectID is empty, metrics are printed to stdout via the OTel stdout exporter.
func NewOTelMetricCollector(serviceName, env, projectID, region string, exportIntervalSec int) (*OTelMetricCollector, error) {
	var exporter sdkmetric.Exporter
	var err error

	if projectID != "" {
		exporter, err = gcpmetric.New(gcpmetric.WithProjectID(projectID))
		if err != nil {
			return nil, err
		}
	} else {
		exporter, err = stdoutmetric.New()
		if err != nil {
			return nil, err
		}
	}

	res, err := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.DeploymentEnvironment(env),
			semconv.CloudRegion(region),
		),
	)
	if err != nil {
		return nil, err
	}

	interval := time.Duration(exportIntervalSec) * time.Second
	reader := sdkmetric.NewPeriodicReader(exporter, sdkmetric.WithInterval(interval))

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	meter := provider.Meter("greenearth/ingex")

	return &OTelMetricCollector{
		meter:      meter,
		provider:   provider,
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Float64Gauge),
	}, nil
}

// newOTelMetricCollectorWithReader creates a collector with a manual reader for testing.
func newOTelMetricCollectorWithReader(reader sdkmetric.Reader, serviceName, env string) *OTelMetricCollector {
	res, _ := resource.New(
		context.Background(),
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.DeploymentEnvironment(env),
		),
	)

	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)

	meter := provider.Meter("greenearth/ingex")

	return &OTelMetricCollector{
		meter:      meter,
		provider:   provider,
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Float64Gauge),
	}
}

// Record records a metric value. Instruments are lazily created based on the metric name suffix:
// - Names ending in "_ms" or "_sec" → histogram
// - Names ending in "hit_rate" → gauge
// - All others → histogram
func (c *OTelMetricCollector) Record(name string, value float64) {
	if isGaugeMetric(name) {
		gauge := c.getOrCreateGauge(name)
		gauge.Record(context.Background(), value)
	} else {
		hist := c.getOrCreateHistogram(name)
		hist.Record(context.Background(), value)
	}
}

// Shutdown flushes pending metrics and shuts down the provider.
func (c *OTelMetricCollector) Shutdown(ctx context.Context) error {
	return c.provider.Shutdown(ctx)
}

func isGaugeMetric(name string) bool {
	return strings.HasSuffix(name, "hit_rate")
}

func (c *OTelMetricCollector) getOrCreateHistogram(name string) metric.Float64Histogram {
	c.mu.RLock()
	h, ok := c.histograms[name]
	c.mu.RUnlock()
	if ok {
		return h
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if h, ok := c.histograms[name]; ok {
		return h
	}
	h, _ = c.meter.Float64Histogram(name)
	c.histograms[name] = h
	return h
}

func (c *OTelMetricCollector) getOrCreateGauge(name string) metric.Float64Gauge {
	c.mu.RLock()
	g, ok := c.gauges[name]
	c.mu.RUnlock()
	if ok {
		return g
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if g, ok := c.gauges[name]; ok {
		return g
	}
	g, _ = c.meter.Float64Gauge(name)
	c.gauges[name] = g
	return g
}
