package common

import (
	"io"
	"log"
	"os"
)

// IngestLogger implements the Logger interface with configurable output
type IngestLogger struct {
	infoLogger      *log.Logger
	errorLogger     *log.Logger
	debugLogger     *log.Logger
	metricLogger    *log.Logger
	metricCollector MetricCollector
	samplingRatio   float64
	metricCounts    map[string]int64
	enabled         bool
	debugEnabled    bool
	gitSHA          string
}

// NewLogger creates a new logger with configurable output destinations
func NewLogger(enabled bool) *IngestLogger {
	gitSHA := os.Getenv("GE_GIT_SHA")
	var prefix string
	if gitSHA != "" {
		prefix = "[" + gitSHA + "] "
	}

	return &IngestLogger{
		infoLogger:   log.New(os.Stdout, prefix+"[INFO] ", 0),
		errorLogger:  log.New(os.Stderr, prefix+"[ERROR] ", 0),
		debugLogger:  log.New(os.Stdout, prefix+"[DEBUG] ", 0),
		metricLogger: log.New(os.Stdout, prefix+"[METRIC] ", 0),
		metricCounts: make(map[string]int64),
		enabled:      enabled,
		debugEnabled: false,
		gitSHA:       gitSHA,
	}
}

// Info logs an informational message
func (l *IngestLogger) Info(msg string, args ...interface{}) {
	if !l.enabled {
		return
	}
	l.infoLogger.Printf(msg, args...)
}

// Error logs an error message
func (l *IngestLogger) Error(msg string, args ...interface{}) {
	if !l.enabled {
		return
	}
	l.errorLogger.Printf(msg, args...)
}

// Debug logs a debug message
func (l *IngestLogger) Debug(msg string, args ...interface{}) {
	if !l.enabled || !l.debugEnabled {
		return
	}
	l.debugLogger.Printf(msg, args...)
}

// SetDebugEnabled enables or disables debug logging
func (l *IngestLogger) SetDebugEnabled(enabled bool) {
	l.debugEnabled = enabled
}

// SetMetricCollector configures the metric collector and sampling ratio.
// samplingRatio controls how often metrics are logged (1.0 = every observation, 0.01 = every 100th).
func (l *IngestLogger) SetMetricCollector(mc MetricCollector, samplingRatio float64) {
	l.metricCollector = mc
	l.samplingRatio = samplingRatio
}

// Metric records a metric value and periodically logs a summary based on the sampling ratio.
func (l *IngestLogger) Metric(name string, value float64) {
	if !l.enabled || l.metricCollector == nil {
		return
	}

	l.metricCollector.Record(name, value)

	if l.samplingRatio <= 0 {
		return
	}

	l.metricCounts[name]++

	interval := int64(1.0 / l.samplingRatio)
	if interval < 1 {
		interval = 1
	}

	if l.metricCounts[name]%interval == 0 {
		summary := l.metricCollector.Summary(name)
		if summary != nil {
			l.metricLogger.Printf("%s=%.2f (count=%d, avg=%.2f, min=%.2f, max=%.2f)",
				name, value, summary.Count, summary.Avg, summary.Min, summary.Max)
		}
	}
}

// SetOutput sets the output destination for all loggers
func (l *IngestLogger) SetOutput(w io.Writer) {
	l.infoLogger.SetOutput(w)
	l.errorLogger.SetOutput(w)
	l.debugLogger.SetOutput(w)
	l.metricLogger.SetOutput(w)
}
