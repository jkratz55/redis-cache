package cacheotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type config struct {
	meterProvider      metric.MeterProvider
	traceProvider      trace.TracerProvider
	mgetKeyBoundaries  []float64
	durationBoundaries []float64
}

func newConfig() *config {
	return &config{
		meterProvider:      otel.GetMeterProvider(),
		traceProvider:      otel.GetTracerProvider(),
		mgetKeyBoundaries:  []float64{100, 250, 500, 1000, 2500, 5000, 10000, 20000, 40000, 80000},
		durationBoundaries: []float64{0.005, 0.010, 0.025, 0.050, 0.100},
	}
}

type Option func(*config)

func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(c *config) {
		c.meterProvider = mp
	}
}

func WithMgetKeyBoundaries(boundaries ...float64) Option {
	return func(c *config) {
		c.mgetKeyBoundaries = boundaries
	}
}

func WithDurationBoundaries(boundaries ...float64) Option {
	return func(c *config) {
		c.durationBoundaries = boundaries
	}
}

func WithTraceProvider(tp trace.TracerProvider) Option {
	return func(c *config) {
		c.traceProvider = tp
	}
}
