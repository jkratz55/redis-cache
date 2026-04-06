package cacheotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

type commonConfig struct {
	meterProvider      metric.MeterProvider
	durationBoundaries []float64
}

type config struct {
	commonConfig
	traceProvider     trace.TracerProvider
	mgetKeyBoundaries []float64
}

func newCommonConfig() *commonConfig {
	return &commonConfig{
		meterProvider:      otel.GetMeterProvider(),
		durationBoundaries: []float64{0.005, 0.010, 0.025, 0.050, 0.100},
	}
}

func newConfig() *config {
	return &config{
		commonConfig:      *newCommonConfig(),
		traceProvider:     otel.GetTracerProvider(),
		mgetKeyBoundaries: []float64{100, 250, 500, 1000, 2500, 5000, 10000, 20000, 40000, 80000},
	}
}

// Option allows for the Cache behavior/configuration to be customized.
type Option interface {
	apply(*config)
}

// SerializerOption allows for the Serializer behavior/configuration to be customized.
type SerializerOption interface {
	applySerializer(*commonConfig)
}

// CompressionOption allows for the CompressionCodec behavior/configuration to be customized.
type CompressionOption interface {
	applyCompression(*commonConfig)
}

// InstrumentOption allows for the Cache, Serializer, and Compression behavior/configuration to be customized.
type InstrumentOption interface {
	Option
	SerializerOption
	CompressionOption
}

type otelOption struct {
	applyFn            func(*config)
	applySerializerFn  func(*commonConfig)
	applyCompressionFn func(*commonConfig)
}

func (o otelOption) apply(c *config)                  { o.applyFn(c) }
func (o otelOption) applySerializer(c *commonConfig)  { o.applySerializerFn(c) }
func (o otelOption) applyCompression(c *commonConfig) { o.applyCompressionFn(c) }

type cacheOption struct {
	applyFn func(*config)
}

func (o cacheOption) apply(c *config) { o.applyFn(c) }

func WithMeterProvider(mp metric.MeterProvider) InstrumentOption {
	return otelOption{
		applyFn: func(c *config) {
			c.meterProvider = mp
		},
		applySerializerFn: func(c *commonConfig) {
			c.meterProvider = mp
		},
		applyCompressionFn: func(c *commonConfig) {
			c.meterProvider = mp
		},
	}
}

func WithMgetKeyBoundaries(boundaries ...float64) Option {
	return cacheOption{
		applyFn: func(c *config) {
			c.mgetKeyBoundaries = boundaries
		},
	}
}

func WithDurationBoundaries(boundaries ...float64) InstrumentOption {
	return otelOption{
		applyFn: func(c *config) {
			c.durationBoundaries = boundaries
		},
		applySerializerFn: func(c *commonConfig) {
			c.durationBoundaries = boundaries
		},
		applyCompressionFn: func(c *commonConfig) {
			c.durationBoundaries = boundaries
		},
	}
}

func WithTraceProvider(tp trace.TracerProvider) Option {
	return cacheOption{
		applyFn: func(c *config) {
			c.traceProvider = tp
		},
	}
}
