package cacheotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

type commonConfig struct {
	meterProvider      metric.MeterProvider
	durationBoundaries []float64
}

func newCommonConfig() *commonConfig {
	return &commonConfig{
		meterProvider:      otel.GetMeterProvider(),
		durationBoundaries: []float64{0.005, 0.010, 0.025, 0.050, 0.100},
	}
}

// SerializerOption allows for the Serializer behavior/configuration to be customized.
type SerializerOption interface {
	applySerializer(*commonConfig)
}

// CompressionOption allows for the CompressionCodec behavior/configuration to be customized.
type CompressionOption interface {
	applyCompression(*commonConfig)
}

// InstrumentOption allows for the Serializer and Compression behavior/configuration to be customized.
type InstrumentOption interface {
	SerializerOption
	CompressionOption
}

type otelOption struct {
	applySerializerFn  func(*commonConfig)
	applyCompressionFn func(*commonConfig)
}

func (o otelOption) applySerializer(c *commonConfig)  { o.applySerializerFn(c) }
func (o otelOption) applyCompression(c *commonConfig) { o.applyCompressionFn(c) }

func WithMeterProvider(mp metric.MeterProvider) InstrumentOption {
	return otelOption{
		applySerializerFn: func(c *commonConfig) {
			c.meterProvider = mp
		},
		applyCompressionFn: func(c *commonConfig) {
			c.meterProvider = mp
		},
	}
}

func WithDurationBoundaries(boundaries ...float64) InstrumentOption {
	return otelOption{
		applySerializerFn: func(c *commonConfig) {
			c.durationBoundaries = boundaries
		},
		applyCompressionFn: func(c *commonConfig) {
			c.durationBoundaries = boundaries
		},
	}
}
