package cacheotel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// defaultDurationBoundaries are the default bucket boundaries, in seconds, of the
// duration histograms. Serialization and compression usually take microseconds,
// so they range from 10µs to 50ms.
var defaultDurationBoundaries = []float64{
	0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05,
}

// defaultSizeBoundaries are the default bucket boundaries, in bytes, of the value
// size histograms. They range from 64B to 16MiB.
var defaultSizeBoundaries = []float64{
	64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216,
}

type commonConfig struct {
	meterProvider      metric.MeterProvider
	durationBoundaries []float64
	sizeBoundaries     []float64
	attributes         []attribute.KeyValue
}

func newCommonConfig() *commonConfig {
	return &commonConfig{
		meterProvider:      otel.GetMeterProvider(),
		durationBoundaries: defaultDurationBoundaries,
		sizeBoundaries:     defaultSizeBoundaries,
	}
}

// attributeSet returns a measurement option with the configured attributes and
// attrs. It is computed once, so recording a measurement doesn't allocate.
func (c *commonConfig) attributeSet(attrs ...attribute.KeyValue) metric.MeasurementOption {
	all := make([]attribute.KeyValue, 0, len(c.attributes)+len(attrs))
	all = append(all, c.attributes...)
	all = append(all, attrs...)
	return metric.WithAttributeSet(attribute.NewSet(all...))
}

// SerializerOption allows for the Serializer behavior/configuration to be customized.
type SerializerOption interface {
	applySerializer(*commonConfig)
}

// CompressionOption allows for the CompressionCodec behavior/configuration to be customized.
type CompressionOption interface {
	applyCompression(*commonConfig)
}

// InstrumentOption customizes the instrumentation created by NewInstrumentedSerializer,
// NewInstrumentedCompressionCodec, NewHooks and ObserveFailoverClient. Options that don't
// apply to an instrumentation, such as WithDurationBoundaries for NewHooks, are ignored.
type InstrumentOption interface {
	SerializerOption
	CompressionOption
	apply(*commonConfig)
}

type otelOption func(*commonConfig)

func (o otelOption) apply(c *commonConfig)            { o(c) }
func (o otelOption) applySerializer(c *commonConfig)  { o(c) }
func (o otelOption) applyCompression(c *commonConfig) { o(c) }

// WithMeterProvider sets the MeterProvider used to create the instruments.
//
// Default is the global MeterProvider, see otel.GetMeterProvider.
func WithMeterProvider(mp metric.MeterProvider) InstrumentOption {
	return otelOption(func(c *commonConfig) {
		c.meterProvider = mp
	})
}

// WithDurationBoundaries sets the bucket boundaries, in seconds, of the
// cache.serialization.duration and cache.compression.duration histograms.
//
// Default ranges from 10µs to 50ms.
func WithDurationBoundaries(boundaries ...float64) InstrumentOption {
	return otelOption(func(c *commonConfig) {
		c.durationBoundaries = boundaries
	})
}

// WithSizeBoundaries sets the bucket boundaries, in bytes, of the cache.value.size and
// cache.value.serialized_size histograms recorded by NewHooks.
//
// Default ranges from 64B to 16MiB.
func WithSizeBoundaries(boundaries ...float64) InstrumentOption {
	return otelOption(func(c *commonConfig) {
		c.sizeBoundaries = boundaries
	})
}

// WithAttributes adds attributes to every measurement, for example to identify the
// cache or the service using it:
//
//	cacheotel.WithAttributes(attribute.String("cache.name", "users"))
//
// The attributes should have a low cardinality.
func WithAttributes(attrs ...attribute.KeyValue) InstrumentOption {
	return otelOption(func(c *commonConfig) {
		c.attributes = append(c.attributes, attrs...)
	})
}
