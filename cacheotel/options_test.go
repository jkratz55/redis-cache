package cacheotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
)

func TestOptions(t *testing.T) {
	mp := metricnoop.NewMeterProvider()

	t.Run("Serializer options", func(t *testing.T) {
		// This tests that common options can be passed to NewInstrumentedSerializer
		s := NewInstrumentedSerializer(&cache.JsonSerializer{},
			WithMeterProvider(mp),
			WithDurationBoundaries(0.1, 0.2),
		)
		assert.NotNil(t, s)
	})

	t.Run("Compression options", func(t *testing.T) {
		// This tests that common options can be passed to NewInstrumentedCompressionCodec
		c := NewInstrumentedCompressionCodec(gzip.NewCodec(6),
			WithMeterProvider(mp),
			WithDurationBoundaries(0.1, 0.2),
		)
		assert.NotNil(t, c)
	})
}

func TestOptions_Apply(t *testing.T) {
	mp := metricnoop.NewMeterProvider()
	conf := newCommonConfig()
	assert.Equal(t, defaultDurationBoundaries, conf.durationBoundaries)
	assert.Equal(t, defaultSizeBoundaries, conf.sizeBoundaries)
	assert.Empty(t, conf.attributes)

	for _, opt := range []InstrumentOption{
		WithMeterProvider(mp),
		WithDurationBoundaries(0.1, 0.2),
		WithSizeBoundaries(10, 20),
		WithAttributes(attribute.String("a", "1")),
		WithAttributes(attribute.String("b", "2")),
	} {
		opt.apply(conf)
	}
	assert.Equal(t, mp, conf.meterProvider)
	assert.Equal(t, []float64{0.1, 0.2}, conf.durationBoundaries)
	assert.Equal(t, []float64{10, 20}, conf.sizeBoundaries)
	assert.Equal(t, []attribute.KeyValue{attribute.String("a", "1"), attribute.String("b", "2")}, conf.attributes)
}
