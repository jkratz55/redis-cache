package cacheotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	cache "github.com/jkratz55/redis-cache/v2"
)

func TestOptions(t *testing.T) {
	mp := metricnoop.NewMeterProvider()
	tp := tracenoop.NewTracerProvider()

	t.Run("Cache options", func(t *testing.T) {
		// This tests that all options can be passed to Init
		Init(
			WithMeterProvider(mp),
			WithTraceProvider(tp),
			WithMgetKeyBoundaries(1, 2, 3),
			WithDurationBoundaries(0.1, 0.2),
		)
	})

	t.Run("Serializer options", func(t *testing.T) {
		// This tests that common options can be passed to NewInstrumentedSerializer
		s := NewInstrumentedSerializer(cache.JsonSerializer{},
			WithMeterProvider(mp),
			WithDurationBoundaries(0.1, 0.2),
		)
		assert.NotNil(t, s)
	})

	t.Run("Compression options", func(t *testing.T) {
		// This tests that common options can be passed to NewInstrumentedCompressionCodec
		c := NewInstrumentedCompressionCodec(nil,
			WithMeterProvider(mp),
			WithDurationBoundaries(0.1, 0.2),
		)
		assert.NotNil(t, c)
	})
}
