package cacheotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
