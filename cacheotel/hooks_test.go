package cacheotel

import (
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
)

func TestNewHooks(t *testing.T) {
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	mp, reader := newTestMeterProvider(t)
	name := attribute.String("cache.name", "users")
	hooks := NewHooks(WithMeterProvider(mp), WithAttributes(name))

	ctx := context.Background()
	c := cache.New(client, cache.Instrumentation(hooks))
	assert.NoError(t, c.Set(ctx, "a", "value", 0))
	assert.NoError(t, c.Set(ctx, "b", "value", 0))

	var val string
	assert.NoError(t, c.Get(ctx, "a", &val))
	assert.ErrorIs(t, c.Get(ctx, "missing", &val), cache.ErrKeyNotFound)
	_, err := cache.MGet[string](ctx, c, "a", "b", "missing")
	assert.NoError(t, err)

	// A corrupt value, a value in a format that isn't registered, and a value
	// exceeding the size limit of its codec.
	assert.NoError(t, client.Set(ctx, "corrupt", []byte{0xC1, 0x01, cache.SerializerMsgPack, cache.CodecNone, 0xC1}, 0).Err())
	assert.NoError(t, client.Set(ctx, "unsupported", []byte{0xC1, 0x01, 250, cache.CodecNone, 0x01}, 0).Err())
	assert.ErrorIs(t, c.Get(ctx, "unsupported", &val), cache.ErrUnsupportedFormat)
	assert.Error(t, c.Get(ctx, "corrupt", &val))
	limited := cache.New(client, cache.Compression(s2.NewCodec(s2.WithMaxDecodedSize(16))), cache.Instrumentation(hooks))
	assert.NoError(t, limited.Set(ctx, "large", strings.Repeat("a", 100), 0))
	assert.ErrorIs(t, limited.Get(ctx, "large", &val), cache.ErrMaxDecodedSizeExceeded)

	_, err = cache.ReadThroughSingleFlight[string](ctx, c, "loaded", cache.ReadThroughOptions{},
		func(ctx context.Context) (string, error) {
			return "value", nil
		})
	assert.NoError(t, err)

	metrics := collect(t, reader)

	lookups := metrics["cache.lookups"]
	// Get a, MGet a and b, Get unsupported, Get corrupt, Get large.
	assert.Equal(t, int64(6), counterValue(t, lookups, name, attribute.Bool("cache.hit", true)))
	// Get missing, MGet missing, ReadThroughSingleFlight loaded.
	assert.Equal(t, int64(3), counterValue(t, lookups, name, attribute.Bool("cache.hit", false)))

	size := metrics["cache.value.size"]
	// Set a, b, large, and loaded by the read-through.
	assert.Equal(t, uint64(4), histogramCount(t, size, name, attribute.String("operation", "encode")))
	assert.Equal(t, uint64(6), histogramCount(t, size, name, attribute.String("operation", "decode")))
	assert.Equal(t, uint64(4), histogramCount(t, metrics["cache.value.serialized_size"], name))
	assert.Equal(t, defaultSizeBoundaries, histogramBounds(t, size))

	decodeErrs := metrics["cache.decode.errors"]
	assert.Equal(t, int64(1), counterValue(t, decodeErrs, name, attribute.String("reason", "unsupported_format")))
	assert.Equal(t, int64(1), counterValue(t, decodeErrs, name, attribute.String("reason", "size_limit")))
	assert.Equal(t, int64(1), counterValue(t, decodeErrs, name, attribute.String("reason", "invalid")))

	singleFlight := metrics["cache.singleflight.calls"]
	assert.Equal(t, int64(1), counterValue(t, singleFlight, name, attribute.Bool("shared", false)))
	assert.Equal(t, int64(0), counterValue(t, singleFlight, name, attribute.Bool("shared", true)))
}

func TestNewHooks_SizeBoundaries(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	hooks := NewHooks(WithMeterProvider(mp), WithSizeBoundaries(10, 100))
	hooks.OnEncode(context.Background(), 5, 50)

	metrics := collect(t, reader)
	assert.Equal(t, []float64{10, 100}, histogramBounds(t, metrics["cache.value.size"]))
	assert.Equal(t, []float64{10, 100}, histogramBounds(t, metrics["cache.value.serialized_size"]))
}
