package cacheotel

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
)

type noIDCodec struct{}

func (noIDCodec) Compress(data []byte) ([]byte, error) { return data, nil }

func (noIDCodec) Decompress(data []byte) ([]byte, error) { return data, nil }

// reservedIDCodec is a custom implementation illegally claiming a reserved ID.
type reservedIDCodec struct{ noIDCodec }

func (reservedIDCodec) FormatID() uint8 { return cache.CodecNone }

func TestInstrumentedCompressionCodec_FormatID(t *testing.T) {
	c := NewInstrumentedCompressionCodec(gzip.NewCodec(6))
	id, ok := cache.FormatIDOf(c)
	assert.True(t, ok)
	assert.Equal(t, cache.CodecGzip, id)

	c = NewInstrumentedCompressionCodec(cache.CodecWithID(201, noIDCodec{}))
	assert.Equal(t, uint8(201), c.FormatID())
}

func TestInstrumentedCompressionCodec_NoFormatIDPanics(t *testing.T) {
	assert.PanicsWithError(t,
		"CompressionCodec cacheotel.noIDCodec has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.CodecWithID",
		func() {
			NewInstrumentedCompressionCodec(noIDCodec{})
		})
	assert.Panics(t, func() {
		NewInstrumentedCompressionCodec(nil)
	})
	assert.Panics(t, func() {
		NewInstrumentedCompressionCodec((*gzip.Codec)(nil))
	})
}

func TestInstrumentedCompressionCodec_Unwrap(t *testing.T) {
	inner := gzip.NewCodec(6)
	assert.Same(t, inner, NewInstrumentedCompressionCodec(inner).Unwrap())
}

// TestInstrumentedCompressionCodec_FormatChecks verifies the cache checks the
// format ID of the wrapped CompressionCodec rather than the instrumentation wrapper.
func TestInstrumentedCompressionCodec_FormatChecks(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	// An instrumented codec and the codec it wraps are the same format.
	assert.NotPanics(t, func() {
		cache.New(client,
			cache.Compression(NewInstrumentedCompressionCodec(gzip.NewCodec(6))),
			cache.DecodeCompression(gzip.NewCodec(6)))
	})

	// Instrumentation doesn't allow a custom codec to use a reserved ID.
	assert.Panics(t, func() {
		cache.New(client, cache.Compression(NewInstrumentedCompressionCodec(reservedIDCodec{})))
	})
}

func TestInstrumentedCompressionCodec_CacheWritesInnerFormatID(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	ctx := context.Background()
	c := cache.New(client, cache.Compression(NewInstrumentedCompressionCodec(gzip.NewCodec(6))))

	assert.NoError(t, c.Set(ctx, "key", "value", 0))

	raw, err := client.Get(ctx, "key").Bytes()
	assert.NoError(t, err)
	if assert.GreaterOrEqual(t, len(raw), 4) {
		assert.Equal(t, []byte{0xC1, 0x01, cache.SerializerMsgPack, cache.CodecGzip}, raw[:4])
	}

	var s string
	assert.NoError(t, c.Get(ctx, "key", &s))
	assert.Equal(t, "value", s)

	// A cache using the plain (non-instrumented) codec reads the same value.
	plain := cache.NewTyped[string](client, cache.DecodeCompression(gzip.NewCodec(6)))
	s, err = plain.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, "value", s)
}

func TestInstrumentedCompressionCodec_Metrics(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	name := attribute.String("cache.name", "users")
	c := NewInstrumentedCompressionCodec(gzip.NewCodec(6), WithMeterProvider(mp), WithAttributes(name))

	compressed, err := c.Compress([]byte("value"))
	assert.NoError(t, err)
	// gzip.Codec implements cache.CompressAppender, so it is forwarded.
	out, err := c.AppendCompress([]byte("prefix"), []byte("value"))
	assert.NoError(t, err)
	assert.Equal(t, append([]byte("prefix"), compressed...), out)

	_, err = c.Decompress(compressed)
	assert.NoError(t, err)
	_, err = c.Decompress([]byte("corrupt"))
	assert.Error(t, err)

	metrics := collect(t, reader)
	codec := attribute.String("cache.codec", "gzip")
	compress := attribute.String("operation", "compress")
	decompress := attribute.String("operation", "decompress")
	duration := metrics["cache.compression.duration"]
	assert.Equal(t, uint64(2), histogramCount(t, duration, name, codec, compress, attribute.Bool("success", true)))
	assert.Equal(t, uint64(1), histogramCount(t, duration, name, codec, decompress, attribute.Bool("success", true)))
	assert.Equal(t, uint64(1), histogramCount(t, duration, name, codec, decompress, attribute.Bool("success", false)))
	assert.Equal(t, int64(1), counterValue(t, metrics["cache.compression.errors"], name, codec, decompress))
}

func TestInstrumentedCompressionCodec_AppendCompressFallback(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	// cache.CodecWithID forwards AppendCompress, noIDCodec doesn't implement it.
	c := NewInstrumentedCompressionCodec(cache.CodecWithID(201, noIDCodec{}), WithMeterProvider(mp))

	out, err := c.AppendCompress([]byte("prefix:"), []byte("value"))
	assert.NoError(t, err)
	assert.Equal(t, "prefix:value", string(out))

	duration := collect(t, reader)["cache.compression.duration"]
	assert.Equal(t, uint64(1), histogramCount(t, duration, attribute.String("cache.codec", "201"),
		attribute.String("operation", "compress"), attribute.Bool("success", true)))
}
