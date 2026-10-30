package cacheotel

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/all"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
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

func newAllCodecs(t *testing.T) *all.Codecs {
	t.Helper()
	codecs, err := all.NewCodecs()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { assert.NoError(t, codecs.Close()) })
	return codecs
}

func TestInstrumentCodecs_Wrapping(t *testing.T) {
	list := newAllCodecs(t).List()
	input := append([]cache.CompressionCodec(nil), list...)

	wrapped := InstrumentCodecs(list)
	if !assert.Len(t, wrapped, len(list)) {
		return
	}
	assert.Len(t, wrapped, 6)
	for i, codec := range wrapped {
		instrumented, ok := codec.(*InstrumentedCompressionCodec)
		if !assert.True(t, ok, "codec %d is %T", i, codec) {
			continue
		}
		assert.Same(t, list[i], instrumented.Unwrap())
		wantID, _ := cache.FormatIDOf(list[i])
		assert.Equal(t, wantID, instrumented.FormatID())
	}

	// The input isn't modified.
	for i := range list {
		assert.Same(t, input[i], list[i])
	}
}

func TestInstrumentCodecs_Empty(t *testing.T) {
	for _, codecs := range [][]cache.CompressionCodec{nil, {}} {
		wrapped := InstrumentCodecs(codecs)
		assert.NotNil(t, wrapped)
		assert.Empty(t, wrapped)
	}
}

func TestInstrumentCodecs_Panics(t *testing.T) {
	assert.PanicsWithError(t, "nil CompressionCodec", func() {
		InstrumentCodecs([]cache.CompressionCodec{gzip.NewCodec(6), nil})
	})
	assert.PanicsWithError(t, "nil CompressionCodec", func() {
		InstrumentCodecs([]cache.CompressionCodec{(*gzip.Codec)(nil)})
	})
	assert.PanicsWithError(t,
		"CompressionCodec cacheotel.noIDCodec has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.CodecWithID",
		func() {
			InstrumentCodecs([]cache.CompressionCodec{noIDCodec{}})
		})
}

// TestInstrumentCodecs_DecodeCompression reads a value written with MessagePack and
// Snappy using a JSON and LZ4 cache that registers the instrumented built-in codecs.
func TestInstrumentCodecs_DecodeCompression(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	writer := cache.New(client,
		cache.Serialization(&cache.MessagePackSerializer{}),
		cache.Compression(snappy.NewCodec()))
	assert.NoError(t, writer.Set(ctx, "key", "value", 0))

	mp, reader := newTestMeterProvider(t)
	name := attribute.String("cache.name", "users")
	c := cache.New(client,
		cache.Serialization(NewInstrumentedSerializer(&cache.JsonSerializer{})),
		cache.Compression(NewInstrumentedCompressionCodec(lz4.NewCodec())),
		cache.DecodeCompression(InstrumentCodecs(newAllCodecs(t).List(), WithMeterProvider(mp), WithAttributes(name))...))

	var s string
	assert.NoError(t, c.Get(ctx, "key", &s))
	assert.Equal(t, "value", s)

	duration := collect(t, reader)["cache.compression.duration"]
	assert.Equal(t, uint64(1), histogramCount(t, duration, name, attribute.String("cache.codec", "snappy"),
		attribute.String("operation", "decompress"), attribute.Bool("success", true)))
}

// TestInstrumentCodecs_ConfiguredCodec verifies the instrumented built-in codecs can
// be registered next to a configured instrumented codec of the same type.
func TestInstrumentCodecs_ConfiguredCodec(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	opts := []cache.Option{
		cache.Compression(NewInstrumentedCompressionCodec(lz4.NewCodec())),
		cache.DecodeCompression(InstrumentCodecs(newAllCodecs(t).List())...),
	}

	var c *cache.Cache
	assert.NotPanics(t, func() { c = cache.New(client, opts...) })
	var typed *cache.TypedCache[string]
	assert.NotPanics(t, func() { typed = cache.NewTyped[string](client, opts...) })
	if c == nil || typed == nil {
		return
	}

	assert.NoError(t, c.Set(ctx, "cache", "value", 0))
	assert.NoError(t, typed.Set(ctx, "typed", "value", 0))
	for _, key := range []string{"cache", "typed"} {
		raw, err := client.Get(ctx, key).Bytes()
		if assert.NoError(t, err) && assert.GreaterOrEqual(t, len(raw), 4) {
			assert.Equal(t, cache.CodecLZ4, raw[3])
		}
	}
}
