package cacheotel

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
)

type noIDSerializer struct{}

func (noIDSerializer) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

func (noIDSerializer) Unmarshal(b []byte, v any) error { return msgpack.Unmarshal(b, v) }

type noIDCodec struct{}

func (noIDCodec) Compress(data []byte) ([]byte, error) { return data, nil }

func (noIDCodec) Decompress(data []byte) ([]byte, error) { return data, nil }

func TestInstrumented_FormatID(t *testing.T) {
	s := NewInstrumentedSerializer(&cache.JsonSerializer{})
	id, ok := cache.FormatIDOf(s)
	assert.True(t, ok)
	assert.Equal(t, cache.SerializerJSON, id)

	c := NewInstrumentedCompressionCodec(gzip.NewCodec(6))
	id, ok = cache.FormatIDOf(c)
	assert.True(t, ok)
	assert.Equal(t, cache.CodecGzip, id)

	s = NewInstrumentedSerializer(cache.SerializerWithID(200, noIDSerializer{}))
	assert.Equal(t, uint8(200), s.FormatID())

	c = NewInstrumentedCompressionCodec(cache.CodecWithID(201, noIDCodec{}))
	assert.Equal(t, uint8(201), c.FormatID())
}

func TestInstrumented_NoFormatIDPanics(t *testing.T) {
	assert.PanicsWithError(t,
		"Serializer cacheotel.noIDSerializer has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.SerializerWithID",
		func() {
			NewInstrumentedSerializer(noIDSerializer{})
		})
	assert.PanicsWithError(t,
		"CompressionCodec cacheotel.noIDCodec has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.CodecWithID",
		func() {
			NewInstrumentedCompressionCodec(noIDCodec{})
		})
	assert.Panics(t, func() {
		NewInstrumentedSerializer(nil)
	})
	assert.Panics(t, func() {
		NewInstrumentedCompressionCodec(nil)
	})
}

func TestInstrumented_CacheWritesInnerFormatID(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	ctx := context.Background()
	c := cache.New(client,
		cache.Serialization(NewInstrumentedSerializer(&cache.JsonSerializer{})),
		cache.Compression(NewInstrumentedCompressionCodec(gzip.NewCodec(6))))

	assert.NoError(t, c.Set(ctx, "key", "value", 0))

	raw, err := client.Get(ctx, "key").Bytes()
	assert.NoError(t, err)
	if assert.GreaterOrEqual(t, len(raw), 4) {
		assert.Equal(t, []byte{0xC1, 0x01, cache.SerializerJSON, cache.CodecGzip}, raw[:4])
	}

	var s string
	assert.NoError(t, c.Get(ctx, "key", &s))
	assert.Equal(t, "value", s)

	// A cache using the plain (non-instrumented) components reads the same value.
	plain := cache.NewTyped[string](client, cache.DecodeCompression(gzip.NewCodec(6)))
	s, err = plain.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, "value", s)
}
