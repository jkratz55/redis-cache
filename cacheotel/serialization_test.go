package cacheotel

import (
	"context"
	"errors"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
	"go.opentelemetry.io/otel/attribute"

	cache "github.com/jkratz55/redis-cache/v2"
)

type noIDSerializer struct{}

func (noIDSerializer) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

func (noIDSerializer) Unmarshal(b []byte, v any) error { return msgpack.Unmarshal(b, v) }

type otherNoIDSerializer struct{ noIDSerializer }

// reservedIDSerializer is a custom implementation illegally claiming a reserved ID.
type reservedIDSerializer struct{ noIDSerializer }

func (reservedIDSerializer) FormatID() uint8 { return cache.SerializerMsgPack }

var errStrict = errors.New("strict unmarshal")

type strictSerializer struct{ noIDSerializer }

func (strictSerializer) UnmarshalStrict([]byte, any) error { return errStrict }

func (strictSerializer) FormatID() uint8 { return 200 }

func TestInstrumentedSerializer_FormatID(t *testing.T) {
	s := NewInstrumentedSerializer(&cache.JsonSerializer{})
	id, ok := cache.FormatIDOf(s)
	assert.True(t, ok)
	assert.Equal(t, cache.SerializerJSON, id)

	s = NewInstrumentedSerializer(cache.SerializerWithID(200, noIDSerializer{}))
	assert.Equal(t, uint8(200), s.FormatID())
}

func TestInstrumentedSerializer_NoFormatIDPanics(t *testing.T) {
	assert.PanicsWithError(t,
		"serializer cacheotel.noIDSerializer has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.SerializerWithID",
		func() {
			NewInstrumentedSerializer(noIDSerializer{})
		})
	assert.Panics(t, func() {
		NewInstrumentedSerializer(nil)
	})
	assert.Panics(t, func() {
		NewInstrumentedSerializer((*cache.JsonSerializer)(nil))
	})
}

func TestInstrumentedSerializer_Unwrap(t *testing.T) {
	inner := cache.SerializerWithID(200, noIDSerializer{})
	assert.Same(t, inner, NewInstrumentedSerializer(inner).Unwrap())
}

func TestInstrumentedSerializer_UnmarshalStrict(t *testing.T) {
	s := NewInstrumentedSerializer(strictSerializer{})
	assert.ErrorIs(t, s.UnmarshalStrict([]byte("data"), new(string)), errStrict)

	// Serializers that don't implement cache.StrictUnmarshaler use Unmarshal.
	s = NewInstrumentedSerializer(&cache.JsonSerializer{})
	var actual string
	assert.NoError(t, s.UnmarshalStrict([]byte(`"value"`), &actual))
	assert.Equal(t, "value", actual)
}

// TestInstrumentedSerializer_FormatChecks verifies the cache checks the format ID of
// the wrapped Serializer rather than the instrumentation wrapper.
func TestInstrumentedSerializer_FormatChecks(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	// Different serializers claiming the same ID conflict, even if both are instrumented.
	assert.Panics(t, func() {
		cache.New(client,
			cache.Serialization(NewInstrumentedSerializer(cache.SerializerWithID(200, noIDSerializer{}))),
			cache.DecodeSerializers(NewInstrumentedSerializer(cache.SerializerWithID(200, otherNoIDSerializer{}))))
	})

	// An instrumented serializer and the serializer it wraps are the same format.
	assert.NotPanics(t, func() {
		cache.New(client,
			cache.Serialization(NewInstrumentedSerializer(cache.JsonSerializer{})),
			cache.DecodeSerializers(cache.JsonSerializer{}))
	})

	// Instrumentation doesn't allow a custom serializer to use a reserved ID.
	assert.Panics(t, func() {
		cache.New(client, cache.Serialization(NewInstrumentedSerializer(reservedIDSerializer{})))
	})
}

func TestInstrumentedSerializer_CacheWritesInnerFormatID(t *testing.T) {
	server, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()

	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	defer client.Close()

	ctx := context.Background()
	c := cache.New(client, cache.Serialization(NewInstrumentedSerializer(&cache.JsonSerializer{})))

	assert.NoError(t, c.Set(ctx, "key", "value", 0))

	raw, err := client.Get(ctx, "key").Bytes()
	assert.NoError(t, err)
	if assert.GreaterOrEqual(t, len(raw), 4) {
		assert.Equal(t, []byte{0xC1, 0x01, cache.SerializerJSON, cache.CodecNone}, raw[:4])
	}

	var s string
	assert.NoError(t, c.Get(ctx, "key", &s))
	assert.Equal(t, "value", s)

	// A cache using the plain (non-instrumented) serializer reads the same value.
	plain := cache.NewTyped[string](client, cache.Serialization(&cache.JsonSerializer{}))
	s, err = plain.Get(ctx, "key")
	assert.NoError(t, err)
	assert.Equal(t, "value", s)
}

func TestInstrumentedSerializer_Metrics(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	name := attribute.String("cache.name", "users")
	s := NewInstrumentedSerializer(&cache.MessagePackSerializer{}, WithMeterProvider(mp), WithAttributes(name))

	data, err := s.Marshal("value")
	assert.NoError(t, err)
	// MessagePackSerializer implements cache.MarshalAppender, so it is forwarded.
	out, err := s.AppendMarshal([]byte("prefix"), "value")
	assert.NoError(t, err)
	assert.Equal(t, append([]byte("prefix"), data...), out)
	_, err = s.Marshal(make(chan int))
	assert.Error(t, err)

	var val string
	assert.NoError(t, s.Unmarshal(data, &val))
	assert.Error(t, s.Unmarshal([]byte{0xC1}, &val))

	metrics := collect(t, reader)
	serializer := attribute.String("cache.serializer", "msgpack")
	marshal := attribute.String("operation", "marshal")
	unmarshal := attribute.String("operation", "unmarshal")
	duration := metrics["cache.serialization.duration"]
	assert.Equal(t, uint64(2), histogramCount(t, duration, name, serializer, marshal, attribute.Bool("success", true)))
	assert.Equal(t, uint64(1), histogramCount(t, duration, name, serializer, marshal, attribute.Bool("success", false)))
	assert.Equal(t, uint64(1), histogramCount(t, duration, name, serializer, unmarshal, attribute.Bool("success", true)))
	assert.Equal(t, uint64(1), histogramCount(t, duration, name, serializer, unmarshal, attribute.Bool("success", false)))
	assert.Equal(t, defaultDurationBoundaries, histogramBounds(t, duration))

	errs := metrics["cache.serialization.errors"]
	assert.Equal(t, int64(1), counterValue(t, errs, name, serializer, marshal))
	assert.Equal(t, int64(1), counterValue(t, errs, name, serializer, unmarshal))
}

func TestInstrumentedSerializer_AppendMarshalFallback(t *testing.T) {
	mp, reader := newTestMeterProvider(t)
	// JsonSerializer doesn't implement cache.MarshalAppender.
	s := NewInstrumentedSerializer(&cache.JsonSerializer{}, WithMeterProvider(mp))

	out, err := s.AppendMarshal([]byte("prefix:"), "value")
	assert.NoError(t, err)
	assert.Equal(t, `prefix:"value"`, string(out))
	_, err = s.AppendMarshal(nil, make(chan int))
	assert.Error(t, err)

	duration := collect(t, reader)["cache.serialization.duration"]
	assert.Equal(t, uint64(1), histogramCount(t, duration, attribute.String("cache.serializer", "json"),
		attribute.String("operation", "marshal"), attribute.Bool("success", true)))
}
