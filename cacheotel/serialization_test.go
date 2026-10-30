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
	"google.golang.org/protobuf/types/known/wrapperspb"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/cacheproto"
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

func TestInstrumentSerializers_Wrapping(t *testing.T) {
	msgpackSerializer := &cache.MessagePackSerializer{}
	jsonSerializer := &cache.JsonSerializer{}
	list := []cache.Serializer{msgpackSerializer, jsonSerializer, cacheproto.ProtobufSerializer{}}
	input := append([]cache.Serializer(nil), list...)

	wrapped := InstrumentSerializers(list)
	if !assert.Len(t, wrapped, 3) {
		return
	}
	for i, serializer := range wrapped {
		instrumented, ok := serializer.(*InstrumentedSerializer)
		if !assert.True(t, ok, "serializer %d is %T", i, serializer) {
			continue
		}
		assert.Equal(t, list[i], instrumented.Unwrap())
		wantID, _ := cache.FormatIDOf(list[i])
		assert.Equal(t, wantID, instrumented.FormatID())
	}
	assert.Same(t, msgpackSerializer, wrapped[0].(*InstrumentedSerializer).Unwrap())
	assert.Same(t, jsonSerializer, wrapped[1].(*InstrumentedSerializer).Unwrap())
	assert.Equal(t, cache.SerializerProtobuf, wrapped[2].(*InstrumentedSerializer).FormatID())

	// The input isn't modified.
	assert.Equal(t, input, list)
	assert.Same(t, msgpackSerializer, list[0])
	assert.Same(t, jsonSerializer, list[1])
}

func TestInstrumentSerializers_Empty(t *testing.T) {
	for _, serializers := range [][]cache.Serializer{nil, {}} {
		wrapped := InstrumentSerializers(serializers)
		assert.NotNil(t, wrapped)
		assert.Empty(t, wrapped)
	}
}

func TestInstrumentSerializers_Panics(t *testing.T) {
	assert.PanicsWithError(t, "nil Serializer", func() {
		InstrumentSerializers([]cache.Serializer{&cache.JsonSerializer{}, nil})
	})
	assert.PanicsWithError(t, "nil Serializer", func() {
		InstrumentSerializers([]cache.Serializer{(*cache.JsonSerializer)(nil)})
	})
	assert.PanicsWithError(t,
		"serializer cacheotel.noIDSerializer has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.SerializerWithID",
		func() {
			InstrumentSerializers([]cache.Serializer{noIDSerializer{}})
		})
}

// TestInstrumentSerializers_DecodeSerializers reads values written with MessagePack
// and Protobuf using a JSON cache that registers the instrumented serializers, and
// verifies their unmarshal operations are recorded. The MessagePack read shows the
// instrumented serializer replaces the built-in default.
func TestInstrumentSerializers_DecodeSerializers(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	msgpackWriter := cache.New(client, cache.Serialization(&cache.MessagePackSerializer{}))
	assert.NoError(t, msgpackWriter.Set(ctx, "msgpack", "value", 0))
	protoWriter := cache.New(client, cache.Serialization(cacheproto.ProtobufSerializer{}))
	assert.NoError(t, protoWriter.Set(ctx, "protobuf", wrapperspb.String("value"), 0))

	mp, reader := newTestMeterProvider(t)
	name := attribute.String("cache.name", "users")
	c := cache.New(client,
		cache.Serialization(NewInstrumentedSerializer(&cache.JsonSerializer{})),
		cache.DecodeSerializers(InstrumentSerializers([]cache.Serializer{
			&cache.MessagePackSerializer{},
			cacheproto.ProtobufSerializer{},
		}, WithMeterProvider(mp), WithAttributes(name))...))

	var s string
	assert.NoError(t, c.Get(ctx, "msgpack", &s))
	assert.Equal(t, "value", s)
	msg := &wrapperspb.StringValue{}
	assert.NoError(t, c.Get(ctx, "protobuf", msg))
	assert.Equal(t, "value", msg.GetValue())

	duration := collect(t, reader)["cache.serialization.duration"]
	for _, serializer := range []string{"msgpack", "protobuf"} {
		assert.Equal(t, uint64(1), histogramCount(t, duration, name, attribute.String("cache.serializer", serializer),
			attribute.String("operation", "unmarshal"), attribute.Bool("success", true)), serializer)
	}
}

// TestInstrumentSerializers_ConfiguredSerializer verifies the instrumented serializers
// can be registered next to a configured instrumented serializer of the same type.
func TestInstrumentSerializers_ConfiguredSerializer(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	opts := []cache.Option{
		cache.Serialization(NewInstrumentedSerializer(&cache.JsonSerializer{})),
		cache.DecodeSerializers(InstrumentSerializers([]cache.Serializer{
			&cache.MessagePackSerializer{},
			&cache.JsonSerializer{},
			cacheproto.ProtobufSerializer{},
		})...),
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
			assert.Equal(t, cache.SerializerJSON, raw[2])
		}
		s, err := typed.Get(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, "value", s)
	}
}
