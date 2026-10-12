package cache_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	kzstd "github.com/klauspost/compress/zstd"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/cacheproto"
	"github.com/jkratz55/redis-cache/v2/compression/flate"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
	"github.com/jkratz55/redis-cache/v2/compression/zstd"
)

type extPerson struct {
	FirstName string
	LastName  string
	Age       int
}

// newTestClient starts a miniredis server for the test and returns a client
// connected to it.
func newTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return server, client
}

func newZstdCodec(t testing.TB) *zstd.Codec {
	t.Helper()
	codec, err := zstd.NewCodec(kzstd.SpeedDefault)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = codec.Close() })
	return codec
}

func builtinCodecs(t testing.TB) map[string]cache.CompressionCodec {
	return map[string]cache.CompressionCodec{
		"none":       cache.NopCodecForTest(),
		"flate":      &flate.Codec{Level: -1},
		"flate-zero": &flate.Codec{},
		"gzip":       gzip.NewCodec(6),
		"lz4":        lz4.NewCodec(),
		"s2":         s2.NewCodec(),
		"snappy":     snappy.NewCodec(),
		"zstd":       newZstdCodec(t),
	}
}

// assertHeader asserts the value starts with the format header for the given
// serializer and codec IDs and returns the payload following the header.
func assertHeader(t *testing.T, b []byte, serializerID, codecID uint8) []byte {
	t.Helper()
	if !assert.GreaterOrEqual(t, len(b), cache.HeaderSizeForTest) {
		return nil
	}
	assert.Equal(t, []byte{cache.FormatMagicForTest, cache.FormatVersionForTest, serializerID, codecID},
		b[:cache.HeaderSizeForTest])
	return b[cache.HeaderSizeForTest:]
}

// xorCodec is a custom codec with a custom format ID.
type xorCodec struct{}

func (xorCodec) Compress(data []byte) ([]byte, error) { return xorBytes(data), nil }

func (xorCodec) Decompress(data []byte) ([]byte, error) { return xorBytes(data), nil }

func (xorCodec) FormatID() uint8 { return 201 }

func xorBytes(data []byte) []byte {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b ^ 0x5A
	}
	return out
}

func TestFormats_RoundTrip_AllBuiltins(t *testing.T) {
	person := extPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	for codecName, codec := range builtinCodecs(t) {
		t.Run("msgpack/"+codecName, func(t *testing.T) {
			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codec, nil, nil)
			data, err := f.Encode(person)
			assert.NoError(t, err)

			var actual extPerson
			assert.NoError(t, f.Decode(data, &actual))
			assert.Equal(t, person, actual)
		})
		t.Run("json/"+codecName, func(t *testing.T) {
			f := cache.NewFormatsForTest(&cache.JsonSerializer{}, codec, nil, nil)
			data, err := f.Encode(person)
			assert.NoError(t, err)

			var actual extPerson
			assert.NoError(t, f.Decode(data, &actual))
			assert.Equal(t, person, actual)
		})
		t.Run("protobuf/"+codecName, func(t *testing.T) {
			f := cache.NewFormatsForTest(cacheproto.ProtobufSerializer{}, codec, nil, nil)
			data, err := f.Encode(wrapperspb.String("hello world"))
			assert.NoError(t, err)

			actual := &wrapperspb.StringValue{}
			assert.NoError(t, f.Decode(data, actual))
			assert.True(t, proto.Equal(wrapperspb.String("hello world"), actual))
		})
	}
}

func TestFormats_HeaderLayout(t *testing.T) {
	f := cache.NewFormatsForTest(&cache.JsonSerializer{}, gzip.NewCodec(6), nil, nil)
	data, err := f.Encode("hello")
	assert.NoError(t, err)

	payload := assertHeader(t, data, cache.SerializerJSON, cache.CodecGzip)
	payload, err = gzip.NewCodec(6).Decompress(payload)
	assert.NoError(t, err)
	assert.Equal(t, `"hello"`, string(payload))

	f = cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err = f.Encode("hello")
	assert.NoError(t, err)
	expected, err := msgpack.Marshal("hello")
	assert.NoError(t, err)
	assert.Equal(t, expected, assertHeader(t, data, cache.SerializerMsgPack, cache.CodecNone))
}

// TestFormats_BuiltinIDs verifies every built-in implementation reports the
// format ID constant declared by the cache package.
func TestFormats_BuiltinIDs(t *testing.T) {
	serializers := map[uint8]cache.Serializer{
		cache.SerializerMsgPack:  &cache.MessagePackSerializer{},
		cache.SerializerJSON:     &cache.JsonSerializer{},
		cache.SerializerProtobuf: cacheproto.ProtobufSerializer{},
	}
	for expected, s := range serializers {
		id, ok := cache.FormatIDOf(s)
		assert.True(t, ok, "%T has no format id", s)
		assert.Equal(t, expected, id, "%T", s)
	}

	codecs := map[uint8]cache.CompressionCodec{
		cache.CodecNone:   cache.NopCodecForTest(),
		cache.CodecFlate:  &flate.Codec{},
		cache.CodecGzip:   gzip.NewCodec(6),
		cache.CodecLZ4:    lz4.NewCodec(),
		cache.CodecS2:     s2.NewCodec(),
		cache.CodecSnappy: snappy.NewCodec(),
		cache.CodecZstd:   newZstdCodec(t),
	}
	for expected, c := range codecs {
		id, ok := cache.FormatIDOf(c)
		assert.True(t, ok, "%T has no format id", c)
		assert.Equal(t, expected, id, "%T", c)
	}
}

func TestFormats_SwitchFormats(t *testing.T) {
	person := extPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	writer := cache.NewFormatsForTest(&cache.JsonSerializer{}, gzip.NewCodec(6), nil, nil)
	data, err := writer.Encode(person)
	assert.NoError(t, err)

	// JSON is always readable, gzip has to be registered for decoding.
	reader := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, newZstdCodec(t), nil,
		[]cache.CompressionCodec{gzip.NewCodec(6)})
	var actual extPerson
	assert.NoError(t, reader.Decode(data, &actual))
	assert.Equal(t, person, actual)

	// Without gzip registered the value can't be decoded.
	reader = cache.NewFormatsForTest(&cache.MessagePackSerializer{}, newZstdCodec(t), nil, nil)
	err = reader.Decode(data, &actual)
	if assert.ErrorIs(t, err, cache.ErrUnsupportedFormat) {
		assert.Contains(t, err.Error(), "codec id 2")
	}

	// Switching from compression to none and back is always supported.
	writer = cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err = writer.Encode(person)
	assert.NoError(t, err)
	reader = cache.NewFormatsForTest(&cache.JsonSerializer{}, lz4.NewCodec(), nil, nil)
	actual = extPerson{}
	assert.NoError(t, reader.Decode(data, &actual))
	assert.Equal(t, person, actual)
}

func TestFormats_Legacy(t *testing.T) {
	person := extPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	for codecName, codec := range builtinCodecs(t) {
		t.Run(codecName, func(t *testing.T) {
			raw, err := msgpack.Marshal(person)
			assert.NoError(t, err)
			raw, err = codec.Compress(raw)
			assert.NoError(t, err)

			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codec, nil, nil)
			var actual extPerson
			assert.NoError(t, f.Decode(raw, &actual))
			assert.Equal(t, person, actual)
		})
	}
}

func TestFormats_UnsupportedCodecNoLegacyFallback(t *testing.T) {
	// Values with a valid header naming an unregistered codec never fall back to
	// legacy decoding, regardless of how tolerant the serializer is.
	for name, s := range map[string]cache.Serializer{
		"msgpack":  &cache.MessagePackSerializer{},
		"protobuf": cacheproto.ProtobufSerializer{},
	} {
		t.Run(name, func(t *testing.T) {
			var value any = "hello"
			if name == "protobuf" {
				value = wrapperspb.String("hello")
			}
			writer := cache.NewFormatsForTest(s, gzip.NewCodec(6), nil, nil)
			data, err := writer.Encode(value)
			assert.NoError(t, err)

			reader := cache.NewFormatsForTest(s, cache.NopCodecForTest(), nil, nil)
			if name == "protobuf" {
				err = reader.Decode(data, &wrapperspb.StringValue{})
			} else {
				var str string
				err = reader.Decode(data, &str)
			}
			assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
		})
	}
}

func TestFormats_DecodeErrorsBuiltin(t *testing.T) {
	f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, gzip.NewCodec(6), nil, nil)
	var s string
	err := f.Decode([]byte("definitely not gzip"), &s)
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "decompress value")
		assert.NotErrorIs(t, err, cache.ErrUnsupportedFormat)
	}
}

func TestFormats_TypedNilBuiltinCodecPanics(t *testing.T) {
	assert.Panics(t, func() { cache.Compression((*gzip.Codec)(nil)) })
	assert.Panics(t, func() { cache.DecodeCompression((*zstd.Codec)(nil)) })
	assert.Panics(t, func() { cache.CodecWithID(200, (*flate.Codec)(nil)) })
}
