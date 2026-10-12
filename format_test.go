package cache_test

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
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

// The tests in this file use the external cache_test package because the
// built-in codec and serializer packages import cache, and because types
// declared in the cache package are always treated as built-in implementations.

type formatTestPerson struct {
	FirstName string
	LastName  string
	Age       int
}

// customSerializer implements FormatIdentifier directly using gob.
type customSerializer struct{}

func (customSerializer) Marshal(v any) ([]byte, error) {
	var buffer bytes.Buffer
	err := gob.NewEncoder(&buffer).Encode(v)
	return buffer.Bytes(), err
}

func (customSerializer) Unmarshal(b []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(v)
}

func (customSerializer) FormatID() uint8 { return 200 }

// otherCustomSerializer claims the same ID as customSerializer.
type otherCustomSerializer struct{ customSerializer }

// noIDSerializer has no format ID.
type noIDSerializer struct{}

func (noIDSerializer) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

func (noIDSerializer) Unmarshal(b []byte, v any) error { return msgpack.Unmarshal(b, v) }

// reservedIDSerializer is a custom implementation illegally claiming a reserved ID.
type reservedIDSerializer struct{ noIDSerializer }

func (reservedIDSerializer) FormatID() uint8 { return 50 }

type customIDSerializer struct{ reservedIDSerializer }

func (customIDSerializer) FormatID() uint8 { return 150 }

// noIDCodec has no format ID and XORs every byte so its output differs from the input.
type noIDCodec struct{}

func (noIDCodec) Compress(data []byte) ([]byte, error) { return xor(data), nil }

func (noIDCodec) Decompress(data []byte) ([]byte, error) { return xor(data), nil }

// reservedIDCodec is a custom implementation illegally claiming a reserved ID.
type reservedIDCodec struct{ noIDCodec }

func (reservedIDCodec) FormatID() uint8 { return 0 }

// xorCodec is a custom codec with a custom format ID.
type xorCodec struct{}

func (xorCodec) Compress(data []byte) ([]byte, error) { return xor(data), nil }

func (xorCodec) Decompress(data []byte) ([]byte, error) { return xor(data), nil }

func (xorCodec) FormatID() uint8 { return 201 }

func xor(data []byte) []byte {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b ^ 0x5A
	}
	return out
}

// unwrapOnlyCodec wraps a codec with an ID but only exposes it via Unwrap.
type unwrapOnlyCodec struct{ inner cache.CompressionCodec }

func (u unwrapOnlyCodec) Compress(data []byte) ([]byte, error) { return u.inner.Compress(data) }

func (u unwrapOnlyCodec) Decompress(data []byte) ([]byte, error) { return u.inner.Decompress(data) }

func (u unwrapOnlyCodec) Unwrap() cache.CompressionCodec { return u.inner }

// rawSerializer copies the bytes into a *[]byte. Used to construct legacy values
// that collide with the header.
type rawSerializer struct{}

func (rawSerializer) Marshal(v any) ([]byte, error) {
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("expected []byte, got %T", v)
	}
	return b, nil
}

func (rawSerializer) Unmarshal(b []byte, v any) error {
	dst, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("expected *[]byte, got %T", v)
	}
	*dst = append((*dst)[:0], b...)
	return nil
}

func (rawSerializer) FormatID() uint8 { return 250 }

// failingCodec fails to decompress every value.
type failingCodec struct{}

func (failingCodec) Compress(data []byte) ([]byte, error) { return data, nil }

func (failingCodec) Decompress([]byte) ([]byte, error) { return nil, errors.New("corrupt input") }

func (failingCodec) FormatID() uint8 { return 220 }

// passThroughSerializer wraps a Serializer without altering the bytes, so it
// forwards the format ID of the Serializer it wraps and implements Unwrap.
type passThroughSerializer struct{ cache.Serializer }

func (p passThroughSerializer) FormatID() uint8 {
	id, _ := cache.FormatIDOf(p.Serializer)
	return id
}

func (p passThroughSerializer) Unwrap() cache.Serializer { return p.Serializer }

// passThroughCodec wraps a CompressionCodec without altering the bytes, so it
// forwards the format ID of the CompressionCodec it wraps and implements Unwrap.
type passThroughCodec struct{ cache.CompressionCodec }

func (p passThroughCodec) FormatID() uint8 {
	id, _ := cache.FormatIDOf(p.CompressionCodec)
	return id
}

func (p passThroughCodec) Unwrap() cache.CompressionCodec { return p.CompressionCodec }

// idForwardingCodec forwards the format ID of the CompressionCodec it wraps but
// doesn't implement Unwrap.
type idForwardingCodec struct{ cache.CompressionCodec }

func (p idForwardingCodec) FormatID() uint8 {
	id, _ := cache.FormatIDOf(p.CompressionCodec)
	return id
}

// strictRawSerializer is a rawSerializer that rejects values starting with the
// format header when unmarshalling strictly.
type strictRawSerializer struct{ rawSerializer }

func (strictRawSerializer) UnmarshalStrict(b []byte, v any) error {
	if len(b) > 0 && b[0] == cache.FormatMagicForTest {
		return errors.New("value starts with the format header")
	}
	return rawSerializer{}.Unmarshal(b, v)
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

func TestFormats_UnsupportedSerializer(t *testing.T) {
	writer := cache.NewFormatsForTest(customSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err := writer.Encode("hello")
	assert.NoError(t, err)

	reader := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	var actual string
	err = reader.Decode(data, &actual)
	if assert.ErrorIs(t, err, cache.ErrUnsupportedFormat) {
		assert.Contains(t, err.Error(), "serializer id 200")
	}

	reader = cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(),
		[]cache.Serializer{customSerializer{}}, nil)
	assert.NoError(t, reader.Decode(data, &actual))
	assert.Equal(t, "hello", actual)
}

func TestFormats_LegacyShortValue(t *testing.T) {
	raw, err := msgpack.Marshal(5)
	assert.NoError(t, err)
	assert.Less(t, len(raw), cache.HeaderSizeForTest)

	f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	var actual int
	assert.NoError(t, f.Decode(raw, &actual))
	assert.Equal(t, 5, actual)

	// A short value that starts with the magic and version is still legacy.
	f = cache.NewFormatsForTest(rawSerializer{}, cache.NopCodecForTest(), nil, nil)
	var b []byte
	assert.NoError(t, f.Decode([]byte{0xC1, 0x01, 0x02}, &b))
	assert.Equal(t, []byte{0xC1, 0x01, 0x02}, b)
}

func TestFormats_LegacyCollidingWithHeader(t *testing.T) {
	// A legacy value can start with the magic and version bytes. If the following
	// bytes name an unknown serializer or codec, or the value fails to decode in
	// headered mode, it is decoded as a legacy value.
	f := cache.NewFormatsForTest(rawSerializer{}, cache.NopCodecForTest(), nil, nil)
	for _, legacy := range [][]byte{
		{0xC1, 0x01, 0xEE, cache.CodecNone, 'h', 'i'},
		{0xC1, 0x01, cache.SerializerMsgPack, 0xEE, 'h', 'i'},
		{0xC1, 0x01, cache.SerializerJSON, cache.CodecNone, '{'},
	} {
		var b []byte
		assert.NoError(t, f.Decode(legacy, &b))
		assert.Equal(t, legacy, b)
	}

	// The legacy attempt uses UnmarshalStrict, also through SerializerWithID, so a
	// value with an unsupported format isn't mistaken for a legacy value.
	f = cache.NewFormatsForTest(cache.SerializerWithID(240, strictRawSerializer{}), cache.NopCodecForTest(), nil, nil)
	var b []byte
	err := f.Decode([]byte{0xC1, 0x01, 0xEE, cache.CodecNone, 'h', 'i'}, &b)
	assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
	assert.Nil(t, b)

	// If both fail, the error from the headered decoding is returned.
	f = cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	var s string
	err = f.Decode([]byte{0xC1, 0x01, cache.SerializerJSON, cache.CodecNone, '{'}, &s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal value")
	assert.Empty(t, s)
	err = f.Decode([]byte{0xC1, 0x01, cache.SerializerMsgPack, 0xEE, 'h', 'i'}, &s)
	assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
}

// TestFormats_LegacyCompressedValueStartingWithHeader verifies values written
// before the header existed are read even if the compressed bytes start with the
// header. Snappy and S2 start a value with its uncompressed length, which is
// encoded as C1 01 for 193 bytes.
func TestFormats_LegacyCompressedValueStartingWithHeader(t *testing.T) {
	value := strings.Repeat("x", 191)
	raw, err := msgpack.Marshal(value)
	assert.NoError(t, err)
	assert.Len(t, raw, 193)

	for name, codec := range map[string]cache.CompressionCodec{
		"snappy": snappy.NewCodec(),
		"s2":     s2.NewCodec(),
	} {
		t.Run(name, func(t *testing.T) {
			legacy, err := codec.Compress(raw)
			assert.NoError(t, err)
			assert.Equal(t, []byte{0xC1, 0x01}, legacy[:2])

			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codec, nil, nil)
			var actual string
			assert.NoError(t, f.Decode(legacy, &actual))
			assert.Equal(t, value, actual)
		})
	}
}

// TestFormats_CorruptValueNotMistakenForLegacy verifies the decode error of a
// value with a header is returned when a tolerant serializer would accept the
// value, header included, as a legacy value. Protobuf reads the header bytes
// C1 01 as field 24.
func TestFormats_CorruptValueNotMistakenForLegacy(t *testing.T) {
	for name, s := range map[string]cache.Serializer{
		"protobuf":         cacheproto.ProtobufSerializer{},
		"SerializerWithID": cache.SerializerWithID(200, cacheproto.ProtobufSerializer{}),
	} {
		t.Run(name, func(t *testing.T) {
			f := cache.NewFormatsForTest(s, cache.NopCodecForTest(), nil, nil)
			data, err := f.Encode(wrapperspb.Bytes([]byte{0xde, 0xad, 0xbe, 0xef}))
			assert.NoError(t, err)

			// The bytes aren't valid UTF-8, so they can't be decoded as a string.
			actual := &wrapperspb.StringValue{}
			err = f.Decode(data, actual)
			assert.ErrorContains(t, err, "unmarshal value")
			assert.Empty(t, actual.GetValue())
		})
	}
}

func TestFormats_FailedDecodeZeroesDestination(t *testing.T) {
	stale := formatTestPerson{FirstName: "Stale", LastName: "Data", Age: 99}

	t.Run("headered and legacy decoding fail", func(t *testing.T) {
		f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
		dst := stale
		err := f.Decode([]byte{0xC1, 0x01, cache.SerializerJSON, cache.CodecNone, '{', '"', 'F'}, &dst)
		assert.Error(t, err)
		assert.Equal(t, formatTestPerson{}, dst)
	})

	t.Run("legacy decoding fails", func(t *testing.T) {
		f := cache.NewFormatsForTest(&cache.JsonSerializer{}, cache.NopCodecForTest(), nil, nil)
		dst := stale
		err := f.Decode([]byte(`{"FirstName":"Bob","Age":"not a number"}`), &dst)
		assert.Error(t, err)
		assert.Equal(t, formatTestPerson{}, dst)
	})

	t.Run("unsupported format", func(t *testing.T) {
		f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
		dst := stale
		err := f.Decode([]byte{0xC1, 0x01, cache.SerializerMsgPack, 0xEE, 0x01}, &dst)
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
		// The value is also decoded as a legacy value, which may have partially
		// populated dst.
		assert.Equal(t, formatTestPerson{}, dst)
	})

	t.Run("successful legacy retry", func(t *testing.T) {
		// The headered decoding fails part way through, the legacy retry must
		// not see any data from the first attempt.
		f := cache.NewFormatsForTest(rawSerializer{}, cache.NopCodecForTest(), nil, nil)
		legacy := []byte{0xC1, 0x01, cache.SerializerJSON, cache.CodecNone, '[', '1', ','}
		var b []byte
		assert.NoError(t, f.Decode(legacy, &b))
		assert.Equal(t, legacy, b)
	})
}

func TestFormats_TypedNilPanics(t *testing.T) {
	assert.Panics(t, func() { cache.Serialization((*cache.JsonSerializer)(nil)) })
	assert.Panics(t, func() { cache.Serialization((*cache.SerializerWithIDForTest)(nil)) })
	assert.Panics(t, func() { cache.Compression((*cache.CodecWithIDForTest)(nil)) })
	assert.Panics(t, func() { cache.DecodeSerializers(&cache.JsonSerializer{}, (*cache.MessagePackSerializer)(nil)) })
	assert.Panics(t, func() { cache.DecodeCompression((*cache.CodecWithIDForTest)(nil)) })
	assert.Panics(t, func() { cache.SerializerWithID(200, (*cache.JsonSerializer)(nil)) })
	assert.Panics(t, func() { cache.CodecWithID(200, (*cache.CodecWithIDForTest)(nil)) })
	assert.Panics(t, func() { cache.NewFormatsForTest((*cache.JsonSerializer)(nil), cache.NopCodecForTest(), nil, nil) })
	assert.Panics(t, func() { cache.NewFormatsForTest(&cache.JsonSerializer{}, (*cache.CodecWithIDForTest)(nil), nil, nil) })
	assert.Panics(t, func() {
		cache.NewFormatsForTest(&cache.JsonSerializer{}, cache.NopCodecForTest(),
			[]cache.Serializer{(*cache.MessagePackSerializer)(nil)}, nil)
	})
}

func TestFormats_TypedNilBuiltinCodecPanics(t *testing.T) {
	assert.Panics(t, func() { cache.Compression((*gzip.Codec)(nil)) })
	assert.Panics(t, func() { cache.DecodeCompression((*zstd.Codec)(nil)) })
	assert.Panics(t, func() { cache.CodecWithID(200, (*flate.Codec)(nil)) })
}

func TestFormats_ValueAndPointerSerializers(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	defer client.Close()

	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	// Value and pointer forms of the built-in serializers are the same
	// implementation and must not be reported as conflicting.
	assert.NotPanics(t, func() {
		cache.New(client, cache.Serialization(cache.JsonSerializer{}),
			cache.DecodeSerializers(&cache.JsonSerializer{}, cache.MessagePackSerializer{}))
	})
	assert.NotPanics(t, func() {
		cache.NewTyped[formatTestPerson](client, cache.Serialization(cache.MessagePackSerializer{}),
			cache.DecodeSerializers(cache.JsonSerializer{}))
	})

	writer := cache.NewFormatsForTest(cache.JsonSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err := writer.Encode(person)
	assert.NoError(t, err)

	reader := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	var actual formatTestPerson
	assert.NoError(t, reader.Decode(data, &actual))
	assert.Equal(t, person, actual)
}

func TestFormats_CustomImplementations(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	// Implements FormatIdentifier directly.
	f := cache.NewFormatsForTest(customSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err := f.Encode(person)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xC1, 0x01, 200, cache.CodecNone}, data[:cache.HeaderSizeForTest])
	var actual formatTestPerson
	assert.NoError(t, f.Decode(data, &actual))
	assert.Equal(t, person, actual)

	// Third-party implementations without an ID wrapped with the adapters.
	f = cache.NewFormatsForTest(cache.SerializerWithID(201, noIDSerializer{}), cache.CodecWithID(210, noIDCodec{}), nil, nil)
	data, err = f.Encode(person)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xC1, 0x01, 201, 210}, data[:cache.HeaderSizeForTest])
	actual = formatTestPerson{}
	assert.NoError(t, f.Decode(data, &actual))
	assert.Equal(t, person, actual)

	// Decode-only registration of the old custom formats.
	reader := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, xorCodec{},
		[]cache.Serializer{cache.SerializerWithID(201, noIDSerializer{})},
		[]cache.CompressionCodec{cache.CodecWithID(210, noIDCodec{})})
	actual = formatTestPerson{}
	assert.NoError(t, reader.Decode(data, &actual))
	assert.Equal(t, person, actual)

	// Same implementation registered as configured and decode-only is fine.
	assert.NotPanics(t, func() {
		cache.NewFormatsForTest(customSerializer{}, cache.CodecWithID(230, noIDCodec{}), []cache.Serializer{customSerializer{}},
			[]cache.CompressionCodec{cache.CodecWithID(230, noIDCodec{})})
	})
}

func TestFormats_CustomReservedID(t *testing.T) {
	assert.PanicsWithError(t,
		"Serializer cache_test.reservedIDSerializer uses reserved format id 50: custom ids must be in the range 128-255, implement FormatID() accordingly or wrap it with cache.SerializerWithID. Wrappers that don't alter the bytes must implement Unwrap() returning the wrapped Serializer",
		func() {
			cache.NewFormatsForTest(reservedIDSerializer{}, cache.CodecWithID(200, noIDCodec{}), nil, nil)
		})

	assert.Panics(t, func() {
		cache.NewFormatsForTest(&cache.MessagePackSerializer{}, reservedIDCodec{}, nil, nil)
	})

	assert.NotPanics(t, func() {
		cache.NewFormatsForTest(customIDSerializer{}, cache.CodecWithID(200, noIDCodec{}), nil, nil)
	})
}

func TestFormats_PassThroughWrappers(t *testing.T) {
	t.Run("wrapping a built-in uses its reserved id", func(t *testing.T) {
		f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, passThroughCodec{gzip.NewCodec(6)}, nil, nil)
		data, err := f.Encode("hello")
		assert.NoError(t, err)
		assertHeader(t, data, cache.SerializerMsgPack, cache.CodecGzip)

		// The wrapper and the codec it wraps are the same format.
		var reader *cache.FormatsForTest
		assert.NotPanics(t, func() {
			reader = cache.NewFormatsForTest(&cache.JsonSerializer{}, gzip.NewCodec(6), nil,
				[]cache.CompressionCodec{passThroughCodec{gzip.NewCodec(6)}})
		})
		var s string
		assert.NoError(t, reader.Decode(data, &s))
		assert.Equal(t, "hello", s)
	})
	t.Run("wrapping a custom implementation with a reserved id panics", func(t *testing.T) {
		assert.Panics(t, func() {
			cache.NewFormatsForTest(passThroughSerializer{reservedIDSerializer{}}, cache.NopCodecForTest(), nil, nil)
		})
		assert.Panics(t, func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, passThroughCodec{reservedIDCodec{}}, nil, nil)
		})
	})
	t.Run("different implementations behind wrappers conflict", func(t *testing.T) {
		assert.PanicsWithError(t,
			"Serializer cache_test.customSerializer and cache_test.otherCustomSerializer both claim format id 200: each format must have a unique id",
			func() {
				cache.NewFormatsForTest(passThroughSerializer{customSerializer{}}, cache.NopCodecForTest(),
					[]cache.Serializer{passThroughSerializer{otherCustomSerializer{}}}, nil)
			})
	})
	t.Run("wrapper without Unwrap can't use a reserved id", func(t *testing.T) {
		defer func() {
			err, _ := recover().(error)
			if assert.Error(t, err) {
				assert.Contains(t, err.Error(), "uses reserved format id 2")
				assert.Contains(t, err.Error(), "must implement Unwrap() returning the wrapped CompressionCodec")
			}
		}()
		cache.NewFormatsForTest(&cache.MessagePackSerializer{}, idForwardingCodec{gzip.NewCodec(6)}, nil, nil)
	})
	t.Run("wrapping nil panics", func(t *testing.T) {
		assert.PanicsWithError(t, "CompressionCodec cache_test.passThroughCodec wraps a nil CompressionCodec", func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, passThroughCodec{(*gzip.Codec)(nil)}, nil, nil)
		})
	})
}

func TestFormats_ConstructionPanics(t *testing.T) {
	assertPanicContains := func(t *testing.T, contains string, fn func()) {
		t.Helper()
		defer func() {
			r := recover()
			if !assert.NotNil(t, r, "expected panic") {
				return
			}
			err, ok := r.(error)
			if !assert.True(t, ok, "expected panic with error, got %T", r) {
				return
			}
			assert.Contains(t, err.Error(), contains)
		}()
		fn()
	}

	t.Run("serializer without id", func(t *testing.T) {
		assertPanicContains(t, "SerializerWithID", func() {
			cache.NewFormatsForTest(noIDSerializer{}, cache.NopCodecForTest(), nil, nil)
		})
	})
	t.Run("codec without id", func(t *testing.T) {
		assertPanicContains(t, "CodecWithID", func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, noIDCodec{}, nil, nil)
		})
	})
	t.Run("decode-only serializer without id", func(t *testing.T) {
		assertPanicContains(t, "noIDSerializer", func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), []cache.Serializer{noIDSerializer{}}, nil)
		})
	})
	t.Run("decode-only codec without id", func(t *testing.T) {
		assertPanicContains(t, "noIDCodec", func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, []cache.CompressionCodec{noIDCodec{}})
		})
	})
	t.Run("duplicate serializer id", func(t *testing.T) {
		assertPanicContains(t, "both claim format id 200", func() {
			cache.NewFormatsForTest(customSerializer{}, cache.NopCodecForTest(), []cache.Serializer{otherCustomSerializer{}}, nil)
		})
	})
	t.Run("duplicate codec id", func(t *testing.T) {
		assertPanicContains(t, "both claim format id 210", func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.CodecWithID(210, noIDCodec{}), nil,
				[]cache.CompressionCodec{cache.CodecWithID(210, xorCodec{})})
		})
	})
	t.Run("adapter id below 128", func(t *testing.T) {
		assertPanicContains(t, "custom ids must be in the range 128-255", func() {
			cache.SerializerWithID(127, noIDSerializer{})
		})
		assertPanicContains(t, "custom ids must be in the range 128-255", func() {
			cache.CodecWithID(cache.CodecGzip, noIDCodec{})
		})
	})
	t.Run("adapter nil", func(t *testing.T) {
		assertPanicContains(t, "nil Serializer", func() {
			cache.SerializerWithID(200, nil)
		})
		assertPanicContains(t, "nil CompressionCodec", func() {
			cache.CodecWithID(200, nil)
		})
	})
	t.Run("unwrap only wrapper has no id", func(t *testing.T) {
		wrapper := unwrapOnlyCodec{inner: xorCodec{}}
		_, ok := cache.FormatIDOf(wrapper)
		assert.False(t, ok)
		assertPanicContains(t, "has no format id", func() {
			cache.NewFormatsForTest(&cache.MessagePackSerializer{}, wrapper, nil, nil)
		})
	})
}

func TestFormats_DecodeErrors(t *testing.T) {
	f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, failingCodec{}, nil, nil)

	var s string
	err := f.Decode([]byte("definitely not compressed"), &s)
	if !assert.Error(t, err) {
		return
	}
	assert.Contains(t, err.Error(), "decompress value")
	assert.False(t, errors.Is(err, cache.ErrUnsupportedFormat))

	f = cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	err = f.Decode([]byte{0xC1, 0x01, cache.SerializerMsgPack, cache.CodecNone, 0xC1}, &s)
	if !assert.Error(t, err) {
		return
	}
	assert.Contains(t, err.Error(), "unmarshal value")
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

func TestFormats_RoundTrip_AllBuiltins(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	for codecName, codec := range builtinCodecs(t) {
		t.Run("msgpack/"+codecName, func(t *testing.T) {
			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codec, nil, nil)
			data, err := f.Encode(person)
			assert.NoError(t, err)

			var actual formatTestPerson
			assert.NoError(t, f.Decode(data, &actual))
			assert.Equal(t, person, actual)
		})
		t.Run("json/"+codecName, func(t *testing.T) {
			f := cache.NewFormatsForTest(&cache.JsonSerializer{}, codec, nil, nil)
			data, err := f.Encode(person)
			assert.NoError(t, err)

			var actual formatTestPerson
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
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	writer := cache.NewFormatsForTest(&cache.JsonSerializer{}, gzip.NewCodec(6), nil, nil)
	data, err := writer.Encode(person)
	assert.NoError(t, err)

	// JSON is always readable, gzip has to be registered for decoding.
	reader := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, newZstdCodec(t), nil,
		[]cache.CompressionCodec{gzip.NewCodec(6)})
	var actual formatTestPerson
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
	actual = formatTestPerson{}
	assert.NoError(t, reader.Decode(data, &actual))
	assert.Equal(t, person, actual)
}

func TestFormats_Legacy(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	for codecName, codec := range builtinCodecs(t) {
		t.Run(codecName, func(t *testing.T) {
			raw, err := msgpack.Marshal(person)
			assert.NoError(t, err)
			raw, err = codec.Compress(raw)
			assert.NoError(t, err)

			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codec, nil, nil)
			var actual formatTestPerson
			assert.NoError(t, f.Decode(raw, &actual))
			assert.Equal(t, person, actual)
		})
	}
}

func TestFormats_UnsupportedCodecNotMistakenForLegacy(t *testing.T) {
	// Values with a valid header naming an unregistered codec aren't mistaken for
	// legacy values, even if the serializer is tolerant.
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

func benchmarkPerson() formatTestPerson {
	return formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}
}

func BenchmarkEncode(b *testing.B) {
	f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	person := benchmarkPerson()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.Encode(person); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeLegacy(b *testing.B) {
	s := &cache.MessagePackSerializer{}
	c := cache.NopCodecForTest()
	person := benchmarkPerson()
	b.ReportAllocs()
	for b.Loop() {
		data, err := s.Marshal(person)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := c.Compress(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeHeadered(b *testing.B) {
	f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err := f.Encode(benchmarkPerson())
	if err != nil {
		b.Fatal(err)
	}
	var dst formatTestPerson
	b.ReportAllocs()
	for b.Loop() {
		if err := f.Decode(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeLegacy(b *testing.B) {
	f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(), nil, nil)
	data, err := msgpack.Marshal(benchmarkPerson())
	if err != nil {
		b.Fatal(err)
	}
	var dst formatTestPerson
	b.ReportAllocs()
	for b.Loop() {
		if err := f.Decode(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBaseline(b *testing.B) {
	s := &cache.MessagePackSerializer{}
	c := cache.NopCodecForTest()
	data, err := msgpack.Marshal(benchmarkPerson())
	if err != nil {
		b.Fatal(err)
	}
	var dst formatTestPerson
	b.ReportAllocs()
	for b.Loop() {
		raw, err := c.Decompress(data)
		if err != nil {
			b.Fatal(err)
		}
		if err := s.Unmarshal(raw, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

// TestFormats_EncodeMatchesCompress asserts encoding values directly after the
// header, with MarshalAppender and CompressAppender, writes the same bytes as
// prefixing the header to the output of Marshal and Compress.
func TestFormats_EncodeMatchesCompress(t *testing.T) {
	person := benchmarkDocument()
	serializers := map[string]cache.Serializer{
		"msgpack": &cache.MessagePackSerializer{},
		"json":    &cache.JsonSerializer{},
		"custom":  customSerializer{},
	}
	for sName, s := range serializers {
		for cName, c := range builtinCodecs(t) {
			t.Run(sName+"/"+cName, func(t *testing.T) {
				sID, _ := cache.FormatIDOf(s)
				cID, _ := cache.FormatIDOf(c)
				data, err := s.Marshal(person)
				assert.NoError(t, err)
				compressed, err := c.Compress(data)
				assert.NoError(t, err)

				f := cache.NewFormatsForTest(s, c, nil, nil)
				encoded, err := f.Encode(person)
				assert.NoError(t, err)
				assert.Equal(t, compressed, assertHeader(t, encoded, sID, cID))

				var decoded formatTestDocument
				assert.NoError(t, f.Decode(encoded, &decoded))
				assert.Equal(t, person, decoded)
			})
		}
	}
}

// limitedCodecs returns the built-in codecs with a limit of 1 MiB on the size of
// decompressed values.
func limitedCodecs(t testing.TB) []cache.CompressionCodec {
	const limit = 1 << 20
	zstdCodec, err := zstd.NewCodec(kzstd.SpeedDefault, zstd.WithMaxDecodedSize(limit))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = zstdCodec.Close() })
	return []cache.CompressionCodec{
		flate.NewCodec(-1, flate.WithMaxDecodedSize(limit)),
		gzip.NewCodec(6, gzip.WithMaxDecodedSize(limit)),
		lz4.NewCodec(lz4.WithMaxDecodedSize(limit)),
		s2.NewCodec(s2.WithMaxDecodedSize(limit)),
		snappy.NewCodec(snappy.WithMaxDecodedSize(limit)),
		zstdCodec,
	}
}

// FuzzDecode decodes arbitrary values, as a value read from a Redis shared with
// other services can be corrupt or crafted. Decoding must never panic, and must
// reset the destination if it fails. The codecs limit the size of decompressed
// values, so the fuzzer doesn't run out of memory.
//
// The seed corpus holds a value for every serializer and codec pair, legacy
// values, and values with a corrupt or unsupported header.
func FuzzDecode(f *testing.F) {
	codecs := limitedCodecs(f)
	serializers := []cache.Serializer{&cache.MessagePackSerializer{}, &cache.JsonSerializer{}}

	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}
	for _, s := range serializers {
		for _, c := range append(codecs, cache.NopCodecForTest()) {
			data, err := cache.NewFormatsForTest(s, c, nil, nil).Encode(person)
			if err != nil {
				f.Fatal(err)
			}
			f.Add(data)
		}
	}
	legacy, err := msgpack.Marshal(person)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(legacy)
	f.Add([]byte{})
	f.Add([]byte{cache.FormatMagicForTest, cache.FormatVersionForTest, cache.SerializerMsgPack, cache.CodecGzip})
	f.Add([]byte{cache.FormatMagicForTest, cache.FormatVersionForTest, 250, cache.CodecNone, 0x01})
	f.Add([]byte{cache.FormatMagicForTest, cache.FormatVersionForTest, cache.SerializerMsgPack, cache.CodecS2,
		0xff, 0xff, 0xff, 0xff, 0x0f, 0x00})

	reader := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, cache.NopCodecForTest(),
		[]cache.Serializer{cacheproto.ProtobufSerializer{}}, codecs)

	f.Fuzz(func(t *testing.T, data []byte) {
		var p formatTestPerson
		if err := reader.Decode(data, &p); err != nil {
			assert.Zero(t, p)
		}
		var str string
		if err := reader.Decode(data, &str); err != nil {
			assert.Zero(t, str)
		}
		msg := &wrapperspb.StringValue{}
		if err := reader.Decode(data, msg); err != nil {
			assert.True(t, proto.Equal(&wrapperspb.StringValue{}, msg))
		}
	})
}

// formatTestDocument is a value of about 1 KiB used to benchmark the codecs.
type formatTestDocument struct {
	ID       int
	Title    string
	Body     string
	Tags     []string
	Revision int
}

func benchmarkDocument() formatTestDocument {
	return formatTestDocument{
		ID:       42,
		Title:    "Caching structured values in Redis",
		Body:     strings.Repeat("Redis Cache stores Go values in Redis as bytes. ", 20),
		Tags:     []string{"redis", "cache", "go", "serialization", "compression"},
		Revision: 7,
	}
}

func BenchmarkEncodeCodecs(b *testing.B) {
	codecs := builtinCodecs(b)
	doc := benchmarkDocument()
	for _, name := range slices.Sorted(maps.Keys(codecs)) {
		b.Run(name, func(b *testing.B) {
			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codecs[name], nil, nil)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := f.Encode(doc); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkDecodeCodecs(b *testing.B) {
	codecs := builtinCodecs(b)
	doc := benchmarkDocument()
	for _, name := range slices.Sorted(maps.Keys(codecs)) {
		b.Run(name, func(b *testing.B) {
			f := cache.NewFormatsForTest(&cache.MessagePackSerializer{}, codecs[name], nil, nil)
			data, err := f.Encode(doc)
			if err != nil {
				b.Fatal(err)
			}
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			var dst formatTestDocument
			for b.Loop() {
				if err := f.Decode(data, &dst); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
