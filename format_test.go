package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"

	kzstd "github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/jkratz55/redis-cache/v2/cacheproto"
	"github.com/jkratz55/redis-cache/v2/compression/flate"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
	"github.com/jkratz55/redis-cache/v2/compression/zstd"
)

type formatTestPerson struct {
	FirstName string
	LastName  string
	Age       int
}

func newZstdCodec(t testing.TB) *zstd.Codec {
	codec, err := zstd.NewCodec(kzstd.SpeedDefault)
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func builtinCodecs(t testing.TB) map[string]CompressionCodec {
	return map[string]CompressionCodec{
		"none":   nopCodec{},
		"flate":  &flate.Codec{Level: -1},
		"gzip":   gzip.NewCodec(6),
		"lz4":    lz4.NewCodec(),
		"s2":     s2.NewCodec(),
		"snappy": snappy.NewCodec(),
		"zstd":   newZstdCodec(t),
	}
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

// noIDCodec has no format ID and XORs every byte so its output differs from the input.
type noIDCodec struct{}

func (noIDCodec) Compress(data []byte) ([]byte, error) { return xor(data), nil }

func (noIDCodec) Decompress(data []byte) ([]byte, error) { return xor(data), nil }

func xor(data []byte) []byte {
	out := make([]byte, len(data))
	for i, b := range data {
		out[i] = b ^ 0x5A
	}
	return out
}

// unwrapOnlyCodec wraps a codec with an ID but only exposes it via Unwrap.
type unwrapOnlyCodec struct{ inner CompressionCodec }

func (u unwrapOnlyCodec) Compress(data []byte) ([]byte, error) { return u.inner.Compress(data) }

func (u unwrapOnlyCodec) Decompress(data []byte) ([]byte, error) { return u.inner.Decompress(data) }

func (u unwrapOnlyCodec) Unwrap() CompressionCodec { return u.inner }

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

func TestFormats_RoundTrip_AllBuiltins(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	for codecName, codec := range builtinCodecs(t) {
		t.Run("msgpack/"+codecName, func(t *testing.T) {
			f := newFormats(&MessagePackSerializer{}, codec, nil, nil)
			data, err := f.encode(person)
			assert.NoError(t, err)

			var actual formatTestPerson
			assert.NoError(t, f.decode(data, &actual))
			assert.Equal(t, person, actual)
		})
		t.Run("json/"+codecName, func(t *testing.T) {
			f := newFormats(&JsonSerializer{}, codec, nil, nil)
			data, err := f.encode(person)
			assert.NoError(t, err)

			var actual formatTestPerson
			assert.NoError(t, f.decode(data, &actual))
			assert.Equal(t, person, actual)
		})
		t.Run("protobuf/"+codecName, func(t *testing.T) {
			f := newFormats(cacheproto.ProtobufSerializer{}, codec, nil, nil)
			data, err := f.encode(wrapperspb.String("hello world"))
			assert.NoError(t, err)

			actual := &wrapperspb.StringValue{}
			assert.NoError(t, f.decode(data, actual))
			assert.True(t, proto.Equal(wrapperspb.String("hello world"), actual))
		})
	}
}

func TestFormats_HeaderLayout(t *testing.T) {
	f := newFormats(&JsonSerializer{}, gzip.NewCodec(6), nil, nil)
	data, err := f.encode("hello")
	assert.NoError(t, err)

	if !assert.GreaterOrEqual(t, len(data), headerSize) {
		return
	}
	assert.Equal(t, []byte{0xC1, 0x01, SerializerJSON, CodecGzip}, data[:headerSize])

	payload, err := gzip.NewCodec(6).Decompress(data[headerSize:])
	assert.NoError(t, err)
	assert.Equal(t, `"hello"`, string(payload))

	f = newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	data, err = f.encode("hello")
	assert.NoError(t, err)
	expected, err := msgpack.Marshal("hello")
	assert.NoError(t, err)
	assert.Equal(t, append([]byte{0xC1, 0x01, SerializerMsgPack, CodecNone}, expected...), data)
}

func TestFormats_BuiltinIDs(t *testing.T) {
	serializers := map[uint8]Serializer{
		SerializerMsgPack:  &MessagePackSerializer{},
		SerializerJSON:     &JsonSerializer{},
		SerializerProtobuf: cacheproto.ProtobufSerializer{},
	}
	for expected, s := range serializers {
		id, ok := FormatIDOf(s)
		assert.True(t, ok, "%T has no format id", s)
		assert.Equal(t, expected, id, "%T", s)
	}

	codecs := map[uint8]CompressionCodec{
		CodecNone:   nopCodec{},
		CodecFlate:  &flate.Codec{},
		CodecGzip:   gzip.NewCodec(6),
		CodecLZ4:    lz4.NewCodec(),
		CodecS2:     s2.NewCodec(),
		CodecSnappy: snappy.NewCodec(),
		CodecZstd:   newZstdCodec(t),
	}
	for expected, c := range codecs {
		id, ok := FormatIDOf(c)
		assert.True(t, ok, "%T has no format id", c)
		assert.Equal(t, expected, id, "%T", c)
	}
}

func TestFormats_SwitchFormats(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	writer := newFormats(&JsonSerializer{}, gzip.NewCodec(6), nil, nil)
	data, err := writer.encode(person)
	assert.NoError(t, err)

	// JSON is always readable, gzip has to be registered for decoding.
	reader := newFormats(&MessagePackSerializer{}, newZstdCodec(t), nil, []CompressionCodec{gzip.NewCodec(6)})
	var actual formatTestPerson
	assert.NoError(t, reader.decode(data, &actual))
	assert.Equal(t, person, actual)

	// Without gzip registered the value can't be decoded.
	reader = newFormats(&MessagePackSerializer{}, newZstdCodec(t), nil, nil)
	err = reader.decode(data, &actual)
	if assert.ErrorIs(t, err, ErrUnsupportedFormat) {
		assert.Contains(t, err.Error(), "codec id 2")
	}

	// Switching from compression to none and back is always supported.
	writer = newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	data, err = writer.encode(person)
	assert.NoError(t, err)
	reader = newFormats(&JsonSerializer{}, lz4.NewCodec(), nil, nil)
	actual = formatTestPerson{}
	assert.NoError(t, reader.decode(data, &actual))
	assert.Equal(t, person, actual)
}

func TestFormats_UnsupportedSerializer(t *testing.T) {
	writer := newFormats(customSerializer{}, nopCodec{}, nil, nil)
	data, err := writer.encode("hello")
	assert.NoError(t, err)

	reader := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	var actual string
	err = reader.decode(data, &actual)
	if assert.ErrorIs(t, err, ErrUnsupportedFormat) {
		assert.Contains(t, err.Error(), "serializer id 200")
	}

	reader = newFormats(&MessagePackSerializer{}, nopCodec{}, []Serializer{customSerializer{}}, nil)
	assert.NoError(t, reader.decode(data, &actual))
	assert.Equal(t, "hello", actual)
}

func TestFormats_Legacy(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	for codecName, codec := range builtinCodecs(t) {
		t.Run(codecName, func(t *testing.T) {
			raw, err := msgpack.Marshal(person)
			assert.NoError(t, err)
			raw, err = codec.Compress(raw)
			assert.NoError(t, err)

			f := newFormats(&MessagePackSerializer{}, codec, nil, nil)
			var actual formatTestPerson
			assert.NoError(t, f.decode(raw, &actual))
			assert.Equal(t, person, actual)
		})
	}
}

func TestFormats_LegacyShortValue(t *testing.T) {
	raw, err := msgpack.Marshal(5)
	assert.NoError(t, err)
	assert.Less(t, len(raw), headerSize)

	f := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	var actual int
	assert.NoError(t, f.decode(raw, &actual))
	assert.Equal(t, 5, actual)

	// A short value that starts with the magic and version is still legacy.
	f = newFormats(rawSerializer{}, nopCodec{}, nil, nil)
	var b []byte
	assert.NoError(t, f.decode([]byte{0xC1, 0x01, 0x02}, &b))
	assert.Equal(t, []byte{0xC1, 0x01, 0x02}, b)
}

func TestFormats_LegacyCollidingWithHeader(t *testing.T) {
	// Legacy value that starts with the magic and version bytes but names an
	// unknown serializer. Headered decoding fails so the legacy retry kicks in.
	legacy := []byte{0xC1, 0x01, 0xEE, 0xEE, 'h', 'i'}
	f := newFormats(rawSerializer{}, nopCodec{}, nil, nil)
	var b []byte
	assert.NoError(t, f.decode(legacy, &b))
	assert.Equal(t, legacy, b)

	// Legacy value that names known IDs but fails to decode in headered mode.
	legacy = []byte{0xC1, 0x01, SerializerJSON, CodecNone, '{'}
	b = nil
	assert.NoError(t, f.decode(legacy, &b))
	assert.Equal(t, legacy, b)

	// If both fail, the error from the headered decoding is returned.
	f = newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	var s string
	err := f.decode([]byte{0xC1, 0x01, 0xEE, 0x00, 'x'}, &s)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
}

func TestFormats_CustomImplementations(t *testing.T) {
	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	// Implements FormatIdentifier directly.
	f := newFormats(customSerializer{}, nopCodec{}, nil, nil)
	data, err := f.encode(person)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xC1, 0x01, 200, CodecNone}, data[:headerSize])
	var actual formatTestPerson
	assert.NoError(t, f.decode(data, &actual))
	assert.Equal(t, person, actual)

	// Third-party implementations without an ID wrapped with the adapters.
	f = newFormats(SerializerWithID(201, noIDSerializer{}), CodecWithID(210, noIDCodec{}), nil, nil)
	data, err = f.encode(person)
	assert.NoError(t, err)
	assert.Equal(t, []byte{0xC1, 0x01, 201, 210}, data[:headerSize])
	actual = formatTestPerson{}
	assert.NoError(t, f.decode(data, &actual))
	assert.Equal(t, person, actual)

	// Decode-only registration of the old custom formats.
	reader := newFormats(&MessagePackSerializer{}, newZstdCodec(t),
		[]Serializer{SerializerWithID(201, noIDSerializer{})},
		[]CompressionCodec{CodecWithID(210, noIDCodec{})})
	actual = formatTestPerson{}
	assert.NoError(t, reader.decode(data, &actual))
	assert.Equal(t, person, actual)

	// Same implementation registered as configured and decode-only is fine.
	assert.NotPanics(t, func() {
		newFormats(customSerializer{}, gzip.NewCodec(6), []Serializer{customSerializer{}}, []CompressionCodec{gzip.NewCodec(1)})
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
			newFormats(noIDSerializer{}, nopCodec{}, nil, nil)
		})
	})
	t.Run("codec without id", func(t *testing.T) {
		assertPanicContains(t, "CodecWithID", func() {
			newFormats(&MessagePackSerializer{}, noIDCodec{}, nil, nil)
		})
	})
	t.Run("decode-only serializer without id", func(t *testing.T) {
		assertPanicContains(t, "noIDSerializer", func() {
			newFormats(&MessagePackSerializer{}, nopCodec{}, []Serializer{noIDSerializer{}}, nil)
		})
	})
	t.Run("decode-only codec without id", func(t *testing.T) {
		assertPanicContains(t, "noIDCodec", func() {
			newFormats(&MessagePackSerializer{}, nopCodec{}, nil, []CompressionCodec{noIDCodec{}})
		})
	})
	t.Run("duplicate serializer id", func(t *testing.T) {
		assertPanicContains(t, "both claim format id 200", func() {
			newFormats(customSerializer{}, nopCodec{}, []Serializer{otherCustomSerializer{}}, nil)
		})
	})
	t.Run("duplicate codec id", func(t *testing.T) {
		assertPanicContains(t, "both claim format id 210", func() {
			newFormats(&MessagePackSerializer{}, CodecWithID(210, noIDCodec{}), nil,
				[]CompressionCodec{CodecWithID(210, gzip.NewCodec(6))})
		})
	})
	t.Run("adapter id below 128", func(t *testing.T) {
		assertPanicContains(t, "custom ids must be in the range 128-255", func() {
			SerializerWithID(127, noIDSerializer{})
		})
		assertPanicContains(t, "custom ids must be in the range 128-255", func() {
			CodecWithID(CodecGzip, noIDCodec{})
		})
	})
	t.Run("adapter nil", func(t *testing.T) {
		assertPanicContains(t, "nil Serializer", func() {
			SerializerWithID(200, nil)
		})
		assertPanicContains(t, "nil CompressionCodec", func() {
			CodecWithID(200, nil)
		})
	})
	t.Run("unwrap only wrapper has no id", func(t *testing.T) {
		wrapper := unwrapOnlyCodec{inner: gzip.NewCodec(6)}
		_, ok := FormatIDOf(wrapper)
		assert.False(t, ok)
		assertPanicContains(t, "has no format id", func() {
			newFormats(&MessagePackSerializer{}, wrapper, nil, nil)
		})
	})
}

func TestFormats_DecodeErrors(t *testing.T) {
	f := newFormats(&MessagePackSerializer{}, gzip.NewCodec(6), nil, nil)

	var s string
	err := f.decode([]byte("definitely not gzip"), &s)
	if !assert.Error(t, err) {
		return
	}
	assert.Contains(t, err.Error(), "decompress value")
	assert.False(t, errors.Is(err, ErrUnsupportedFormat))

	f = newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	err = f.decode([]byte{0xC1, 0x01, SerializerMsgPack, CodecNone, 0xC1}, &s)
	if !assert.Error(t, err) {
		return
	}
	assert.Contains(t, err.Error(), "unmarshall value")
}

func benchmarkPerson() formatTestPerson {
	return formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}
}

func BenchmarkEncode(b *testing.B) {
	f := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	person := benchmarkPerson()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := f.encode(person); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeLegacy(b *testing.B) {
	s := &MessagePackSerializer{}
	c := nopCodec{}
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
	f := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	data, err := f.encode(benchmarkPerson())
	if err != nil {
		b.Fatal(err)
	}
	var dst formatTestPerson
	b.ReportAllocs()
	for b.Loop() {
		if err := f.decode(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeLegacy(b *testing.B) {
	f := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	data, err := msgpack.Marshal(benchmarkPerson())
	if err != nil {
		b.Fatal(err)
	}
	var dst formatTestPerson
	b.ReportAllocs()
	for b.Loop() {
		if err := f.decode(data, &dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecodeBaseline(b *testing.B) {
	s := &MessagePackSerializer{}
	c := nopCodec{}
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
