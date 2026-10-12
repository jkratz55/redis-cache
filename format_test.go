package cache

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

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

// failingCodec fails to decompress every value.
type failingCodec struct{}

func (failingCodec) Compress(data []byte) ([]byte, error) { return data, nil }

func (failingCodec) Decompress([]byte) ([]byte, error) { return nil, errors.New("corrupt input") }

func (failingCodec) FormatID() uint8 { return 220 }

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
	// A value that starts with the magic and version bytes but names an unknown
	// serializer or codec has a valid header, so it is never retried as legacy.
	f := newFormats(rawSerializer{}, nopCodec{}, nil, nil)
	var b []byte
	err := f.decode([]byte{0xC1, 0x01, 0xEE, CodecNone, 'h', 'i'}, &b)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Nil(t, b)
	err = f.decode([]byte{0xC1, 0x01, SerializerMsgPack, 0xEE, 'h', 'i'}, &b)
	assert.ErrorIs(t, err, ErrUnsupportedFormat)
	assert.Nil(t, b)

	// Legacy value that names known IDs but fails to decode in headered mode.
	legacy := []byte{0xC1, 0x01, SerializerJSON, CodecNone, '{'}
	b = nil
	assert.NoError(t, f.decode(legacy, &b))
	assert.Equal(t, legacy, b)

	// If both fail, the error from the headered decoding is returned.
	f = newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	var s string
	err = f.decode([]byte{0xC1, 0x01, SerializerJSON, CodecNone, '{'}, &s)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal value")
	assert.Empty(t, s)
}

func TestFormats_FailedDecodeZeroesDestination(t *testing.T) {
	stale := formatTestPerson{FirstName: "Stale", LastName: "Data", Age: 99}

	t.Run("headered and legacy decoding fail", func(t *testing.T) {
		f := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
		dst := stale
		err := f.decode([]byte{0xC1, 0x01, SerializerJSON, CodecNone, '{', '"', 'F'}, &dst)
		assert.Error(t, err)
		assert.Equal(t, formatTestPerson{}, dst)
	})

	t.Run("legacy decoding fails", func(t *testing.T) {
		f := newFormats(&JsonSerializer{}, nopCodec{}, nil, nil)
		dst := stale
		err := f.decode([]byte(`{"FirstName":"Bob","Age":"not a number"}`), &dst)
		assert.Error(t, err)
		assert.Equal(t, formatTestPerson{}, dst)
	})

	t.Run("unsupported format", func(t *testing.T) {
		f := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
		dst := stale
		err := f.decode([]byte{0xC1, 0x01, SerializerMsgPack, 0xEE, 0x01}, &dst)
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		// Nothing was decoded, so dst is left untouched.
		assert.Equal(t, stale, dst)
	})

	t.Run("successful legacy retry", func(t *testing.T) {
		// The headered decoding fails part way through, the legacy retry must
		// not see any data from the first attempt.
		f := newFormats(rawSerializer{}, nopCodec{}, nil, nil)
		legacy := []byte{0xC1, 0x01, SerializerJSON, CodecNone, '[', '1', ','}
		var b []byte
		assert.NoError(t, f.decode(legacy, &b))
		assert.Equal(t, legacy, b)
	})
}

func TestFormats_TypedNilPanics(t *testing.T) {
	assert.Panics(t, func() { Serialization((*JsonSerializer)(nil)) })
	assert.Panics(t, func() { Serialization((*serializerWithID)(nil)) })
	assert.Panics(t, func() { Compression((*codecWithID)(nil)) })
	assert.Panics(t, func() { DecodeSerializers(&JsonSerializer{}, (*MessagePackSerializer)(nil)) })
	assert.Panics(t, func() { DecodeCompression((*codecWithID)(nil)) })
	assert.Panics(t, func() { SerializerWithID(200, (*JsonSerializer)(nil)) })
	assert.Panics(t, func() { CodecWithID(200, (*codecWithID)(nil)) })
	assert.Panics(t, func() { newFormats((*JsonSerializer)(nil), nopCodec{}, nil, nil) })
	assert.Panics(t, func() { newFormats(&JsonSerializer{}, (*codecWithID)(nil), nil, nil) })
	assert.Panics(t, func() {
		newFormats(&JsonSerializer{}, nopCodec{}, []Serializer{(*MessagePackSerializer)(nil)}, nil)
	})
}

func TestFormats_ValueAndPointerSerializers(t *testing.T) {
	setup()
	defer tearDown()

	person := formatTestPerson{FirstName: "Billy", LastName: "Bob", Age: 45}

	// Value and pointer forms of the built-in serializers are the same
	// implementation and must not be reported as conflicting.
	assert.NotPanics(t, func() {
		New(client, Serialization(JsonSerializer{}), DecodeSerializers(&JsonSerializer{}, MessagePackSerializer{}))
	})
	assert.NotPanics(t, func() {
		NewTyped[formatTestPerson](client, Serialization(MessagePackSerializer{}), DecodeSerializers(JsonSerializer{}))
	})

	writer := newFormats(JsonSerializer{}, nopCodec{}, nil, nil)
	data, err := writer.encode(person)
	assert.NoError(t, err)

	reader := newFormats(&MessagePackSerializer{}, nopCodec{}, nil, nil)
	var actual formatTestPerson
	assert.NoError(t, reader.decode(data, &actual))
	assert.Equal(t, person, actual)
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
	reader := newFormats(&MessagePackSerializer{}, xorTestCodec{},
		[]Serializer{SerializerWithID(201, noIDSerializer{})},
		[]CompressionCodec{CodecWithID(210, noIDCodec{})})
	actual = formatTestPerson{}
	assert.NoError(t, reader.decode(data, &actual))
	assert.Equal(t, person, actual)

	// Same implementation registered as configured and decode-only is fine.
	assert.NotPanics(t, func() {
		newFormats(customSerializer{}, CodecWithID(230, noIDCodec{}), []Serializer{customSerializer{}},
			[]CompressionCodec{CodecWithID(230, noIDCodec{})})
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
				[]CompressionCodec{CodecWithID(210, xorTestCodec{})})
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
		wrapper := unwrapOnlyCodec{inner: xorTestCodec{}}
		_, ok := FormatIDOf(wrapper)
		assert.False(t, ok)
		assertPanicContains(t, "has no format id", func() {
			newFormats(&MessagePackSerializer{}, wrapper, nil, nil)
		})
	})
}

func TestFormats_DecodeErrors(t *testing.T) {
	f := newFormats(&MessagePackSerializer{}, failingCodec{}, nil, nil)

	var s string
	err := f.decode([]byte("definitely not compressed"), &s)
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
	assert.Contains(t, err.Error(), "unmarshal value")
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
