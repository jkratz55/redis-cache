package cache

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Every value written by Cache and TypedCache is prefixed with a fixed 4-byte
// header describing how the payload was encoded:
//
//	[0xC1][version][serializer ID][codec ID][payload...]
//
// 0xC1 is never a valid first byte for msgpack or UTF-8/JSON and doesn't collide
// with the gzip, zstd, or lz4 frame signatures.
const (
	formatMagic   byte = 0xC1
	formatVersion byte = 0x01
	headerSize         = 4
)

// Reserved format IDs for the built-in serializers and compression codecs.
//
// IDs 0-127 are reserved for built-in implementations (current and future). IDs
// 128-255 are available for custom implementations. Once values have been
// written with a given ID, that ID must never be reused for a different format
// by any service sharing the same Redis data.
const (
	SerializerMsgPack  uint8 = 1
	SerializerJSON     uint8 = 2
	SerializerProtobuf uint8 = 3

	CodecNone   uint8 = 0
	CodecFlate  uint8 = 1
	CodecGzip   uint8 = 2
	CodecLZ4    uint8 = 3
	CodecS2     uint8 = 4
	CodecSnappy uint8 = 5
	CodecZstd   uint8 = 6
)

// MinCustomFormatID is the lowest format ID available to custom Serializer and
// CompressionCodec implementations.
const MinCustomFormatID uint8 = 128

// ErrUnsupportedFormat is returned when a cached value was written with a
// Serializer or CompressionCodec that isn't registered on the Cache instance
// reading it.
var ErrUnsupportedFormat = errors.New("unsupported format")

// modulePath is used to determine if a type is a built-in implementation, which
// are the only implementations permitted to use the reserved IDs 0-127.
const modulePath = "github.com/jkratz55/redis-cache/v2"

// FormatIdentifier is implemented by Serializer and CompressionCodec types to
// declare the format ID stored in the header of every cached value.
//
// Every Serializer and CompressionCodec used with Cache or TypedCache must have
// a format ID. Custom implementations must return an ID in the range 128-255,
// or be wrapped with SerializerWithID / CodecWithID.
//
// Wrappers that don't alter the encoded bytes (such as instrumentation) should
// forward the ID of the component they wrap. Wrappers that do alter the bytes
// (such as encryption) must declare their own ID.
type FormatIdentifier interface {
	FormatID() uint8
}

// FormatIDOf returns the format ID of v if it implements FormatIdentifier.
//
// Only FormatID is consulted, Unwrap is never followed. This ensures a wrapper
// that transforms the bytes can never inherit the ID of the component it wraps
// by accident.
func FormatIDOf(v any) (uint8, bool) {
	if v == nil {
		return 0, false
	}
	fi, ok := v.(FormatIdentifier)
	if !ok {
		return 0, false
	}
	return fi.FormatID(), true
}

// SerializerWithID wraps a Serializer and assigns it the provided format ID. This
// is useful for Serializer implementations you don't own or can't modify.
//
// The ID must be in the range 128-255, otherwise SerializerWithID panics. It also
// panics if s is nil.
func SerializerWithID(id uint8, s Serializer) Serializer {
	if isNil(s) {
		panic(errors.New("nil Serializer"))
	}
	if id < MinCustomFormatID {
		panic(fmt.Errorf("invalid format id %d for Serializer %T: custom ids must be in the range %d-255", id, s, MinCustomFormatID))
	}
	return &serializerWithID{id: id, Serializer: s}
}

// CodecWithID wraps a CompressionCodec and assigns it the provided format ID.
// This is useful for CompressionCodec implementations you don't own or can't
// modify.
//
// The ID must be in the range 128-255, otherwise CodecWithID panics. It also
// panics if c is nil.
func CodecWithID(id uint8, c CompressionCodec) CompressionCodec {
	if isNil(c) {
		panic(errors.New("nil CompressionCodec"))
	}
	if id < MinCustomFormatID {
		panic(fmt.Errorf("invalid format id %d for CompressionCodec %T: custom ids must be in the range %d-255", id, c, MinCustomFormatID))
	}
	return &codecWithID{id: id, CompressionCodec: c}
}

type serializerWithID struct {
	id uint8
	Serializer
}

func (s *serializerWithID) FormatID() uint8 {
	return s.id
}

type codecWithID struct {
	id uint8
	CompressionCodec
}

func (c *codecWithID) FormatID() uint8 {
	return c.id
}

// formats holds the configured Serializer and CompressionCodec used for writes and
// legacy (headerless) reads, as well as all the known serializers and codecs
// indexed by their format ID.
//
// formats is immutable after construction and safe for concurrent use.
type formats struct {
	serializer   Serializer
	codec        CompressionCodec
	serializerID uint8
	codecID      uint8
	serializers  [256]Serializer
	codecs       [256]CompressionCodec
}

// newFormats builds the format registry. It panics if any Serializer or
// CompressionCodec doesn't have a valid format ID or if two different
// implementations claim the same ID.
//
// The configured serializer and codec, the built-in msgpack and JSON serializers,
// and the no-op codec are always available for reading.
func newFormats(s Serializer, c CompressionCodec, extraS []Serializer, extraC []CompressionCodec) *formats {
	if isNil(s) {
		panic(errors.New("nil Serializer"))
	}
	if isNil(c) {
		panic(errors.New("nil CompressionCodec"))
	}

	f := &formats{
		serializer: s,
		codec:      c,
	}
	f.serializerID = registerFormat(&f.serializers, s, "Serializer", "SerializerWithID")
	f.codecID = registerFormat(&f.codecs, c, "CompressionCodec", "CodecWithID")
	for _, es := range extraS {
		registerFormat(&f.serializers, es, "Serializer", "SerializerWithID")
	}
	for _, ec := range extraC {
		registerFormat(&f.codecs, ec, "CompressionCodec", "CodecWithID")
	}

	// Built-in defaults are always readable, but never override an explicitly
	// registered implementation using the same ID, such as an instrumented wrapper.
	if f.serializers[SerializerMsgPack] == nil {
		f.serializers[SerializerMsgPack] = &MessagePackSerializer{}
	}
	if f.serializers[SerializerJSON] == nil {
		f.serializers[SerializerJSON] = &JsonSerializer{}
	}
	if f.codecs[CodecNone] == nil {
		f.codecs[CodecNone] = nopCodec{}
	}

	return f
}

// registerFormat validates the format ID of v and stores it in the registry.
func registerFormat[T any](registry *[256]T, v T, kind string, adapter string) uint8 {
	if isNil(v) {
		panic(fmt.Errorf("nil %s", kind))
	}
	id, ok := FormatIDOf(v)
	if !ok {
		panic(fmt.Errorf("%s %T has no format id: implement FormatID() uint8 returning an id in the range %d-255 or wrap it with cache.%s",
			kind, v, MinCustomFormatID, adapter))
	}
	if id < MinCustomFormatID && !isBuiltin(v) {
		panic(fmt.Errorf("%s %T uses reserved format id %d: custom ids must be in the range %d-255, implement FormatID() accordingly or wrap it with cache.%s",
			kind, v, id, MinCustomFormatID, adapter))
	}
	existing := any(registry[id])
	if existing != nil {
		if implementationType(existing) != implementationType(v) {
			panic(fmt.Errorf("%s %s and %s both claim format id %d: each format must have a unique id",
				kind, implementationType(existing), implementationType(v), id))
		}
		// Same implementation registered more than once, the first registration
		// (the configured one) wins.
		return id
	}
	registry[id] = v
	return id
}

// implementationType returns the concrete type of v, looking through the ID
// adapters so two different implementations wrapped with the same ID are
// detected as conflicting. Pointer types are dereferenced so a value and a
// pointer of the same implementation, such as JsonSerializer{} and
// &JsonSerializer{}, aren't reported as conflicting.
func implementationType(v any) reflect.Type {
	var t reflect.Type
	switch a := v.(type) {
	case *serializerWithID:
		t = reflect.TypeOf(a.Serializer)
	case *codecWithID:
		t = reflect.TypeOf(a.CompressionCodec)
	default:
		t = reflect.TypeOf(v)
	}
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

// isNil reports whether v is nil, including interfaces holding a typed nil
// pointer, map, slice, func or channel.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
		return rv.IsNil()
	default:
		return false
	}
}

// isBuiltin reports whether the concrete type of v is declared within one of the
// packages of this library that provide or wrap built-in implementations. Only
// built-in implementations may use the reserved IDs 0-127.
func isBuiltin(v any) bool {
	t := reflect.TypeOf(v)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	pkg := t.PkgPath()
	return pkg == modulePath ||
		pkg == modulePath+"/cacheproto" ||
		pkg == modulePath+"/cacheotel" ||
		strings.HasPrefix(pkg, modulePath+"/compression/")
}

// encode marshals and compresses v using the configured Serializer and
// CompressionCodec and prefixes the result with the format header.
func (f *formats) encode(v any) ([]byte, error) {
	data, err := f.serializer.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal value: %w", err)
	}
	data, err = f.codec.Compress(data)
	if err != nil {
		return nil, fmt.Errorf("compress value: %w", err)
	}
	out := make([]byte, headerSize+len(data))
	out[0] = formatMagic
	out[1] = formatVersion
	out[2] = f.serializerID
	out[3] = f.codecID
	copy(out[headerSize:], data)
	return out, nil
}

// decode decompresses and unmarshals data into dst.
//
// If data starts with the format header, the Serializer and CompressionCodec are
// selected by the IDs in the header. Otherwise, data is treated as a legacy
// headerless value and decoded with the configured Serializer and CompressionCodec.
// If a value looks like it has a header but can't be decoded, decoding is retried
// once as a legacy value before returning the original error. Values with a valid
// header referencing an unregistered Serializer or CompressionCodec are never
// retried and return ErrUnsupportedFormat.
//
// If decoding fails, dst is reset to its zero value so it never holds partially
// decoded data.
func (f *formats) decode(data []byte, dst any) error {
	if len(data) < headerSize || data[0] != formatMagic || data[1] != formatVersion {
		if err := decodeWith(f.serializer, f.codec, data, dst); err != nil {
			zeroDst(dst)
			return err
		}
		return nil
	}

	err := f.decodeHeadered(data, dst)
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrUnsupportedFormat) {
		return err
	}

	// The failed attempt may have partially populated dst, so it is reset before
	// retrying to avoid mixing data from both attempts.
	zeroDst(dst)
	if legacyErr := decodeWith(f.serializer, f.codec, data, dst); legacyErr == nil {
		return nil
	}
	zeroDst(dst)
	return err
}

// zeroDst sets the value dst points to to its zero value. It is a no-op if dst
// isn't a non-nil pointer.
func zeroDst(dst any) {
	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return
	}
	elem := rv.Elem()
	if elem.CanSet() {
		elem.SetZero()
	}
}

func (f *formats) decodeHeadered(data []byte, dst any) error {
	serializer := f.serializers[data[2]]
	if serializer == nil {
		return fmt.Errorf("%w: serializer id %d", ErrUnsupportedFormat, data[2])
	}
	codec := f.codecs[data[3]]
	if codec == nil {
		return fmt.Errorf("%w: codec id %d", ErrUnsupportedFormat, data[3])
	}
	return decodeWith(serializer, codec, data[headerSize:], dst)
}

func decodeWith(s Serializer, c CompressionCodec, data []byte, dst any) error {
	data, err := c.Decompress(data)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}
	if err := s.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("unmarshal value: %w", err)
	}
	return nil
}
