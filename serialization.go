package cache

import (
	"bytes"
	"encoding/json/v2"

	"github.com/vmihailenco/msgpack/v5"
)

// A Serializer is a type that can marshal and unmarshal data types and structures between a Go
// application and Redis.
//
// Implementations of Serializer must be safe for concurrent use.
//
// Implementations must also have a format ID, which is stored alongside every
// cached value, either by implementing FormatIdentifier or by being wrapped with
// SerializerWithID.
type Serializer interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

// StrictUnmarshaler is implemented by Serializers whose Unmarshal accepts data that
// wasn't produced by Marshal for the destination type, such as Protobuf, which
// keeps fields it doesn't recognize instead of failing.
//
// Values written by versions of the library before v2.0.0 have no format header
// and can start with the same bytes as the header. When a value with a header
// can't be decoded, it is decoded once more as a value without a header, using
// UnmarshalStrict if the Serializer implements it. UnmarshalStrict must return an
// error if data contains anything Unmarshal would ignore, so a corrupt value or a
// value written in an unsupported format isn't mistaken for a value without a
// header.
type StrictUnmarshaler interface {
	UnmarshalStrict([]byte, any) error
}

// MarshalAppender is an optional interface implemented by Serializers that can
// append the serialized value to a byte slice. When no compression is used, Cache
// and TypedCache use it to serialize values directly after the format header,
// which saves an allocation and a copy of the value on every write.
// MessagePackSerializer implements it.
//
// AppendMarshal must append the same bytes Marshal returns to dst and return the
// extended slice. It must not modify dst[:len(dst)] or retain dst.
type MarshalAppender interface {
	AppendMarshal(dst []byte, v any) ([]byte, error)
}

// appendMarshal appends v serialized by s to dst, using AppendMarshal if s
// implements MarshalAppender.
func appendMarshal(s Serializer, dst []byte, v any) ([]byte, error) {
	if ma, ok := s.(MarshalAppender); ok {
		return ma.AppendMarshal(dst, v)
	}
	data, err := s.Marshal(v)
	if err != nil {
		return nil, err
	}
	return append(dst, data...), nil
}

// JsonSerializer is a Serializer that uses JSON for marshaling and unmarshalling data.
//
// JsonSerializer uses encoding/json/v2 with its default options, which differ from
// encoding/json, the package used by v1 of this library. For example, time.Duration
// can't be marshaled or unmarshaled without a format option, object names are
// matched case-sensitively when unmarshaling, and nil slices and maps are marshaled
// as [] and {} instead of null. Values written as JSON by v1 of this library may not
// decode, or may decode differently. See the encoding/json/v2 documentation for every
// difference.
//
// To keep the behavior of encoding/json, use a custom Serializer that calls
// encoding/json, or encoding/json/v2 with json.DefaultOptionsV1(), with its own format
// ID, for example by wrapping it with SerializerWithID.
type JsonSerializer struct{}

// Marshal returns the JSON encoding of data using encoding/json/v2.
func (j JsonSerializer) Marshal(data any) ([]byte, error) {
	return json.Marshal(data)
}

// Unmarshal decodes the JSON in bytes into dst using encoding/json/v2.
func (j JsonSerializer) Unmarshal(bytes []byte, dst any) error {
	return json.Unmarshal(bytes, dst)
}

// FormatID returns SerializerJSON.
func (j JsonSerializer) FormatID() uint8 {
	return SerializerJSON
}

// MessagePackSerializer is a Serializer that uses MessagePack for marshaling and unmarshalling data.
type MessagePackSerializer struct{}

// Marshal returns the MessagePack encoding of data.
func (m MessagePackSerializer) Marshal(data any) ([]byte, error) {
	return msgpack.Marshal(data)
}

// AppendMarshal appends the MessagePack encoding of data to dst and returns the
// extended slice.
func (m MessagePackSerializer) AppendMarshal(dst []byte, data any) ([]byte, error) {
	// This mirrors msgpack.Marshal, but the encoder writes to dst instead of an
	// empty buffer.
	buf := bytes.NewBuffer(dst)
	enc := msgpack.GetEncoder()
	enc.Reset(buf)
	err := enc.Encode(data)
	msgpack.PutEncoder(enc)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Unmarshal decodes the MessagePack encoded bytes into dst.
func (m MessagePackSerializer) Unmarshal(bytes []byte, dst any) error {
	return msgpack.Unmarshal(bytes, dst)
}

// FormatID returns SerializerMsgPack.
func (m MessagePackSerializer) FormatID() uint8 {
	return SerializerMsgPack
}
