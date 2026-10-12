package cache

import (
	"encoding/json/v2"

	"github.com/vmihailenco/msgpack/v5"
)

// A Serializer is a type that can marshal and unmarshal data types and structures between a GO
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

// JsonSerializer is a Serializer that uses JSON for marshaling and unmarshalling data.
type JsonSerializer struct{}

func (j JsonSerializer) Marshal(data any) ([]byte, error) {
	return json.Marshal(data)
}

func (j JsonSerializer) Unmarshal(bytes []byte, dst any) error {
	return json.Unmarshal(bytes, dst)
}

// FormatID returns SerializerJSON.
func (j JsonSerializer) FormatID() uint8 {
	return SerializerJSON
}

// MessagePackSerializer is a Serializer that uses MessagePack for marshaling and unmarshalling data.
type MessagePackSerializer struct{}

func (m MessagePackSerializer) Marshal(data any) ([]byte, error) {
	return msgpack.Marshal(data)
}

func (m MessagePackSerializer) Unmarshal(bytes []byte, dst any) error {
	return msgpack.Unmarshal(bytes, dst)
}

// FormatID returns SerializerMsgPack.
func (m MessagePackSerializer) FormatID() uint8 {
	return SerializerMsgPack
}
