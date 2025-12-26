package cache

import (
	"encoding/json"

	"github.com/vmihailenco/msgpack/v5"
)

type marshaller func(v any) ([]byte, error)

type unmarshaller func(b []byte, v any) error

// A Serializer is a type that can marshal and unmarshal data types and structures between a GO
// application and Redis.
//
// Implementations of Serializer must be safe for concurrent use.
type Serializer interface {
	Marshal(interface{}) ([]byte, error)
	Unmarshal([]byte, interface{}) error
}

// JsonSerializer is a Serializer that uses JSON for marshaling and unmarshalling data.
type JsonSerializer struct{}

func (j JsonSerializer) Marshal(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

func (j JsonSerializer) Unmarshal(bytes []byte, dst interface{}) error {
	return json.Unmarshal(bytes, dst)
}

// MessagePackSerializer is a Serializer that uses MessagePack for marshaling and unmarshalling data.
type MessagePackSerializer struct{}

func (m MessagePackSerializer) Marshal(data interface{}) ([]byte, error) {
	return msgpack.Marshal(data)
}

func (m MessagePackSerializer) Unmarshal(bytes []byte, dst interface{}) error {
	return msgpack.Unmarshal(bytes, dst)
}
