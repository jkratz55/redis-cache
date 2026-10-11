package cache

import (
	"encoding/json/v2"

	"github.com/vmihailenco/msgpack/v5"
)

// A Serializer is a type that can marshal and unmarshal data types and structures between a GO
// application and Redis.
//
// Implementations of Serializer must be safe for concurrent use.
type Serializer interface {
	Marshal(any) ([]byte, error)
	Unmarshal([]byte, any) error
}

// JsonSerializer is a Serializer that uses JSON for marshaling and unmarshalling data.
type JsonSerializer struct{}

func (j *JsonSerializer) Marshal(data any) ([]byte, error) {
	return json.Marshal(data)
}

func (j *JsonSerializer) Unmarshal(bytes []byte, dst any) error {
	return json.Unmarshal(bytes, dst)
}

// MessagePackSerializer is a Serializer that uses MessagePack for marshaling and unmarshalling data.
type MessagePackSerializer struct{}

func (m *MessagePackSerializer) Marshal(data any) ([]byte, error) {
	return msgpack.Marshal(data)
}

func (m *MessagePackSerializer) Unmarshal(bytes []byte, dst any) error {
	return msgpack.Unmarshal(bytes, dst)
}
