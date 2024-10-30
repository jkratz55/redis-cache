package cache

import (
	"github.com/vmihailenco/msgpack/v5"
)

// Marshaller is a function type that marshals the value of a cache entry for
// storage.
type Marshaller func(v any) ([]byte, error)

// Unmarshaller is a function type that unmarshalls the value retrieved from the
// cache into the target type.
type Unmarshaller func(b []byte, v any) error

// DefaultMarshaller returns a Marshaller using msgpack to marshall
// values.
func DefaultMarshaller() Marshaller {
	return func(v any) ([]byte, error) {
		return msgpack.Marshal(v)
	}
}

// DefaultUnmarshaller returns an Unmarshaller using msgpack to unmarshall
// values.
func DefaultUnmarshaller() Unmarshaller {
	return func(b []byte, v any) error {
		return msgpack.Unmarshal(b, v)
	}
}
