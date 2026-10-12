package cacheproto

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	cache "github.com/jkratz55/redis-cache/v2"
)

// ProtobufSerializer is an implementation of Serializer that uses Protobuf to marshal and unmarshal data.
type ProtobufSerializer struct{}

func (p ProtobufSerializer) Marshal(data any) ([]byte, error) {
	msg, ok := data.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T is not an implementation of proto.Message", data)
	}
	return proto.Marshal(msg)
}

func (p ProtobufSerializer) Unmarshal(bytes []byte, dst any) error {
	msg, ok := dst.(proto.Message)
	if !ok {
		return fmt.Errorf("%T is not an implementation of proto.Message", dst)
	}
	return proto.Unmarshal(bytes, msg)
}

// UnmarshalStrict is like Unmarshal but returns an error if data contains fields
// that aren't defined by the message type of dst. Protobuf keeps unknown fields
// instead of failing, so the cache uses UnmarshalStrict to decode values that may
// have been written without a format header (see cache.StrictUnmarshaler).
func (p ProtobufSerializer) UnmarshalStrict(bytes []byte, dst any) error {
	msg, ok := dst.(proto.Message)
	if !ok {
		return fmt.Errorf("%T is not an implementation of proto.Message", dst)
	}
	if err := proto.Unmarshal(bytes, msg); err != nil {
		return err
	}
	if len(msg.ProtoReflect().GetUnknown()) > 0 {
		return fmt.Errorf("%T contains unknown fields", dst)
	}
	return nil
}

// FormatID returns cache.SerializerProtobuf.
func (p ProtobufSerializer) FormatID() uint8 {
	return cache.SerializerProtobuf
}
