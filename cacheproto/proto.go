package cacheproto

import (
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ProtobufSerializer is an implementation of Serializer that uses Protobuf to marshal and unmarshal data.
type ProtobufSerializer struct{}

func (p ProtobufSerializer) Marshal(data interface{}) ([]byte, error) {
	msg, ok := data.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T is not an implementation of proto.Message", data)
	}
	return proto.Marshal(msg)
}

func (p ProtobufSerializer) Unmarshal(bytes []byte, dst interface{}) error {
	msg, ok := dst.(proto.Message)
	if !ok {
		return fmt.Errorf("%T is not an implementation of proto.Message", dst)
	}
	return proto.Unmarshal(bytes, msg)
}
