package cacheproto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtobufSerializer_UnmarshalStrict(t *testing.T) {
	s := ProtobufSerializer{}
	data, err := s.Marshal(wrapperspb.String("hello"))
	assert.NoError(t, err)

	actual := &wrapperspb.StringValue{}
	assert.NoError(t, s.UnmarshalStrict(data, actual))
	assert.Equal(t, "hello", actual.GetValue())

	// Field 2 (varint 1) isn't defined by StringValue. Unmarshal keeps it as an
	// unknown field, UnmarshalStrict rejects it.
	unknown := append(data, 0x10, 0x01)
	assert.NoError(t, s.Unmarshal(unknown, &wrapperspb.StringValue{}))
	assert.ErrorContains(t, s.UnmarshalStrict(unknown, &wrapperspb.StringValue{}), "unknown fields")

	assert.Error(t, s.UnmarshalStrict(data, &struct{}{}))
}
