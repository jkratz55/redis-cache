package main

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestProtoText(t *testing.T) {
	var nested []byte
	nested = protowire.AppendTag(nested, 1, protowire.BytesType)
	nested = protowire.AppendString(nested, "inner")

	var group []byte
	group = protowire.AppendTag(group, 1, protowire.VarintType)
	group = protowire.AppendVarint(group, 5)

	var msg []byte
	msg = protowire.AppendTag(msg, 1, protowire.VarintType)
	msg = protowire.AppendVarint(msg, 150)
	msg = protowire.AppendTag(msg, 2, protowire.VarintType)
	msg = protowire.AppendVarint(msg, math.MaxUint64)
	msg = protowire.AppendTag(msg, 3, protowire.Fixed32Type)
	msg = protowire.AppendFixed32(msg, math.Float32bits(1.5))
	msg = protowire.AppendTag(msg, 4, protowire.Fixed64Type)
	msg = protowire.AppendFixed64(msg, math.Float64bits(-0.25))
	msg = protowire.AppendTag(msg, 5, protowire.BytesType)
	msg = protowire.AppendString(msg, "hello, world")
	msg = protowire.AppendTag(msg, 6, protowire.BytesType)
	msg = protowire.AppendBytes(msg, nested)
	msg = protowire.AppendTag(msg, 7, protowire.BytesType)
	msg = protowire.AppendBytes(msg, []byte{0xff, 0x00})
	msg = protowire.AppendTag(msg, 8, protowire.BytesType)
	msg = protowire.AppendBytes(msg, nil)
	msg = protowire.AppendTag(msg, 9, protowire.StartGroupType)
	msg = append(msg, group...)
	msg = protowire.AppendTag(msg, 9, protowire.EndGroupType)

	got, err := protoText(msg)
	assert.NoError(t, err)
	assert.Equal(t, `1: 150
2: 18446744073709551615  # int64 -1
3: 0x3fc00000  # float 1.5
4: 0xbfd0000000000000  # double -0.25
5: "hello, world"
6 {
  1: "inner"
}
7: "\xff\x00"
8: ""
9 {
  1: 5
}
`, got)
}

func TestProtoText_Depth(t *testing.T) {
	var msg []byte
	for range maxProtoDepth + 10 {
		var outer []byte
		outer = protowire.AppendTag(outer, 1, protowire.BytesType)
		msg = protowire.AppendBytes(outer, msg)
	}

	got, err := protoText(msg)
	assert.NoError(t, err)
	// Messages nested deeper than maxProtoDepth are printed as bytes.
	assert.Equal(t, maxProtoDepth, strings.Count(got, "{"))
}

func TestProtoText_Invalid(t *testing.T) {
	for name, msg := range map[string][]byte{
		"truncated tag":    {0x80},
		"truncated varint": {0x08, 0x80},
		"truncated bytes":  {0x0a, 0x05, 'a'},
		"invalid wire":     {0x0f},
		"field number 0":   {0x00, 0x01},
		"unmatched group":  {0x0b, 0x08, 0x01},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := protoText(msg)
			assert.Error(t, err)
		})
	}
}
