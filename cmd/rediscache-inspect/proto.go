package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"google.golang.org/protobuf/encoding/protowire"
)

// maxProtoDepth is the deepest nested message protoText prints, like the default
// recursion limit of Protobuf decoders. Deeper length-delimited fields are printed
// as bytes, since every level parses the bytes of the levels below it again.
const maxProtoDepth = 100

// protoText formats data, a Protobuf message, without its schema, like
// protoc --decode_raw. Every field is printed with its number. Length-delimited
// fields are printed as strings if they hold printable text, as nested messages if
// they parse as one, and as escaped bytes otherwise. Fixed-size fields are printed
// in hex with their floating-point value, and varints that don't fit an int64 with
// their signed value.
func protoText(data []byte) (string, error) {
	var b strings.Builder
	if err := writeProto(&b, data, 0); err != nil {
		return "", err
	}
	return b.String(), nil
}

// writeProto writes the fields of data, a Protobuf message, to b, indented by
// depth levels.
func writeProto(b *strings.Builder, data []byte, depth int) error {
	indent := strings.Repeat("  ", depth)
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]

		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return fmt.Errorf("field %d: %w", num, protowire.ParseError(n))
			}
			data = data[n:]
			fmt.Fprintf(b, "%s%d: %d", indent, num, v)
			if v > math.MaxInt64 {
				fmt.Fprintf(b, "  # int64 %d", int64(v))
			}
			b.WriteByte('\n')

		case protowire.Fixed32Type:
			v, n := protowire.ConsumeFixed32(data)
			if n < 0 {
				return fmt.Errorf("field %d: %w", num, protowire.ParseError(n))
			}
			data = data[n:]
			fmt.Fprintf(b, "%s%d: 0x%08x  # float %s\n", indent, num, v,
				strconv.FormatFloat(float64(math.Float32frombits(v)), 'g', -1, 32))

		case protowire.Fixed64Type:
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return fmt.Errorf("field %d: %w", num, protowire.ParseError(n))
			}
			data = data[n:]
			fmt.Fprintf(b, "%s%d: 0x%016x  # double %s\n", indent, num, v,
				strconv.FormatFloat(math.Float64frombits(v), 'g', -1, 64))

		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return fmt.Errorf("field %d: %w", num, protowire.ParseError(n))
			}
			data = data[n:]
			switch {
			case len(v) > 0 && isText(v):
				fmt.Fprintf(b, "%s%d: %s\n", indent, num, strconv.Quote(string(v)))
			case depth < maxProtoDepth && isProtoMessage(v):
				fmt.Fprintf(b, "%s%d {\n", indent, num)
				if err := writeProto(b, v, depth+1); err != nil {
					return err
				}
				fmt.Fprintf(b, "%s}\n", indent)
			default:
				fmt.Fprintf(b, "%s%d: %s\n", indent, num, strconv.Quote(string(v)))
			}

		case protowire.StartGroupType:
			v, n := protowire.ConsumeGroup(num, data)
			if n < 0 {
				return fmt.Errorf("field %d: %w", num, protowire.ParseError(n))
			}
			if depth >= maxProtoDepth {
				return fmt.Errorf("field %d: groups nested more than %d levels deep", num, maxProtoDepth)
			}
			data = data[n:]
			fmt.Fprintf(b, "%s%d {\n", indent, num)
			if err := writeProto(b, v, depth+1); err != nil {
				return err
			}
			fmt.Fprintf(b, "%s}\n", indent)

		default:
			return fmt.Errorf("field %d: unexpected wire type %d", num, typ)
		}
	}
	return nil
}

// isProtoMessage reports whether data parses as a non-empty Protobuf message.
func isProtoMessage(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 || typ == protowire.EndGroupType {
			return false
		}
		data = data[n:]
		n = protowire.ConsumeFieldValue(num, typ, data)
		if n < 0 {
			return false
		}
		data = data[n:]
	}
	return true
}
