package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json/jsontext"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	"github.com/vmihailenco/msgpack/v5/msgpcode"
)

// msgpackToJSON converts data, a single MessagePack value, to JSON. Unlike decoding
// into an any and marshaling it, map keys keep their order, so structs show their
// fields in declaration order. Keys that aren't strings use their JSON text, binary
// data is shown as text if it's printable and base64-encoded otherwise, and
// timestamps use RFC 3339 in UTC.
func msgpackToJSON(data []byte) (jsontext.Value, error) {
	// Skip validates the whole value first, so nothing is converted from an invalid
	// value. It reads strings in chunks rather than allocating their declared
	// length.
	r := bytes.NewReader(data)
	if err := msgpack.NewDecoder(r).Skip(); err != nil {
		return nil, err
	}
	if r.Len() > 0 {
		return nil, fmt.Errorf("%d bytes after the value", r.Len())
	}

	var buf bytes.Buffer
	enc := jsontext.NewEncoder(&buf, jsonOptions...)
	if err := writeMsgpack(msgpack.NewDecoder(bytes.NewReader(data)), enc); err != nil {
		return nil, err
	}
	// The encoder ends every top-level value with a newline.
	return jsontext.Value(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

// writeMsgpack converts the next MessagePack value of dec to JSON written to enc.
//
// Maps and arrays are never decoded with DecodeInterface: msgpack v5.4.1 allocates
// the length they declare up front, so a corrupt value could allocate gigabytes.
func writeMsgpack(dec *msgpack.Decoder, enc *jsontext.Encoder) error {
	c, err := dec.PeekCode()
	if err != nil {
		return err
	}

	switch {
	case msgpcode.IsFixedMap(c) || c == msgpcode.Map16 || c == msgpcode.Map32:
		n, err := dec.DecodeMapLen()
		if err != nil {
			return err
		}
		if err := enc.WriteToken(jsontext.BeginObject); err != nil {
			return err
		}
		for range n {
			key, err := msgpackKey(dec)
			if err != nil {
				return err
			}
			if err := enc.WriteToken(jsontext.String(key)); err != nil {
				return err
			}
			if err := writeMsgpack(dec, enc); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndObject)

	case msgpcode.IsFixedArray(c) || c == msgpcode.Array16 || c == msgpcode.Array32:
		n, err := dec.DecodeArrayLen()
		if err != nil {
			return err
		}
		if err := enc.WriteToken(jsontext.BeginArray); err != nil {
			return err
		}
		for range n {
			if err := writeMsgpack(dec, enc); err != nil {
				return err
			}
		}
		return enc.WriteToken(jsontext.EndArray)

	case msgpcode.IsExt(c):
		raw, err := dec.DecodeRaw()
		if err != nil {
			return err
		}
		// Timestamps are the only extension registered by default.
		var v any
		if err := msgpack.Unmarshal(raw, &v); err != nil {
			return enc.WriteToken(jsontext.String("msgpack extension " + base64.StdEncoding.EncodeToString(raw)))
		}
		return writeScalar(enc, v)
	}

	v, err := dec.DecodeInterface()
	if err != nil {
		return err
	}
	return writeScalar(enc, v)
}

// msgpackKey decodes the next MessagePack value of dec as an object name.
func msgpackKey(dec *msgpack.Decoder) (string, error) {
	c, err := dec.PeekCode()
	if err != nil {
		return "", err
	}
	if msgpcode.IsString(c) || msgpcode.IsBin(c) {
		return dec.DecodeString()
	}

	// Keys that aren't strings use their JSON text, such as 42 or [1,2].
	var buf bytes.Buffer
	if err := writeMsgpack(dec, jsontext.NewEncoder(&buf, jsonOptions...)); err != nil {
		return "", err
	}
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n"))), nil
}

// writeScalar writes v, a MessagePack value that isn't a map or an array, to enc.
func writeScalar(enc *jsontext.Encoder, v any) error {
	switch v := v.(type) {
	case nil:
		return enc.WriteToken(jsontext.Null)
	case bool:
		return enc.WriteToken(jsontext.Bool(v))
	case int8:
		return enc.WriteToken(jsontext.Int(int64(v)))
	case int16:
		return enc.WriteToken(jsontext.Int(int64(v)))
	case int32:
		return enc.WriteToken(jsontext.Int(int64(v)))
	case int64:
		return enc.WriteToken(jsontext.Int(v))
	case uint8:
		return enc.WriteToken(jsontext.Uint(uint64(v)))
	case uint16:
		return enc.WriteToken(jsontext.Uint(uint64(v)))
	case uint32:
		return enc.WriteToken(jsontext.Uint(uint64(v)))
	case uint64:
		return enc.WriteToken(jsontext.Uint(v))
	case float32:
		// Formatted with 32 bits so 0.1 isn't printed as 0.10000000149011612.
		return writeFloat(enc, float64(v), 32)
	case float64:
		return writeFloat(enc, v, 64)
	case string:
		return enc.WriteToken(jsontext.String(v))
	case []byte:
		if isText(v) {
			return enc.WriteToken(jsontext.String(string(v)))
		}
		return enc.WriteToken(jsontext.String(base64.StdEncoding.EncodeToString(v)))
	case time.Time:
		// msgpack decodes timestamps in the local time zone.
		return enc.WriteToken(jsontext.String(v.UTC().Format(time.RFC3339Nano)))
	}
	return enc.WriteToken(jsontext.String(fmt.Sprint(v)))
}

// writeFloat writes f, a float of bitSize bits, to enc. JSON has no NaN or
// infinity, so they are written as strings.
func writeFloat(enc *jsontext.Encoder, f float64, bitSize int) error {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return enc.WriteToken(jsontext.String(strconv.FormatFloat(f, 'g', -1, bitSize)))
	}
	return enc.WriteValue(jsontext.Value(strconv.FormatFloat(f, 'g', -1, bitSize)))
}
