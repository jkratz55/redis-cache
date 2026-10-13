package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json/jsontext"
	"fmt"
	"maps"
	"slices"
	"time"
	"unicode"
	"unicode/utf8"

	kzstd "github.com/klauspost/compress/zstd"
	"github.com/vmihailenco/msgpack/v5"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/flate"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
	"github.com/jkratz55/redis-cache/v2/compression/zstd"
)

// The format header redis-cache writes before every value, see format.go in the
// root package. TestInspect_BuiltInFormats fails if they drift apart.
const (
	formatMagic   byte = 0xC1
	formatVersion byte = 0x01
	headerSize         = 4
)

// encoding is how a value is serialized.
type encoding int

const (
	encodingMsgPack encoding = iota + 1
	encodingJSON
	encodingProtobuf
	// encodingText and encodingBinary are only detected for values without a
	// header, which may not have been written by redis-cache.
	encodingText
	encodingBinary
)

// encodingNames are the display names of the encodings.
var encodingNames = map[encoding]string{
	encodingMsgPack:  "MessagePack",
	encodingJSON:     "JSON",
	encodingProtobuf: "Protobuf",
	encodingText:     "plain text",
	encodingBinary:   "binary",
}

// serializerEncodings are the encodings of the built-in serializers, by format ID.
var serializerEncodings = map[uint8]encoding{
	cache.SerializerMsgPack:  encodingMsgPack,
	cache.SerializerJSON:     encodingJSON,
	cache.SerializerProtobuf: encodingProtobuf,
}

// legacyEncodings are the values of -legacy-encoding besides auto, in the order of
// legacyEncodingFlags.
var legacyEncodings = []string{"msgpack", "json", "protobuf", "text", "binary"}

var legacyEncodingFlags = map[string]encoding{
	"msgpack":  encodingMsgPack,
	"json":     encodingJSON,
	"protobuf": encodingProtobuf,
	"text":     encodingText,
	"binary":   encodingBinary,
}

// codecNames are the flag and display names of the built-in codecs, by format ID.
var codecNames = map[uint8]struct{ flag, name string }{
	cache.CodecNone:   {"none", "none"},
	cache.CodecFlate:  {"flate", "Flate"},
	cache.CodecGzip:   {"gzip", "Gzip"},
	cache.CodecLZ4:    {"lz4", "LZ4"},
	cache.CodecS2:     {"s2", "S2"},
	cache.CodecSnappy: {"snappy", "Snappy"},
	cache.CodecZstd:   {"zstd", "Zstandard"},
}

// codecFlagNames returns the flag names of the built-in codecs, ordered by ID.
func codecFlagNames() []string {
	names := make([]string, 0, len(codecNames))
	for _, id := range slices.Sorted(maps.Keys(codecNames)) {
		names = append(names, codecNames[id].flag)
	}
	return names
}

// jsonOptions accept JSON that isn't strictly valid, since values are displayed
// as they are rather than validated.
var jsonOptions = []jsontext.Options{
	jsontext.AllowDuplicateNames(true),
	jsontext.AllowInvalidUTF8(true),
}

// hexDumpLimit is the number of bytes printed by hexDump.
const hexDumpLimit = 1024

// inspection is the outcome of fetching a key.
type inspection struct {
	key   string
	found bool
	// redisType is the type of the key if it isn't a string.
	redisType string
	// ttl is the remaining time to live, or a negative duration if the key doesn't
	// expire.
	ttl     time.Duration
	showHex bool
	// err is set if Redis failed.
	err error
	result
}

// ok reports whether the key was found and its value decoded.
func (i inspection) ok() bool {
	return i.found && i.redisType == "" && i.err == nil && i.result.err == nil
}

// format describes the encoding or compression of a value.
type format struct {
	name string
	// id is the format ID from the header, or -1 if the value has no header.
	id int
	// note says how the format was determined when it isn't from the header.
	note string
}

// result is a value read from Redis, decoded as far as possible.
type result struct {
	stored      []byte
	header      bool
	encoding    format
	compression format
	// decodedSize is the size of the value after decompression, or -1 if it can't
	// be decompressed.
	decodedSize int
	// value is the value as JSON if it has a JSON form, otherwise text is the
	// value as text: the Protobuf wire format, plain text, or a hex dump.
	value jsontext.Value
	text  string
	// hint suggests how to decode a value that couldn't be decoded.
	hint string
	// err is set if the value couldn't be decoded.
	err error
}

// decoder decodes values with the built-in codecs and serializers.
type decoder struct {
	codecs            map[uint8]cache.CompressionCodec
	zstd              *zstd.Codec
	legacyEncoding    string
	legacyCompression string
}

// newDecoder returns a decoder whose codecs decompress values up to maxSize bytes.
// legacyEncoding and legacyCompression are the -legacy-encoding and
// -legacy-compression flags.
func newDecoder(maxSize byteSize, legacyEncoding, legacyCompression string) (*decoder, error) {
	if _, ok := legacyEncodingFlags[legacyEncoding]; !ok && legacyEncoding != "auto" {
		return nil, fmt.Errorf("invalid -legacy-encoding %q", legacyEncoding)
	}
	if _, ok := codecByFlag(legacyCompression); !ok && legacyCompression != "auto" {
		return nil, fmt.Errorf("invalid -legacy-compression %q", legacyCompression)
	}

	n := uint64(maxSize)
	z, err := zstd.NewCodec(kzstd.SpeedFastest, zstd.WithMaxDecodedSize(n))
	if err != nil {
		return nil, fmt.Errorf("create zstd codec: %w", err)
	}
	return &decoder{
		codecs: map[uint8]cache.CompressionCodec{
			// The level only affects compression.
			cache.CodecFlate:  flate.NewCodec(1, flate.WithMaxDecodedSize(n)),
			cache.CodecGzip:   gzip.NewCodec(1, gzip.WithMaxDecodedSize(n)),
			cache.CodecLZ4:    lz4.NewCodec(lz4.WithMaxDecodedSize(n)),
			cache.CodecS2:     s2.NewCodec(s2.WithMaxDecodedSize(n)),
			cache.CodecSnappy: snappy.NewCodec(snappy.WithMaxDecodedSize(n)),
			cache.CodecZstd:   z,
		},
		zstd:              z,
		legacyEncoding:    legacyEncoding,
		legacyCompression: legacyCompression,
	}, nil
}

// Close releases the resources of the zstd codec.
func (d *decoder) Close() error {
	return d.zstd.Close()
}

// codecByFlag returns the ID of the codec with the flag name.
func codecByFlag(name string) (uint8, bool) {
	for id, c := range codecNames {
		if c.flag == name {
			return id, true
		}
	}
	return 0, false
}

// inspect decodes data, a value stored in Redis.
func (d *decoder) inspect(data []byte) result {
	r := result{stored: data, decodedSize: -1}
	if len(data) >= headerSize && data[0] == formatMagic && data[1] == formatVersion {
		d.inspectHeader(&r, data[2], data[3], data[headerSize:])
	} else {
		d.inspectLegacy(&r, data)
	}
	return r
}

// inspectHeader decodes payload, the value after the header, with the serializer
// and codec of the header.
func (d *decoder) inspectHeader(r *result, serializerID, codecID uint8, payload []byte) {
	r.header = true
	enc, encKnown := serializerEncodings[serializerID]
	r.encoding = format{id: int(serializerID), name: encodingNames[enc]}
	if !encKnown {
		r.encoding.name = "custom"
		r.encoding.note = "not built in"
	}
	codec, codecKnown := codecNames[codecID]
	r.compression = format{id: int(codecID), name: codec.name}
	if !codecKnown {
		r.compression.name = "custom"
		r.compression.note = "not built in"
		r.text = hexDump(payload)
		r.hint = "the value is shown compressed, since its codec isn't built in"
		return
	}

	data, err := d.decompress(codecID, payload)
	if err != nil {
		r.err = err
		r.text = hexDump(payload)
		return
	}
	r.decodedSize = len(data)
	if !encKnown {
		r.text = hexDump(data)
		r.hint = "the value is shown as hex, since its serializer isn't built in"
		return
	}
	r.decode(enc, data)
}

// inspectLegacy decodes data, a value without a header, with the encoding and
// compression of the -legacy-encoding and -legacy-compression flags, or detected.
func (d *decoder) inspectLegacy(r *result, data []byte) {
	codecID, note := cache.CodecNone, "detected"
	if d.legacyCompression == "auto" {
		if id, ok := detectCodec(data); ok {
			// The value may only start with the signature by chance.
			if out, err := d.decompress(id, data); err == nil {
				codecID, data = id, out
			}
		}
	} else {
		codecID, _ = codecByFlag(d.legacyCompression)
		note = "-legacy-compression"
		out, err := d.decompress(codecID, data)
		if err != nil {
			r.compression = format{id: -1, name: codecNames[codecID].name, note: note}
			r.err = err
			r.text = hexDump(data)
			return
		}
		data = out
	}
	r.compression = format{id: -1, name: codecNames[codecID].name, note: note}
	r.decodedSize = len(data)

	enc, note := legacyEncodingFlags[d.legacyEncoding], "-legacy-encoding"
	if d.legacyEncoding == "auto" {
		enc, note = detectEncoding(data), "detected"
	}
	r.encoding = format{id: -1, name: encodingNames[enc], note: note}
	if enc == encodingBinary && d.legacyCompression == "auto" && codecID == cache.CodecNone {
		r.hint = "values compressed with flate, s2, or snappy can't be detected, set -legacy-compression"
	}
	r.decode(enc, data)
}

// decompress decompresses data with the built-in codec id.
func (d *decoder) decompress(id uint8, data []byte) ([]byte, error) {
	if id == cache.CodecNone {
		return data, nil
	}
	out, err := d.codecs[id].Decompress(data)
	if err != nil {
		return nil, fmt.Errorf("decompress with %s: %w", codecNames[id].name, err)
	}
	return out, nil
}

// decode decodes data, a serialized value, into r.
func (r *result) decode(enc encoding, data []byte) {
	var err error
	switch enc {
	case encodingMsgPack:
		r.value, err = msgpackToJSON(data)
	case encodingJSON:
		r.value, err = compactJSON(data)
	case encodingProtobuf:
		r.text, err = protoText(data)
	case encodingText:
		r.text = string(data)
	case encodingBinary:
		r.text = hexDump(data)
	}
	if err != nil {
		r.err = fmt.Errorf("decode %s: %w", encodingNames[enc], err)
		r.value, r.text = nil, hexDump(data)
	}
}

// compactJSON returns data, a JSON value, without insignificant whitespace.
func compactJSON(data []byte) (jsontext.Value, error) {
	// Compact rewrites the value in place, and data can share its memory with a
	// string read from Redis.
	v := jsontext.Value(slices.Clone(data))
	if err := v.Compact(jsonOptions...); err != nil {
		return nil, err
	}
	return v, nil
}

// detectCodec returns the codec of data, a value without a header, if it starts
// with the signature of a codec. Flate, S2, and Snappy values have no signature.
func detectCodec(data []byte) (uint8, bool) {
	switch {
	case bytes.HasPrefix(data, []byte{0x1f, 0x8b}):
		return cache.CodecGzip, true
	case bytes.HasPrefix(data, []byte{0x28, 0xb5, 0x2f, 0xfd}):
		return cache.CodecZstd, true
	case bytes.HasPrefix(data, []byte{0x04, 0x22, 0x4d, 0x18}):
		return cache.CodecLZ4, true
	}
	return cache.CodecNone, false
}

// detectEncoding returns the encoding of data, a serialized value without a
// header. Protobuf can't be detected, since nearly any bytes are valid Protobuf.
func detectEncoding(data []byte) encoding {
	switch {
	case len(data) > 0 && jsontext.Value(data).IsValid(jsonOptions...):
		return encodingJSON
	case isMsgpack(data):
		return encodingMsgPack
	case isText(data):
		return encodingText
	}
	return encodingBinary
}

// isMsgpack reports whether data holds exactly one MessagePack value.
func isMsgpack(data []byte) bool {
	r := bytes.NewReader(data)
	return len(data) > 0 && msgpack.NewDecoder(r).Skip() == nil && r.Len() == 0
}

// isText reports whether data is printable UTF-8 text.
func isText(data []byte) bool {
	if !utf8.Valid(data) {
		return false
	}
	for _, r := range string(data) {
		if !unicode.IsPrint(r) && !unicode.IsSpace(r) {
			return false
		}
	}
	return true
}

// hexDump returns a hex dump of the first hexDumpLimit bytes of data.
func hexDump(data []byte) string {
	if len(data) <= hexDumpLimit {
		return hex.Dump(data)
	}
	return hex.Dump(data[:hexDumpLimit]) + fmt.Sprintf("... %d more bytes\n", len(data)-hexDumpLimit)
}
