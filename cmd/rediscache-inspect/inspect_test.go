package main

import (
	"cmp"
	"context"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	kzstd "github.com/klauspost/compress/zstd"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/cacheproto"
	"github.com/jkratz55/redis-cache/v2/compression/flate"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
	"github.com/jkratz55/redis-cache/v2/compression/zstd"
	"github.com/jkratz55/redis-cache/v2/internal/benchmarks/benchpb"
)

type testUser struct {
	ID   int
	Name string
	Tags []string
}

var (
	user     = testUser{ID: 42, Name: "Ada", Tags: []string{"a", "b"}}
	userJSON = `{"ID":42,"Name":"Ada","Tags":["a","b"]}`

	order = &benchpb.Order{
		Id:              7,
		CustomerId:      "c-19",
		ShippingAddress: &benchpb.Address{City: "Austin"},
		Total:           12.5,
	}
	orderText = "1: 7\n2: \"c-19\"\n5 {\n  2: \"Austin\"\n}\n9: 0x4029000000000000  # double 12.5\n"
)

// newTestDecoder returns a decoder with the flags legacyEncoding and
// legacyCompression and a 1 MiB limit.
func newTestDecoder(t *testing.T, legacyEncoding, legacyCompression string) *decoder {
	t.Helper()
	d, err := newDecoder(1<<20, legacyEncoding, legacyCompression)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d
}

// encode returns val as a Cache configured with opts stores it in Redis.
func encode(t *testing.T, val any, opts ...cache.Option) []byte {
	t.Helper()
	s := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
	defer rdb.Close()

	ctx := context.Background()
	if err := cache.New(rdb, opts...).Set(ctx, "key", val, 0); err != nil {
		t.Fatal(err)
	}
	data, err := rdb.Get(ctx, "key").Bytes()
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func newZstd(t *testing.T) *zstd.Codec {
	t.Helper()
	z, err := zstd.NewCodec(kzstd.SpeedDefault)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { z.Close() })
	return z
}

func TestInspect_BuiltInFormats(t *testing.T) {
	codecs := []struct {
		name  string
		id    uint8
		codec cache.CompressionCodec
	}{
		{"none", cache.CodecNone, nil},
		{"Flate", cache.CodecFlate, flate.NewCodec(6)},
		{"Gzip", cache.CodecGzip, gzip.NewCodec(6)},
		{"LZ4", cache.CodecLZ4, lz4.NewCodec()},
		{"S2", cache.CodecS2, s2.NewCodec()},
		{"Snappy", cache.CodecSnappy, snappy.NewCodec()},
		{"Zstandard", cache.CodecZstd, newZstd(t)},
	}
	serializers := []struct {
		name       string
		id         uint8
		serializer cache.Serializer
		val        any
		json, text string
	}{
		{"MessagePack", cache.SerializerMsgPack, cache.MessagePackSerializer{}, user, userJSON, ""},
		{"JSON", cache.SerializerJSON, cache.JsonSerializer{}, user, userJSON, ""},
		{"Protobuf", cache.SerializerProtobuf, cacheproto.ProtobufSerializer{}, order, "", orderText},
	}
	d := newTestDecoder(t, "auto", "auto")

	for _, s := range serializers {
		for _, c := range codecs {
			t.Run(s.name+"/"+c.name, func(t *testing.T) {
				opts := []cache.Option{cache.Serialization(s.serializer)}
				if c.codec != nil {
					opts = append(opts, cache.Compression(c.codec))
				}
				data := encode(t, s.val, opts...)

				r := d.inspect(data)
				assert.NoError(t, r.err)
				assert.True(t, r.header)
				assert.Equal(t, format{name: s.name, id: int(s.id)}, r.encoding)
				assert.Equal(t, format{name: c.name, id: int(c.id)}, r.compression)
				assert.Equal(t, s.json, string(r.value))
				assert.Equal(t, s.text, r.text)
				assert.Empty(t, r.hint)
				if c.codec == nil {
					assert.Equal(t, len(data)-headerSize, r.decodedSize)
				}
			})
		}
	}
}

func TestInspect_Legacy(t *testing.T) {
	jsonValue := []byte(`{"name": "v1"}`)
	msgpackValue, err := msgpack.Marshal(map[string]string{"name": "v1"})
	assert.NoError(t, err)
	gzipped, err := gzip.NewCodec(6).Compress(jsonValue)
	assert.NoError(t, err)
	zstded, err := newZstd(t).Compress(msgpackValue)
	assert.NoError(t, err)
	snappied, err := snappy.NewCodec().Compress(jsonValue)
	assert.NoError(t, err)
	orderValue := encode(t, order, cache.Serialization(cacheproto.ProtobufSerializer{}))[headerSize:]

	detected := func(name string) format { return format{name: name, id: -1, note: "detected"} }
	tests := []struct {
		name                 string
		data                 []byte
		legacyEncoding       string
		legacyCompression    string
		encoding, compressed format
		json, text, hint     string
		err                  string
	}{
		{
			name: "JSON", data: jsonValue,
			encoding: detected("JSON"), compressed: detected("none"),
			json: `{"name":"v1"}`,
		},
		{
			name: "MessagePack", data: msgpackValue,
			encoding: detected("MessagePack"), compressed: detected("none"),
			json: `{"name":"v1"}`,
		},
		{
			name: "Gzip JSON", data: gzipped,
			encoding: detected("JSON"), compressed: detected("Gzip"),
			json: `{"name":"v1"}`,
		},
		{
			name: "Zstandard MessagePack", data: zstded,
			encoding: detected("MessagePack"), compressed: detected("Zstandard"),
			json: `{"name":"v1"}`,
		},
		{
			name: "plain text", data: []byte("hello world"),
			encoding: detected("plain text"), compressed: detected("none"),
			text: "hello world",
		},
		{
			name: "binary", data: []byte{0x00, 0xff, 0x10},
			encoding: detected("binary"), compressed: detected("none"),
			text: "00000000  00 ff 10                                          |...|\n",
			hint: "values compressed with flate, s2, or snappy can't be detected, set -legacy-compression",
		},
		{
			name: "Snappy JSON", data: snappied, legacyCompression: "snappy",
			encoding:   detected("JSON"),
			compressed: format{name: "Snappy", id: -1, note: "-legacy-compression"},
			json:       `{"name":"v1"}`,
		},
		{
			name: "Protobuf", data: orderValue, legacyEncoding: "protobuf",
			encoding:   format{name: "Protobuf", id: -1, note: "-legacy-encoding"},
			compressed: detected("none"),
			text:       orderText,
		},
		{
			name: "wrong -legacy-compression", data: jsonValue, legacyCompression: "snappy",
			compressed: format{name: "Snappy", id: -1, note: "-legacy-compression"},
			text:       hexDump(jsonValue),
			err:        "decompress with Snappy",
		},
		{
			name: "wrong -legacy-encoding", data: jsonValue, legacyEncoding: "msgpack",
			encoding:   format{name: "MessagePack", id: -1, note: "-legacy-encoding"},
			compressed: detected("none"),
			text:       hexDump(jsonValue),
			err:        "decode MessagePack",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDecoder(t, cmp.Or(tt.legacyEncoding, "auto"), cmp.Or(tt.legacyCompression, "auto"))
			r := d.inspect(tt.data)
			assert.False(t, r.header)
			assert.Equal(t, tt.encoding, r.encoding)
			assert.Equal(t, tt.compressed, r.compression)
			assert.Equal(t, tt.json, string(r.value))
			assert.Equal(t, tt.text, r.text)
			assert.Equal(t, tt.hint, r.hint)
			if tt.err == "" {
				assert.NoError(t, r.err)
			} else {
				assert.ErrorContains(t, r.err, tt.err)
			}
		})
	}
}

func TestInspect_CustomFormats(t *testing.T) {
	d := newTestDecoder(t, "auto", "auto")

	t.Run("serializer", func(t *testing.T) {
		data := encode(t, "value", cache.Serialization(cache.SerializerWithID(200, cache.JsonSerializer{})))
		r := d.inspect(data)
		assert.NoError(t, r.err)
		assert.Equal(t, format{name: "custom", id: 200, note: "not built in"}, r.encoding)
		assert.Equal(t, format{name: "none", id: 0}, r.compression)
		assert.Equal(t, hexDump([]byte(`"value"`)), r.text)
		assert.Equal(t, "the value is shown as hex, since its serializer isn't built in", r.hint)
	})

	t.Run("codec", func(t *testing.T) {
		data := encode(t, "value", cache.Compression(cache.CodecWithID(210, reverseCodec{})))
		r := d.inspect(data)
		assert.NoError(t, r.err)
		assert.Equal(t, format{name: "MessagePack", id: 1}, r.encoding)
		assert.Equal(t, format{name: "custom", id: 210, note: "not built in"}, r.compression)
		assert.Equal(t, -1, r.decodedSize)
		assert.Equal(t, hexDump(data[headerSize:]), r.text)
		assert.Equal(t, "the value is shown compressed, since its codec isn't built in", r.hint)
	})
}

// reverseCodec is a codec that isn't built in.
type reverseCodec struct{}

func (reverseCodec) Compress(data []byte) ([]byte, error)   { return reverse(data), nil }
func (reverseCodec) Decompress(data []byte) ([]byte, error) { return reverse(data), nil }

func reverse(data []byte) []byte {
	out := make([]byte, len(data))
	for i, b := range data {
		out[len(data)-1-i] = b
	}
	return out
}

func TestInspect_Errors(t *testing.T) {
	d := newTestDecoder(t, "auto", "auto")

	t.Run("corrupt value", func(t *testing.T) {
		r := d.inspect([]byte{formatMagic, formatVersion, cache.SerializerMsgPack, cache.CodecNone, 0xC1})
		assert.ErrorContains(t, r.err, "decode MessagePack: msgpack: unknown code c1")
		assert.True(t, r.header)
		assert.Nil(t, r.value)
		assert.Equal(t, hexDump([]byte{0xC1}), r.text)
	})

	t.Run("corrupt compressed value", func(t *testing.T) {
		r := d.inspect([]byte{formatMagic, formatVersion, cache.SerializerMsgPack, cache.CodecGzip, 1, 2, 3})
		assert.ErrorContains(t, r.err, "decompress with Gzip")
		assert.Equal(t, -1, r.decodedSize)
		assert.Equal(t, hexDump([]byte{1, 2, 3}), r.text)
	})

	t.Run("max size", func(t *testing.T) {
		limited, err := newDecoder(16, "auto", "auto")
		assert.NoError(t, err)
		defer limited.Close()

		r := limited.inspect(encode(t, strings.Repeat("a", 100), cache.Compression(s2.NewCodec())))
		assert.ErrorIs(t, r.err, cache.ErrMaxDecodedSizeExceeded)
	})

	t.Run("invalid flags", func(t *testing.T) {
		_, err := newDecoder(1, "yaml", "auto")
		assert.EqualError(t, err, `invalid -legacy-encoding "yaml"`)
		_, err = newDecoder(1, "auto", "brotli")
		assert.EqualError(t, err, `invalid -legacy-compression "brotli"`)
	})
}

func TestCodecFlagNames(t *testing.T) {
	assert.Equal(t, []string{"none", "flate", "gzip", "lz4", "s2", "snappy", "zstd"}, codecFlagNames())
}

func TestHexDump(t *testing.T) {
	assert.Equal(t, "00000000  61 62                                             |ab|\n", hexDump([]byte("ab")))

	dump := hexDump(make([]byte, hexDumpLimit+10))
	assert.True(t, strings.HasSuffix(dump, "... 10 more bytes\n"), dump)
	assert.Equal(t, hexDumpLimit/16+1, strings.Count(dump, "\n"))
}
