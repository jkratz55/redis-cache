package main

import (
	"bytes"
	"encoding/json/jsontext"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
)

func TestTextRenderer(t *testing.T) {
	d := newTestDecoder(t, "auto", "auto")
	compressed := encode(t, user, cache.Compression(gzip.NewCodec(6)))
	decompressed := d.inspect(compressed).decodedSize

	var buf bytes.Buffer
	r := newTextRenderer(&buf)
	assert.False(t, r.color, "colors are only used on a terminal")
	for _, ins := range []inspection{
		{key: "user:42", found: true, ttl: 90*time.Second + 400*time.Millisecond, result: d.inspect(compressed)},
		{key: "legacy", found: true, ttl: -1, showHex: true, result: d.inspect([]byte{0x00, 0x01})},
		{key: "corrupt", found: true, ttl: -1, result: d.inspect([]byte{0xC1, 0x01, 1, 0, 0xC1})},
		{key: "missing"},
		{key: "hash", found: true, redisType: "hash"},
		{key: "timeout", err: errors.New("redis: i/o timeout")},
	} {
		assert.NoError(t, r.render(ins))
	}

	assert.Equal(t, `Key          user:42
Encoding     MessagePack (ID 1)
Compression  Gzip (ID 2)
Size         `+formatSize(len(compressed))+` in Redis, `+formatSize(decompressed)+` decompressed
TTL          1m30s
Header       v1

{
  "ID": 42,
  "Name": "Ada",
  "Tags": [
    "a",
    "b"
  ]
}

Key          legacy
Encoding     binary (detected)
Compression  none (detected)
Size         2 B in Redis
TTL          none
Header       none, written before v2 or not by redis-cache
Note         values compressed with flate, s2, or snappy can't be detected, set -legacy-compression

00000000  00 01                                             |..|

Stored in Redis
00000000  00 01                                             |..|

Key          corrupt
Encoding     MessagePack (ID 1)
Compression  none (ID 0)
Size         5 B in Redis
TTL          none
Header       v1
Error        decode MessagePack: msgpack: unknown code c1

00000000  c1                                                |.|

Key          missing
Status       not found

Key          hash
Status       holds a Redis hash, not a value written by redis-cache

Key          timeout
Error        redis: i/o timeout
`, buf.String())
}

func TestTextRenderer_Color(t *testing.T) {
	var buf bytes.Buffer
	r := &textRenderer{w: &buf, color: true, first: true}
	assert.NoError(t, r.render(inspection{key: "missing"}))
	assert.Equal(t, "Key          \x1b[1mmissing\x1b[0m\nStatus       \x1b[31mnot found\x1b[0m\n", buf.String())
}

func TestJSONRenderer(t *testing.T) {
	d := newTestDecoder(t, "auto", "auto")
	data := encode(t, user)

	var buf bytes.Buffer
	r := newJSONRenderer(&buf)
	for _, ins := range []inspection{
		{key: "user:42", found: true, ttl: 1500 * time.Millisecond, result: d.inspect(data)},
		{key: "legacy", found: true, ttl: -1, result: d.inspect([]byte("text"))},
		{key: "corrupt", found: true, ttl: -1, result: d.inspect([]byte{0xC1, 0x01, 1, 0, 0xC1})},
		{key: "missing"},
		{key: "hash", found: true, redisType: "hash"},
		{key: "timeout", err: errors.New("redis: i/o timeout")},
	} {
		assert.NoError(t, r.render(ins))
	}

	lines := strings.Split(strings.TrimSuffix(buf.String(), "\n"), "\n")
	want := []string{
		`{"key":"user:42","found":true,"ttl_ms":1500,"size":` + strconv.Itoa(len(data)) + `,"header":true,` +
			`"encoding":{"name":"MessagePack","id":1},"compression":{"name":"none","id":0},` +
			`"decoded_size":` + strconv.Itoa(len(data)-headerSize) + `,"value":` + userJSON + `}`,
		`{"key":"legacy","found":true,"ttl_ms":-1,"size":4,"header":false,` +
			`"encoding":{"name":"plain text","note":"detected"},"compression":{"name":"none","note":"detected"},` +
			`"decoded_size":4,"text":"text"}`,
		`{"key":"corrupt","found":true,"ttl_ms":-1,"size":5,"header":true,` +
			`"encoding":{"name":"MessagePack","id":1},"compression":{"name":"none","id":0},` +
			`"decoded_size":1,"text":"00000000  c1                                                |.|\n",` +
			`"error":"decode MessagePack: msgpack: unknown code c1"}`,
		`{"key":"missing","found":false}`,
		`{"key":"hash","found":true,"redis_type":"hash"}`,
		`{"key":"timeout","found":false,"error":"redis: i/o timeout"}`,
	}
	assert.Equal(t, want, lines)
	for _, line := range lines {
		assert.True(t, jsontext.Value(line).IsValid(), line)
	}
}

func TestFormatSize(t *testing.T) {
	for n, want := range map[int]string{
		0:               "0 B",
		1023:            "1023 B",
		1024:            "1.0 KiB",
		1536:            "1.5 KiB",
		5 << 20:         "5.0 MiB",
		3 << 30:         "3.0 GiB",
		3 << 40:         "3072.0 GiB",
		1024*1024 - 100: "1023.9 KiB",
	} {
		assert.Equal(t, want, formatSize(n), n)
	}
}

func TestFormatTTL(t *testing.T) {
	assert.Equal(t, "none", formatTTL(-1))
	assert.Equal(t, "250ms", formatTTL(250*time.Millisecond))
	assert.Equal(t, "1h0m5s", formatTTL(time.Hour+5*time.Second+300*time.Millisecond))
}
