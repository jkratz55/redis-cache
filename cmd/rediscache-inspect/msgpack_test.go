package main

import (
	"bytes"
	"math"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

// mustMsgpack returns the values encoded one after another with msgpack.
func mustMsgpack(t *testing.T, write func(enc *msgpack.Encoder) error) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := write(msgpack.NewEncoder(&buf)); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestMsgpackToJSON(t *testing.T) {
	type fields struct {
		Zebra   string
		Apple   int
		Nested  map[string][]any
		Created time.Time
	}
	created := time.Date(2026, 10, 1, 9, 30, 0, 500, time.FixedZone("EDT", -4*3600))

	tests := []struct {
		name  string
		value []byte
		want  string
	}{
		{
			name: "struct fields keep their order",
			value: mustMsgpack(t, func(enc *msgpack.Encoder) error {
				return enc.Encode(fields{Zebra: "z", Apple: 1, Nested: map[string][]any{"list": {true, nil, -3}}, Created: created})
			}),
			want: `{"Zebra":"z","Apple":1,"Nested":{"list":[true,null,-3]},"Created":"2026-10-01T13:30:00.0000005Z"}`,
		},
		{
			name: "keys that aren't strings",
			value: mustMsgpack(t, func(enc *msgpack.Encoder) error {
				return encodeAll(enc.EncodeMapLen(3),
					enc.EncodeInt(1), enc.EncodeString("int"),
					enc.EncodeBytes([]byte("bin")), enc.EncodeString("bytes"),
					enc.EncodeArrayLen(2), enc.EncodeInt(1), enc.EncodeInt(2), enc.EncodeString("array"))
			}),
			want: `{"1":"int","bin":"bytes","[1,2]":"array"}`,
		},
		{
			name: "numbers",
			value: mustMsgpack(t, func(enc *msgpack.Encoder) error {
				return enc.Encode([]any{int8(-1), int64(math.MinInt64), uint64(math.MaxUint64), float32(0.1), 2.5, 1e21})
			}),
			want: `[-1,-9223372036854775808,18446744073709551615,0.1,2.5,1e+21]`,
		},
		{
			name: "NaN and infinity",
			value: mustMsgpack(t, func(enc *msgpack.Encoder) error {
				return enc.Encode([]float64{math.NaN(), math.Inf(-1)})
			}),
			want: `["NaN","-Inf"]`,
		},
		{
			name: "binary",
			value: mustMsgpack(t, func(enc *msgpack.Encoder) error {
				return enc.Encode([][]byte{[]byte("text"), {0x00, 0xff}})
			}),
			want: `["text","AP8="]`,
		},
		{
			name:  "unknown extension",
			value: []byte{0xd4, 0x05, 0x01},
			want:  `"msgpack extension 1AUB"`,
		},
		{
			name:  "duplicate keys and invalid UTF-8",
			value: []byte{0x82, 0xa1, 'a', 0x01, 0xa1, 'a', 0xa1, 0xff},
			want:  "{\"a\":1,\"a\":\"�\"}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := msgpackToJSON(tt.value)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, string(got))
		})
	}
}

func encodeAll(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func TestMsgpackToJSON_Invalid(t *testing.T) {
	for name, value := range map[string][]byte{
		"empty":           {},
		"unknown code":    {0xc1},
		"truncated":       {0x92, 0x01},
		"trailing bytes":  {0x01, 0x02},
		"truncated array": {0xdd, 0xff, 0xff, 0xff, 0xff, 0x01, 0x02, 0x03},
		"truncated map":   {0xdf, 0xff, 0xff, 0xff, 0xff, 0xa1, 'a', 0x01},
		"truncated str":   {0xdb, 0xff, 0xff, 0xff, 0xff, 'a', 'b', 'c'},
		"truncated bin":   {0xc6, 0xff, 0xff, 0xff, 0xff, 'a', 'b', 'c'},
	} {
		t.Run(name, func(t *testing.T) {
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			_, err := msgpackToJSON(value)
			runtime.ReadMemStats(&after)

			assert.Error(t, err)
			// Decoding these into an any with msgpack v5.4.1 allocates up to 64 GiB.
			assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(4<<20))
		})
	}
}
