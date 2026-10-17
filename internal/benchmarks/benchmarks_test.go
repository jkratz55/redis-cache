package benchmarks

import (
	stdflate "compress/flate"
	stdgzip "compress/gzip"
	"context"
	"fmt"
	"testing"
	"time"

	kzstd "github.com/klauspost/compress/zstd"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"

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

// serializerCase is a Serializer with the value it marshals for an Order and the
// destination it unmarshals into.
type serializerCase struct {
	name       string
	serializer cache.Serializer
	value      func(Order) any
	newDst     func() any
}

func serializers() []serializerCase {
	return []serializerCase{
		{
			name:       "msgpack",
			serializer: cache.MessagePackSerializer{},
			value:      func(o Order) any { return o },
			newDst:     func() any { return new(Order) },
		},
		{
			name:       "json",
			serializer: cache.JsonSerializer{},
			value:      func(o Order) any { return o },
			newDst:     func() any { return new(Order) },
		},
		{
			name:       "protobuf",
			serializer: cacheproto.ProtobufSerializer{},
			value:      func(o Order) any { return o.Proto() },
			newDst:     func() any { return new(benchpb.Order) },
		},
	}
}

// codecCase is a named CompressionCodec. Codecs with a compression level are
// benchmarked at their fastest, default and best levels.
type codecCase struct {
	name  string
	codec cache.CompressionCodec
}

func codecs(tb testing.TB) []codecCase {
	newZstd := func(level kzstd.EncoderLevel) cache.CompressionCodec {
		codec, err := zstd.NewCodec(level)
		if err != nil {
			tb.Fatal(err)
		}
		tb.Cleanup(func() { _ = codec.Close() })
		return codec
	}
	return []codecCase{
		{"flate-speed", flate.NewCodec(stdflate.BestSpeed)},
		{"flate-default", flate.NewCodec(stdflate.DefaultCompression)},
		{"flate-best", flate.NewCodec(stdflate.BestCompression)},
		{"gzip-speed", gzip.NewCodec(stdgzip.BestSpeed)},
		{"gzip-default", gzip.NewCodec(stdgzip.DefaultCompression)},
		{"gzip-best", gzip.NewCodec(stdgzip.BestCompression)},
		{"lz4", lz4.NewCodec()},
		{"s2", s2.NewCodec()},
		{"snappy", snappy.NewCodec()},
		{"zstd-fastest", newZstd(kzstd.SpeedFastest)},
		{"zstd-default", newZstd(kzstd.SpeedDefault)},
		{"zstd-better", newZstd(kzstd.SpeedBetterCompression)},
		{"zstd-best", newZstd(kzstd.SpeedBestCompression)},
	}
}

// codecInput is data compressed by the codec benchmarks.
type codecInput struct {
	name string
	data []byte
}

// codecInputs returns the payloads encoded with MessagePackSerializer, the default
// serializer, and 4 KiB of incompressible data.
func codecInputs(tb testing.TB) []codecInput {
	var inputs []codecInput
	for _, p := range Payloads() {
		data, err := cache.MessagePackSerializer{}.Marshal(p.Order)
		if err != nil {
			tb.Fatal(err)
		}
		inputs = append(inputs, codecInput{name: p.Name, data: data})
	}
	return append(inputs, codecInput{name: "random", data: Incompressible(4, 4096)})
}

// BenchmarkSerializer measures marshaling and unmarshaling every payload with every
// serializer. The encoded-B metric is the size of the encoded payload.
func BenchmarkSerializer(b *testing.B) {
	for _, s := range serializers() {
		for _, p := range Payloads() {
			value := s.value(p.Order)
			data, err := s.serializer.Marshal(value)
			if err != nil {
				b.Fatal(err)
			}

			b.Run(fmt.Sprintf("format=%s/payload=%s/op=marshal", s.name, p.Name), func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := s.serializer.Marshal(value); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(data)), "encoded-B")
			})

			b.Run(fmt.Sprintf("format=%s/payload=%s/op=unmarshal", s.name, p.Name), func(b *testing.B) {
				b.SetBytes(int64(len(data)))
				b.ReportAllocs()
				for b.Loop() {
					// Every read decodes into a new value, like TypedCache.Get.
					if err := s.serializer.Unmarshal(data, s.newDst()); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(len(data)), "encoded-B")
			})
		}
	}
}

// BenchmarkCodec measures compressing and decompressing every payload with every
// codec. Throughput is relative to the uncompressed size, and the ratio metric is
// the uncompressed size divided by the compressed size.
func BenchmarkCodec(b *testing.B) {
	inputs := codecInputs(b)
	for _, c := range codecs(b) {
		for _, in := range inputs {
			compressed, err := c.codec.Compress(in.data)
			if err != nil {
				b.Fatal(err)
			}
			ratio := float64(len(in.data)) / float64(len(compressed))

			b.Run(fmt.Sprintf("codec=%s/payload=%s/op=compress", c.name, in.name), func(b *testing.B) {
				b.SetBytes(int64(len(in.data)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := c.codec.Compress(in.data); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(ratio, "ratio")
			})

			b.Run(fmt.Sprintf("codec=%s/payload=%s/op=decompress", c.name, in.name), func(b *testing.B) {
				b.SetBytes(int64(len(in.data)))
				b.ReportAllocs()
				for b.Loop() {
					if _, err := c.codec.Decompress(compressed); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(ratio, "ratio")
			})
		}
	}
}

// BenchmarkTypedCache measures TypedCache.Set and TypedCache.Get, including the
// format header and the go-redis commands, with a client storing values in memory,
// so the cost of the library isn't hidden by the network. The stored-B metric is
// the size of the value stored in Redis.
func BenchmarkTypedCache(b *testing.B) {
	all := make(map[string]cache.CompressionCodec)
	for _, c := range codecs(b) {
		all[c.name] = c.codec
	}
	cacheCodecs := []string{"none", "lz4", "s2", "zstd-default", "gzip-default"}

	for _, p := range Payloads() {
		if p.Name == "small" {
			continue
		}
		for _, s := range serializers() {
			for _, codecName := range cacheCodecs {
				var opts []cache.Option
				if codecName != "none" {
					opts = append(opts, cache.Compression(all[codecName]))
				}
				opts = append(opts, cache.Serialization(s.serializer))
				name := fmt.Sprintf("format=%s/codec=%s/payload=%s", s.name, codecName, p.Name)
				if s.name == "protobuf" {
					benchmarkTypedCache(b, name, p.Order.Proto(), opts...)
				} else {
					benchmarkTypedCache(b, name, p.Order, opts...)
				}
			}
		}
	}
}

func benchmarkTypedCache[T any](b *testing.B, name string, value T, opts ...cache.Option) {
	ctx := context.Background()
	client := newMemoryClient()
	c := cache.NewTyped[T](client, opts...)
	if err := c.Set(ctx, "key", value, 0); err != nil {
		b.Fatal(err)
	}
	stored := float64(len(client.values["key"]))

	b.Run(name+"/op=set", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := c.Set(ctx, "key", value, time.Hour); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(stored, "stored-B")
	})

	b.Run(name+"/op=get", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := c.Get(ctx, "key"); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(stored, "stored-B")
	})
}

// memoryClient is a cache.RedisClient keeping values in memory. Only Get and Set
// are implemented. It isn't safe for concurrent use.
type memoryClient struct {
	cache.RedisClient
	values map[string][]byte
}

func newMemoryClient() *memoryClient {
	return &memoryClient{values: make(map[string][]byte)}
}

func (m *memoryClient) Get(ctx context.Context, key string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx, "get", key)
	val, ok := m.values[key]
	if !ok {
		cmd.SetErr(redis.Nil)
		return cmd
	}
	// go-redis reads replies into a string as well.
	cmd.SetVal(string(val))
	return cmd
}

func (m *memoryClient) Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd {
	cmd := redis.NewStatusCmd(ctx, "set", key, val)
	m.values[key] = val.([]byte)
	cmd.SetVal("OK")
	return cmd
}

// TestPayloads verifies every serializer encodes the same data and every codec
// decompresses what it compressed, so the benchmarks compare equivalent work, and
// that the payloads have the sizes documented in the README.
func TestPayloads(t *testing.T) {
	sizes := map[string][2]int{
		"small":  {300, 400},
		"medium": {1900, 2200},
		"large":  {62_000, 68_000},
	}
	for _, p := range Payloads() {
		for _, s := range serializers() {
			data, err := s.serializer.Marshal(s.value(p.Order))
			assert.NoError(t, err)
			dst := s.newDst()
			assert.NoError(t, s.serializer.Unmarshal(data, dst))
			switch dst := dst.(type) {
			case *Order:
				assert.Equal(t, p.Order, *dst, "%s %s", s.name, p.Name)
			case *benchpb.Order:
				assert.True(t, proto.Equal(p.Order.Proto(), dst), "%s %s", s.name, p.Name)
			}
			if s.name == "msgpack" {
				assert.GreaterOrEqual(t, len(data), sizes[p.Name][0], p.Name)
				assert.LessOrEqual(t, len(data), sizes[p.Name][1], p.Name)
			}
		}
	}

	for _, c := range codecs(t) {
		for _, in := range codecInputs(t) {
			compressed, err := c.codec.Compress(in.data)
			assert.NoError(t, err)
			out, err := c.codec.Decompress(compressed)
			assert.NoError(t, err)
			assert.Equal(t, in.data, out, "%s %s", c.name, in.name)
		}
	}
}
