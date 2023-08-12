package cache

import (
	"encoding/json"
	"fmt"

	"github.com/jkratz55/redis-cache/compression/brotli"
	"github.com/jkratz55/redis-cache/compression/flate"
	"github.com/jkratz55/redis-cache/compression/gzip"
	"github.com/jkratz55/redis-cache/compression/lz4"
)

// Option allows for the Cache behavior/configuration to be customized.
type Option func(c *Cache)

// Serialization allows for the marshalling and unmarshalling behavior to be
// customized for the Cache.
//
// A valid Marshaller and Unmarshaller must be provided. Providing nil for either
// will immediately panic.
func Serialization(mar Marshaller, unmar Unmarshaller) Option {
	if mar == nil || unmar == nil {
		panic(fmt.Errorf("nil Marshaller and/or Unmarshaller not permitted, illegal use of api"))
	}
	return func(c *Cache) {
		c.marshaller = mar
		c.unmarshaller = unmar
	}
}

// JSON is a convenient Option for configuring Cache to use JSON for serializing
// data stored in the cache.
//
// JSON is the equivalent of using Serialization passing it a Marshaller and
// Unmarshaller using json.
func JSON() Option {
	mar := func(v any) ([]byte, error) {
		return json.Marshal(v)
	}
	unmar := func(data []byte, v any) error {
		return json.Unmarshal(data, v)
	}
	return Serialization(mar, unmar)
}

// Compression allows for the values to be flated and deflated to conserve bandwidth
// and memory at the cost of higher CPU time. Compression accepts a Codec to handle
// compressing and decompressing the data to/from Redis.
func Compression(codec Codec) Option {
	if codec == nil {
		panic(fmt.Errorf("nil Codec not permitted, illegal use of API"))
	}
	return func(c *Cache) {
		c.codec = codec
	}
}

// Flate configures the Cache to use Flate Codec for compressing and decompressing
// values stored in Redis. Flate uses a default configuration favoring compression
// over speed.
func Flate() Option {
	codec := &flate.Codec{Level: 9} // Best Compression
	return Compression(codec)
}

// GZip configures the Cache to use gzip for compressing and decompressing values
// stored in Redis. GZip uses a default configuration favoring compression size
// over speed
func GZip() Option {
	codec := gzip.NewCodec(9) // Best Compression
	return Compression(codec)
}

// LZ4 configures the Cache to use lz4 for compressing and decompressing values
// stored in Redis.
func LZ4() Option {
	codec := lz4.NewCodec()
	return Compression(codec)
}

// Brotli configures the Cache to use Brotli for compressing and decompressing
// values stored in Redis. The default Brotli configuration uses a balanced
// approach between speed and compression level.
func Brotli() Option {
	// While the other options favor the best compression with Brotli the default
	// we use if balanced because benchmarks showed Brotli best compression was
	// very slow in comparison to Gzip.
	codec := brotli.NewCodec(6)
	return Compression(codec)
}
