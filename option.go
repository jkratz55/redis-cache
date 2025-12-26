package cache

import (
	"encoding/json"
	"fmt"
)

type configurable interface {
	setMarshaller(Marshaller)
	setUnmarshaller(Unmarshaller)
	setCodec(CompressionCodec)
	setMGetBatch(int)
}

// Option allows for the Cache behavior/configuration to be customized.
type Option func(c configurable)

// Serialization allows for the marshalling and unmarshalling behavior to be
// customized for the Cache.
//
// A valid Marshaller and Unmarshaller must be provided. Providing nil for either
// will immediately panic.
func Serialization(mar Marshaller, unmar Unmarshaller) Option {
	if mar == nil || unmar == nil {
		panic(fmt.Errorf("nil Marshaller and/or Unmarshaller not permitted, illegal use of api"))
	}
	return func(c configurable) {
		c.setMarshaller(mar)
		c.setUnmarshaller(unmar)
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
// and memory at the cost of higher CPU time. Compression accepts a CompressionCodec to handle
// compressing and decompressing the data to/from Redis.
func Compression(codec CompressionCodec) Option {
	if codec == nil {
		panic(fmt.Errorf("nil CompressionCodec not permitted, illegal use of API"))
	}
	return func(c configurable) {
		c.setCodec(codec)
	}
}

// BatchMultiGets configures the Cache to use pipelining and split keys up into
// multiple MGET commands for increased throughput and lower latency when dealing
// with MGet operations with very large sets of keys.
func BatchMultiGets(batchSize int) Option {
	return func(c configurable) {
		c.setMGetBatch(batchSize)
	}
}
