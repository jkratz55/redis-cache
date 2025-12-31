package cache

import (
	"errors"
	"fmt"
)

type configurable interface {
	setSerializer(Serializer)
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
func Serialization(s Serializer) Option {
	if s == nil {
		panic(errors.New("nil Serializer"))
	}
	return func(c configurable) {
		c.setSerializer(s)
	}
}

// Compression allows for the values to be flated and deflated to conserve bandwidth
// and memory at the cost of higher CPU time. Compression accepts a CompressionCodec to handle
// compressing and decompressing the data to/from Redis.
func Compression(codec CompressionCodec) Option {
	if codec == nil {
		panic(fmt.Errorf("nil CompressionCodec"))
	}
	return func(c configurable) {
		c.setCodec(codec)
	}
}

// BatchMultiGets configures the Cache to use pipelining and split keys up into
// multiple MGET commands for increased throughput and lower latency when dealing
// with MGetMap operations with very large sets of keys.
func BatchMultiGets(batchSize int) Option {
	return func(c configurable) {
		c.setMGetBatch(batchSize)
	}
}
