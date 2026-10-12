package cache

import (
	"errors"
	"fmt"
)

type configurable interface {
	setSerializer(Serializer)
	setCodec(CompressionCodec)
	addDecodeSerializers(...Serializer)
	addDecodeCodecs(...CompressionCodec)
	setMGetBatch(int)
	setHooks(Hooks)
}

// Option allows for the Cache behavior/configuration to be customized.
type Option func(c configurable)

// Serialization allows for the marshalling and unmarshalling behavior to be
// customized for the Cache.
//
// A valid Serializer must be provided. Providing nil will immediately panic. The
// Serializer must have a format ID (see FormatIdentifier and SerializerWithID),
// otherwise New and NewTyped will panic.
func Serialization(s Serializer) Option {
	if isNil(s) {
		panic(errors.New("nil Serializer"))
	}
	return func(c configurable) {
		c.setSerializer(s)
	}
}

// Compression allows for the values to be flated and deflated to conserve bandwidth
// and memory at the cost of higher CPU time. Compression accepts a CompressionCodec to handle
// compressing and decompressing the data to/from Redis.
//
// A valid CompressionCodec must be provided. Providing nil will immediately panic.
// The CompressionCodec must have a format ID (see FormatIdentifier and CodecWithID),
// otherwise New and NewTyped will panic.
func Compression(codec CompressionCodec) Option {
	if isNil(codec) {
		panic(fmt.Errorf("nil CompressionCodec"))
	}
	return func(c configurable) {
		c.setCodec(codec)
	}
}

// DecodeSerializers registers additional Serializer implementations that are only
// used to read values previously written with them. This allows the configured
// Serializer to be changed while existing entries remain readable.
//
// The configured Serializer and the built-in MessagePackSerializer and
// JsonSerializer are always available for reading and don't need to be registered.
//
// Every Serializer must have a unique format ID, otherwise New and NewTyped will
// panic. Providing nil will immediately panic.
func DecodeSerializers(s ...Serializer) Option {
	for _, serializer := range s {
		if isNil(serializer) {
			panic(errors.New("nil Serializer"))
		}
	}
	return func(c configurable) {
		c.addDecodeSerializers(s...)
	}
}

// DecodeCompression registers additional CompressionCodec implementations that are
// only used to read values previously written with them. This allows the configured
// CompressionCodec to be changed while existing entries remain readable.
//
// The configured CompressionCodec and no compression are always available for
// reading and don't need to be registered.
//
// Every CompressionCodec must have a unique format ID, otherwise New and NewTyped
// will panic. Providing nil will immediately panic.
func DecodeCompression(codecs ...CompressionCodec) Option {
	for _, codec := range codecs {
		if isNil(codec) {
			panic(errors.New("nil CompressionCodec"))
		}
	}
	return func(c configurable) {
		c.addDecodeCodecs(codecs...)
	}
}

// BatchMultiGets configures the Cache to use pipelining and split keys up into
// multiple MGET commands for increased throughput and lower latency when dealing
// with MGetMap operations with very large sets of keys.
//
// Keys are split by count, not by hash slot. On Redis Cluster every MGET command
// must only contain keys of the same hash slot, so batching doesn't make MGet and
// MGetMap work with keys of different slots.
//
// A batchSize of 0 disables batching. BatchMultiGets panics if batchSize is negative.
func BatchMultiGets(batchSize int) Option {
	if batchSize < 0 {
		panic(fmt.Errorf("invalid batch size %d: must not be negative", batchSize))
	}
	return func(c configurable) {
		c.setMGetBatch(batchSize)
	}
}

// Instrumentation configures the Hooks invoked by the Cache to instrument cache
// operations, such as cache hits and misses and the size of cached values. See
// cacheotel.NewHooks for an OpenTelemetry implementation.
//
// If Instrumentation is provided more than once, the last Hooks are used.
func Instrumentation(hooks Hooks) Option {
	return func(c configurable) {
		c.setHooks(hooks)
	}
}

// formatOptions collects the Serializer, CompressionCodec and Hooks configuration
// while options are applied. It is embedded by Cache and TypedCache and only used
// during construction to build the formats registry.
type formatOptions struct {
	serializer         Serializer
	codec              CompressionCodec
	decodeSerializers  []Serializer
	decodeCompressions []CompressionCodec
	hooks              Hooks
}

func defaultFormatOptions() formatOptions {
	return formatOptions{
		serializer: &MessagePackSerializer{},
		codec:      nopCodec{},
	}
}

func (o *formatOptions) setSerializer(s Serializer) {
	o.serializer = s
}

func (o *formatOptions) setCodec(c CompressionCodec) {
	o.codec = c
}

func (o *formatOptions) addDecodeSerializers(s ...Serializer) {
	o.decodeSerializers = append(o.decodeSerializers, s...)
}

func (o *formatOptions) addDecodeCodecs(c ...CompressionCodec) {
	o.decodeCompressions = append(o.decodeCompressions, c...)
}

func (o *formatOptions) setHooks(h Hooks) {
	o.hooks = h
}

// build creates the formats registry from the collected configuration. It panics
// if the configuration is invalid.
func (o *formatOptions) build() *formats {
	f := newFormats(o.serializer, o.codec, o.decodeSerializers, o.decodeCompressions)
	f.hooks = o.hooks
	return f
}
