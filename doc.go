// Package cache implements a cache backed by Redis and the official Redis Go client.
//
// Redis Cache provides an implementation of a Cache backed by Redis that handles
// serialization, compression, and metrics with little to no effort from the user.
//
// # Stored Value Format
//
// Every value written by Cache and TypedCache is prefixed with a fixed 4-byte
// header: a magic byte (0xC1), a format version (0x01), the format ID of the
// Serializer, and the format ID of the CompressionCodec used to write the value.
// On read, the Serializer and CompressionCodec are selected by the IDs in the
// header, so the configured Serialization and Compression can be changed while
// previously cached values remain readable.
//
// The configured Serializer and CompressionCodec, MessagePackSerializer,
// JsonSerializer, and no compression are always available for reading. Additional
// formats can be registered for reading only with DecodeSerializers and
// DecodeCompression. Values written with an unregistered format return an error
// wrapping ErrUnsupportedFormat. Values without a header, written by older
// versions of the library, are decoded with the configured Serializer and
// CompressionCodec.
//
// # Format IDs
//
// IDs 0-127 are reserved for the built-in implementations (see SerializerMsgPack,
// SerializerJSON, SerializerProtobuf, CodecNone, CodecFlate, CodecGzip, CodecLZ4,
// CodecS2, CodecSnappy, and CodecZstd). IDs 128-255 are available for custom
// implementations.
//
// Custom Serializer and CompressionCodec implementations must have a format ID,
// either by implementing FormatIdentifier or by being wrapped with SerializerWithID
// or CodecWithID, otherwise New and NewTyped panic. An ID is a permanent contract:
// once values have been written with it, it must never be reused for a different
// format by any service sharing the same Redis data. Codecs layered on top of
// another codec (e.g. encryption over gzip) must declare their own ID. IDs are only
// discovered through FormatID, Unwrap is never followed.
//
// # Decompression Limits
//
// Values read from a Redis shared with other services can be corrupt or crafted to
// decompress to a very large size. The built-in CompressionCodecs have no limit by
// default, but accept a WithMaxDecodedSize option, and fail with an error wrapping
// ErrMaxDecodedSizeExceeded for values exceeding it.
//
// # Redis Cluster
//
// Commands on several keys, used by MGet, MGetMap, MSet and Delete, fail with a
// CROSSSLOT error unless every key is in the same hash slot. BatchMultiGets doesn't
// group keys by slot. Flush and FlushDB only flush a single node of a cluster, while
// Keys and Scan scan every master.
//
// # Instrumentation
//
// Hooks configured with the Instrumentation option are invoked on lookups, when
// values are encoded and decoded, and by ReadThroughSingleFlight. The cacheotel
// package records them as OpenTelemetry metrics.
package cache
