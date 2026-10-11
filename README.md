# Redis Cache

Redis Cache is a thin abstraction layer on top of the go-redis client. The goal of this library is to improve developer experience and ergonmics when storing and retreiving custom types/data between GO and Redis. I created this library because I found myself consistently writing the same boilerplate code for handling encoding and compression between my types in GO and Redis. This library does make a few assumptions about how you want to store your data in Redis. It assumes you have structures defined in GO (or built in structures/types like strings, maps, slices, etc) and that you want to store them in Redis as bytes (STRING type in Redis). The data can be encoded however you want, you just need to implement the `Serializer` interface. By default, Redis Cache uses msgpack to marshal/unmarshal data, but also provides implementions of the `Serializer` interface for json and protobuf. Compression is also supported by doesn't compress by default. You can use whatever compression you want, you just need to implement the `CompressionCodec` interface. Out of the box, there are implementations for gzip, flate, lz4, s2, snappy, and zstd.

Redis Cache provides two cores types, `Cache` and `TypedCache`. They are functionally identical, but have different APIs. The `Cache` type is more generic and flexible, allowing a single instance to be used with multiple types. It's API is similar to built in Marshal/Unmarshal functions in the GO ecosystem. It expects a pointer to a struct to be passed in, and will marshal/unmarshal the struct into bytes. Meanwhile, the `TypedCache` type is more specific and only works with a single type. However, it simplifes the API, and is type-safe thanks to generics. Unlike the `Cache` type, the `TypedCache` type allows us to support MGet as a method, rather than a standalone function. `TypedCache` also works much better when using Protobuf for encoding/serialization. Which one to choose largely depends on your use case and preferences.

Instrumentation is key to any production ready application. Redis Cache provides OpenTelemetry tracing and metrics out of the box which the `cacheotel` pacakge. The `cacheotel` package provides wrapped versions of the `Cache` and `TypedCache` types that automatically instrument the calls with OpenTelemetry. When used in combination with tracing/metrics on the Redis client itself, this can provide a very rich observability story. Specifically, you can see how long the client takes to get a response from Redis, and how long it takes to handle encoding/compression.

## Features

* Save/Load any data structure that can be represented as bytes/string
* Flexible serialization options 
* Flexible compression options
* Change serialization/compression without losing access to previously cached values
* Tracing/Metrics via OpenTelemetry

## Stored Value Format

Every value written by `Cache` and `TypedCache` is prefixed with a fixed 4-byte header that records how the value was encoded:

```
+-------+---------+---------------+----------+-------------------------+
| 0xC1  | version | serializer ID | codec ID | serialized + compressed |
| magic |  0x01   |               |          |         payload         |
+-------+---------+---------------+----------+-------------------------+
```

When a value is read, the serializer and compression codec are selected by the IDs in the header rather than the instance's configuration. This means you can change `Serialization(...)` and/or `Compression(...)` without flushing Redis: entries written with the previous configuration remain readable until they expire. Reading the header costs a couple of byte comparisons and array lookups and doesn't allocate.

The configured serializer and codec, the built-in MessagePack and JSON serializers, and no compression are always readable. Any other format you need to keep reading must be registered for decoding only:

```go
// Previously: cache.Serialization(&cache.JsonSerializer{}), cache.Compression(gzip.NewCodec(6))
c := cache.New(client,
    cache.Serialization(&cache.MessagePackSerializer{}),
    cache.Compression(zstdCodec),
    cache.DecodeCompression(gzip.NewCodec(6)), // keep reading gzip values
)
```

If a value was written with a serializer or codec that isn't registered, reads return an error wrapping `cache.ErrUnsupportedFormat`. Values without a header (written by v1.x and v2.0.0 release candidates) are decoded with the configured serializer and codec.

> **Rolling deployments:** older versions of the library can't read values with the header. Upgrade every service reading from a shared Redis before, or together with, the services writing to it.

### Format IDs

IDs 0-127 are reserved for built-in implementations, IDs 128-255 are available for custom implementations.

| Serializer | ID | Codec | ID |
|---|---|---|---|
| `MessagePackSerializer` | 1 (`SerializerMsgPack`) | none (default) | 0 (`CodecNone`) |
| `JsonSerializer` | 2 (`SerializerJSON`) | `compression/flate` | 1 (`CodecFlate`) |
| `cacheproto.ProtobufSerializer` | 3 (`SerializerProtobuf`) | `compression/gzip` | 2 (`CodecGzip`) |
| | | `compression/lz4` | 3 (`CodecLZ4`) |
| | | `compression/s2` | 4 (`CodecS2`) |
| | | `compression/snappy` | 5 (`CodecSnappy`) |
| | | `compression/zstd` | 6 (`CodecZstd`) |

## Bring your own Serializer/Codec

Custom `Serializer` and `CompressionCodec` implementations are fully supported but must have a format ID in the range 128-255. `New` and `NewTyped` panic if a serializer or codec doesn't have a valid ID, or if two different implementations claim the same ID.

If you own the type, implement `cache.FormatIdentifier`:

```go
type CBORSerializer struct{}

func (CBORSerializer) Marshal(v any) ([]byte, error)  { /* ... */ }
func (CBORSerializer) Unmarshal(b []byte, v any) error { /* ... */ }
func (CBORSerializer) FormatID() uint8                 { return 200 }
```

If you can't modify the type (e.g. it's from a third-party library), assign an ID with an adapter:

```go
c := cache.NewTyped[Person](client,
    cache.Serialization(cache.SerializerWithID(201, thirdparty.NewSerializer())),
    cache.Compression(cache.CodecWithID(210, brotliCodec)),
)
```

To move away from a custom format while keeping existing entries readable, register it with `DecodeSerializers` / `DecodeCompression`:

```go
c := cache.NewTyped[Person](client,
    cache.Compression(s2.NewCodec()),
    cache.DecodeSerializers(CBORSerializer{}),
    cache.DecodeCompression(cache.CodecWithID(210, brotliCodec)),
)
```

Rules for custom implementations:

* **An ID is a permanent contract.** The ID is stored in Redis alongside every value. Once values have been written with an ID, it must never be used for a different format by any service sharing the same Redis.
* **Layered codecs need their own ID.** A codec that changes the bytes of another codec (e.g. encryption on top of gzip) produces a different format and must declare its own ID. IDs are only discovered through `FormatID()`; `Unwrap()` is never followed, so a wrapper can't accidentally inherit the ID of what it wraps.
* **Pass-through wrappers forward the inner ID.** Wrappers that don't alter the bytes, like `cacheotel.InstrumentedSerializer` and `cacheotel.InstrumentedCompressionCodec`, implement `FormatID()` by returning the ID of the wrapped component (use `cache.FormatIDOf`).
* Implementations must be safe for concurrent use.

See `examples/customserializer` for a runnable example.

## Contributing

Please see [CONTRIBUTING.md](CONTRIBUTING.md) for details.

## Requirements

* Go 1.25+ 
* Redis 7+

## Getting Redis Cache

```shell
go get github.com/jkratz55/redis-cache/v2
```

## Examples

There are several examples in the `examples` directory. Please refer to those to get started.