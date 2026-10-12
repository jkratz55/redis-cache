# Redis Cache

Redis Cache is a thin abstraction layer on top of the go-redis client. The goal of this library is to improve developer experience and ergonomics when storing and retrieving custom types/data between Go and Redis. I created this library because I found myself consistently writing the same boilerplate code for handling encoding and compression between my types in Go and Redis. This library does make a few assumptions about how you want to store your data in Redis. It assumes you have structures defined in Go (or built in structures/types like strings, maps, slices, etc) and that you want to store them in Redis as bytes (STRING type in Redis). The data can be encoded however you want, you just need to implement the `Serializer` interface. By default, Redis Cache uses msgpack to marshal/unmarshal data, but also provides implementations of the `Serializer` interface for json and protobuf. Compression is also supported but doesn't compress by default. You can use whatever compression you want, you just need to implement the `CompressionCodec` interface. Out of the box, there are implementations for gzip, flate, lz4, s2, snappy, and zstd.

Redis Cache provides two core types, `Cache` and `TypedCache`. They are functionally identical, but have different APIs. The `Cache` type is more generic and flexible, allowing a single instance to be used with multiple types. Its API is similar to built in Marshal/Unmarshal functions in the Go ecosystem. It expects a pointer to a struct to be passed in, and will marshal/unmarshal the struct into bytes. Meanwhile, the `TypedCache` type is more specific and only works with a single type. However, it simplifies the API, and is type-safe thanks to generics. Unlike the `Cache` type, the `TypedCache` type allows us to support MGet as a method, rather than a standalone function. `TypedCache` also works much better when using Protobuf for encoding/serialization. Which one to choose largely depends on your use case and preferences.

Instrumentation is key to any production ready application. The `cacheotel` package provides OpenTelemetry instrumentation for `Cache` and `TypedCache`: wrappers for the `Serializer` and `CompressionCodec` that record how long encoding and compression take, and hooks that record cache hits and misses, the size of cached values, and decode errors. When used in combination with tracing/metrics on the Redis client itself, such as `redisotel`, this can provide a very rich observability story. Specifically, you can see how long the client takes to get a response from Redis, and how long it takes to handle encoding/compression. See [Observability](#observability).

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
    cache.Compression(cache.CodecWithID(210, thirdparty.NewCodec())),
)
```

To move away from a custom format while keeping existing entries readable, register it with `DecodeSerializers` / `DecodeCompression`:

```go
c := cache.NewTyped[Person](client,
    cache.Compression(s2.NewCodec()),
    cache.DecodeSerializers(CBORSerializer{}),
    cache.DecodeCompression(cache.CodecWithID(210, thirdparty.NewCodec())),
)
```

Rules for custom implementations:

* **An ID is a permanent contract.** The ID is stored in Redis alongside every value. Once values have been written with an ID, it must never be used for a different format by any service sharing the same Redis.
* **Layered codecs need their own ID.** A codec that changes the bytes of another codec (e.g. encryption on top of gzip) produces a different format and must declare its own ID. IDs are only discovered through `FormatID()`; `Unwrap()` is never followed, so a wrapper can't accidentally inherit the ID of what it wraps.
* **Pass-through wrappers forward the inner ID.** Wrappers that don't alter the bytes, like `cacheotel.InstrumentedSerializer` and `cacheotel.InstrumentedCompressionCodec`, implement `FormatID()` by returning the ID of the wrapped component (use `cache.FormatIDOf`) and `Unwrap()` by returning the wrapped component. The wrapped component then identifies the format, so a wrapper can forward the ID of a built-in implementation.
* **Tolerant serializers should implement `StrictUnmarshaler`.** Values written by older versions of the library have no header and can start with the same bytes as the header, so a value that fails to decode is also decoded as a value without a header. If `Unmarshal` accepts input it didn't produce, such as unknown fields, implement `UnmarshalStrict` so a corrupt value isn't mistaken for an old value.
* Implementations must be safe for concurrent use.

See `examples/customserializer` for a runnable example.

## JSON Serialization

`JsonSerializer` uses `encoding/json/v2` with its default options, which differ from `encoding/json`, the package used by v1.x and the v2.0.0 release candidates. For example, `time.Duration` can't be marshaled or unmarshaled without a format option, object names are matched case-sensitively, and nil slices and maps are marshaled as `[]` and `{}`. JSON values written by older versions may not decode, or may decode differently.

To keep the behavior of `encoding/json`, bring your own serializer with a custom format ID:

```go
import "encoding/json"

type JSONv1Serializer struct{}

func (JSONv1Serializer) Marshal(v any) ([]byte, error)    { return json.Marshal(v) }
func (JSONv1Serializer) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (JSONv1Serializer) FormatID() uint8                 { return 200 }

c := cache.New(client, cache.Serialization(JSONv1Serializer{}))
```

Values without a header, written by older versions, are decoded with the configured serializer, so they're read with `encoding/json` as before.

## Decompression Limits

A compressed value is decompressed by every service reading it. If the Redis is shared, anyone with write access, or plain data corruption, can store a value that decompresses to gigabytes. For example, Snappy and S2 allocate the decompressed size declared by the value up front, so a corrupt value of a few bytes can allocate 4 GiB.

There is no limit by default. Every built-in codec accepts a `WithMaxDecodedSize` option, and values exceeding the limit fail to decode with an error wrapping `cache.ErrMaxDecodedSizeExceeded`:

```go
c := cache.New(client,
    cache.Compression(s2.NewCodec(s2.WithMaxDecodedSize(16 << 20))),
    cache.DecodeCompression(gzip.NewCodec(6, gzip.WithMaxDecodedSize(16 << 20))),
)
```

Set a limit comfortably above your largest value whenever the Redis is shared with other services.

## Reading Multiple Keys

`MGet` and `MGetMap` skip keys that don't exist. If some values can't be decoded, for example because they are corrupt or were written in a format that isn't registered, those keys are skipped as well and the values of the other keys are returned along with a `*cache.MultiKeyError` holding the error of each key that failed:

```go
values, err := cache.MGetMap[Person](ctx, c, keys...)
if mkErr, ok := errors.AsType[*cache.MultiKeyError](err); ok {
    for key, keyErr := range mkErr.Errors {
        // values holds every other key, handle or reload the failed ones.
    }
} else if err != nil {
    return err // Redis failed, values is nil.
}
```

## Redis Cluster

Redis Cluster doesn't support commands on keys of different hash slots, and the library doesn't split them for you:

* `MGet`, `MGetMap`, `MSet`, and `Delete` with several keys send a single command, which fails with a `CROSSSLOT` error unless every key is in the same hash slot. Use hash tags, such as `{user:1}:profile` and `{user:1}:friends`, to store related keys in the same slot. `BatchMultiGets` splits keys by count, not by slot, so it doesn't lift this restriction.
* `Flush`, `FlushAsync`, `FlushDB`, and `FlushDBAsync` send `FLUSHDB` to a single master, so they only delete the keys of that node. Use `ForEachMaster` on the `*redis.ClusterClient` to flush every node.
* `Keys` and `Scan` scan every master of a `*redis.ClusterClient`, including one wrapped by a `FailoverClient`.

## Read-Through and Write-Through

`ReadThrough` and `ReadThroughSingleFlight` load a value from the source of truth on a cache miss and cache it. `ReadThroughSingleFlight` shares a load between concurrent callers of the same `Cache` loading the same key and type.

`WriteThrough` updates the source of truth and then the cache. Keeping both consistent is best effort: if the cache update fails, `WriteThrough` deletes the key so the previous value isn't served anymore, but the delete can fail too, for example while Redis is unavailable. Concurrent writes of the same key can also update the source of truth and the cache in a different order. Set a TTL so a stale value eventually expires.

## Failover

`FailoverClient` wraps a primary and a secondary `RedisClient`, health checks both, and sends every command to the primary until it fails its health checks, then to the secondary until the primary recovers.

> **The primary and secondary must replicate each other (active-active replication),** for example Redis Enterprise Active-Active databases. `FailoverClient` only sends a command to the active client, it doesn't copy writes, deletes, or expirations to the other one. Without replication, writes made while failed over never reach the primary, so after recovering the cache serves the values the primary held before the outage, which can be stale and may never expire.

The default health check sends `PING`, which a Redis that rejects writes, such as one out of memory or a read-only replica, still passes. Use `FailoverOpts.HealthCheck` to check writes instead:

```go
fc := cache.NewFailoverClient(primary, secondary, cache.FailoverOptions().
    SetHealthCheck(func(ctx context.Context, client cache.RedisClient) error {
        return client.Set(ctx, "health:canary", "ok", time.Minute).Err()
    }))
```

`FailoverClient` takes ownership of both clients: `Close` closes them.

## Observability

The `cacheotel` package records OpenTelemetry metrics:

```go
name := cacheotel.WithAttributes(attribute.String("cache.name", "users"))
c := cache.New(client,
    cache.Serialization(cacheotel.NewInstrumentedSerializer(&cache.MessagePackSerializer{}, name)),
    cache.Compression(cacheotel.NewInstrumentedCompressionCodec(s2.NewCodec(), name)),
    cache.Instrumentation(cacheotel.NewHooks(name)),
)
```

| Metric | Recorded by | Description |
|---|---|---|
| `cache.serialization.duration`, `cache.serialization.errors` | `NewInstrumentedSerializer` | Duration and errors of marshal and unmarshal operations, by `cache.serializer` |
| `cache.compression.duration`, `cache.compression.errors` | `NewInstrumentedCompressionCodec` | Duration and errors of compress and decompress operations, by `cache.codec` |
| `cache.lookups` | `NewHooks` | Keys read from Redis, by `cache.hit` |
| `cache.value.size`, `cache.value.serialized_size` | `NewHooks` | Size of values as stored in Redis, and after serialization before compression |
| `cache.decode.errors` | `NewHooks` | Values that couldn't be decoded, by `reason`: `unsupported_format`, `size_limit`, or `invalid` |
| `cache.singleflight.calls` | `NewHooks` | `ReadThroughSingleFlight` calls after a miss, by `shared` |
| `cache.failover.active` | `ObserveFailoverClient` | Which client of a `FailoverClient` is active |

`cache.Instrumentation` accepts any `cache.Hooks`, so other instrumentation, such as logging, can be plugged in as well. Instrument the Redis client itself, for example with `redisotel`, for command latency and tracing. See `examples/otel`.

## Contributing

Please see [CONTRIBUTING.adoc](CONTRIBUTING.adoc) for details.

## Requirements

* Go 1.27+
* Redis 7+

## Getting Redis Cache

```shell
go get github.com/jkratz55/redis-cache/v2
```

## Examples

There are several examples in the `examples` directory. Please refer to those to get started.