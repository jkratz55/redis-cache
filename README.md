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
* Benchmarked serializers and codecs, with guidance on choosing them, see [Performance](#performance)

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

| Serializer                      | ID                       | Codec                | ID                |
|---------------------------------|--------------------------|----------------------|-------------------|
| `MessagePackSerializer`         | 1 (`SerializerMsgPack`)  | none (default)       | 0 (`CodecNone`)   |
| `JsonSerializer`                | 2 (`SerializerJSON`)     | `compression/flate`  | 1 (`CodecFlate`)  |
| `cacheproto.ProtobufSerializer` | 3 (`SerializerProtobuf`) | `compression/gzip`   | 2 (`CodecGzip`)   |
|                                 |                          | `compression/lz4`    | 3 (`CodecLZ4`)    |
|                                 |                          | `compression/s2`     | 4 (`CodecS2`)     |
|                                 |                          | `compression/snappy` | 5 (`CodecSnappy`) |
|                                 |                          | `compression/zstd`   | 6 (`CodecZstd`)   |

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

`MGet` reads several keys with a single `MGET` command. Like the reply to `MGET`, the `MultiResult` it returns has an entry for every key, in the order the keys were requested, including keys that don't exist:

```go
res, err := users.MGet(ctx, keys...) // or cache.MGet[User](ctx, c, keys...)

for i, key := range keys {
    if user, ok := res.At(i); ok {
        // user is the value of key.
    }
}

found := res.Values() // the values that were found, in the order of keys
byKey := res.Map()    // the values that were found, by key
for key, user := range res.All() {
    // every key that was found and its value, in the order of keys
}
```

`Values` doesn't copy the values, while `Map` builds a new map on every call, so prefer `At` or `All` when you go through the keys anyway.

`Missing` returns the keys that don't exist, for example to load them from the source of truth and cache them:

```go
if missing := res.Missing(); len(missing) > 0 {
    loaded, err := db.LoadUsers(ctx, missing) // map[string]User
    // ...
    for key, user := range loaded {
        err = users.Set(ctx, key, user, 10*time.Minute)
    }
}
```

If some values can't be decoded, for example because they are corrupt or were written in a format that isn't registered, the values of the other keys are returned along with a `*cache.MultiKeyError` holding the error of each key that failed. Those keys are neither found nor missing, so loading the missing keys doesn't overwrite a value written by a newer service in a format this one can't read:

```go
res, err := users.MGet(ctx, keys...)
if mkErr, ok := errors.AsType[*cache.MultiKeyError](err); ok {
    for key, keyErr := range mkErr.Errors {
        // res holds every other key, handle the failed ones.
    }
} else if err != nil {
    return err // Redis failed, res is empty.
}
```

## Redis Cluster

Redis Cluster doesn't support commands on keys of different hash slots, and the library doesn't split them for you:

* `MGet`, `MSet`, and `Delete` with several keys send a single command, which fails with a `CROSSSLOT` error unless every key is in the same hash slot. Use hash tags, such as `{user:1}:profile` and `{user:1}:friends`, to store related keys in the same slot. `BatchMultiGets` splits keys by count, not by slot, so it doesn't lift this restriction.
* `Flush`, `FlushAsync`, `FlushDB`, and `FlushDBAsync` send `FLUSHDB` to a single master, so they only delete the keys of that node. Use `ForEachMaster` on the `*redis.ClusterClient` to flush every node.
* `Keys` and `Scan` scan every master of a `*redis.ClusterClient`, including one wrapped by a `FailoverClient`.

## Read-Through and Write-Through

`ReadThrough` and `ReadThroughSingleFlight` load a value from the source of truth on a cache miss and cache it. `ReadThroughSingleFlight` shares a load between concurrent callers of the same `Cache` loading the same key and type. Both read Redis once per call.

If a cached value can't be decoded, both report it to `OnCacheError` with `cache.RedisOperationDecode` and load the value from the source of truth. The loaded value replaces a corrupt value. It doesn't replace a value written in a format that isn't registered, which is typically written by a newer service that switched serializer or codec before every reader registered it, so the two services don't keep replacing each other's values. Set `ReadThroughOptions.OverwriteUnsupportedFormat` to replace it anyway.

`WriteThrough` updates the source of truth and then the cache. Keeping both consistent is best effort: if the cache update fails, `WriteThrough` deletes the key so the previous value isn't served anymore, but the delete can fail too, for example while Redis is unavailable. The update and the delete share `CacheWriteTimeout`, so an unavailable Redis delays a write once. Concurrent writes of the same key can also update the source of truth and the cache in a different order. Set a TTL so a stale value eventually expires.

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

A health check must return once its context is done. One that doesn't return within `CheckTimeout` plus 100 ms counts as failed and isn't started again until it returns, so it can't stop failover or hang `Close`.

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

| Metric                                                       | Recorded by                       | Description                                                                                    |
|--------------------------------------------------------------|-----------------------------------|------------------------------------------------------------------------------------------------|
| `cache.serialization.duration`, `cache.serialization.errors` | `NewInstrumentedSerializer`       | Duration and errors of marshal and unmarshal operations, by `cache.serializer`                 |
| `cache.compression.duration`, `cache.compression.errors`     | `NewInstrumentedCompressionCodec` | Duration and errors of compress and decompress operations, by `cache.codec`                    |
| `cache.lookups`                                              | `NewHooks`                        | Keys read from Redis, by `cache.hit`                                                           |
| `cache.value.size`, `cache.value.serialized_size`            | `NewHooks`                        | Size of values as stored in Redis, and after serialization before compression                  |
| `cache.decode.errors`                                        | `NewHooks`                        | Values that couldn't be decoded, by `reason`: `unsupported_format`, `size_limit`, or `invalid` |
| `cache.singleflight.calls`                                   | `NewHooks`                        | `ReadThroughSingleFlight` calls after a miss, by `shared`                                      |
| `cache.failover.active`                                      | `ObserveFailoverClient`           | Which client of a `FailoverClient` is active                                                   |

`cache.Instrumentation` accepts any `cache.Hooks`, so other instrumentation, such as logging, can be plugged in as well. Instrument the Redis client itself, for example with `redisotel`, for command latency and tracing. See `examples/otel`.

## Performance

The `internal/benchmarks` package measures every serializer and compression codec the library provides on the same data, and `TypedCache.Set` and `TypedCache.Get` using them. The values are orders with nested structs, slices, a map, numbers, and text in three sizes. Encoded with MessagePack they are 343 B (small), 2.0 KiB (medium), and 64 KiB (large). The codecs compress the MessagePack encoding, as well as 4 KiB of random, incompressible bytes.

The numbers are medians of 6 runs on an Apple M5 Max (darwin/arm64, Go 1.27.2). Times are per operation on a single goroutine, so they reflect CPU time, and allocations are per operation, as the number of allocations / bytes allocated. Absolute numbers vary with the CPU, but the comparisons hold. The raw results are in `internal/benchmarks/results.txt`. To reproduce them, or to compare a change with `benchstat`:

```shell
go test -run '^$' -bench . -benchmem -count 6 -benchtime 500ms ./internal/benchmarks
```

For context, a Redis round trip over the network usually takes tens to hundreds of microseconds, longer than encoding a small or medium value with any combination below. The choice matters most for large values, CPU-bound services, and the memory used by Redis.

### Choosing a Serializer

* **MessagePack (default) is the best general choice.** It encodes any Go type without a schema or generated code, and is 12-35% faster and 9-23% smaller than JSON.
* **Protobuf is the fastest and the most compact.** It marshals and unmarshals medium and large values about 2x faster than MessagePack and is 34-39% smaller. Use it when your values are already Protobuf messages, or when maintaining a schema is worth it, which also makes evolving the type safer. It requires generated types and works best with `TypedCache[*MyMessage]`. Unmarshaling allocates more objects, one for every nested message.
* **JSON is the slowest and the largest.** Use it when other services or people need to read the cached values, and keep the `encoding/json/v2` semantics in mind, see [JSON Serialization](#json-serialization). It allocates the fewest bytes when marshaling.

| Serializer | Payload | Encoded size | Marshal | Marshal allocs / bytes | Unmarshal | Unmarshal allocs / bytes |
|------------|---------|-------------:|--------:|-----------------------:|----------:|-------------------------:|
| msgpack    | small   |        343 B |  498 ns |             5 / 1008 B |    909 ns |               25 / 960 B |
| json       | small   |        422 B |  670 ns |              4 / 688 B |   1.11 µs |               10 / 704 B |
| protobuf   | small   |        210 B |  458 ns |              9 / 352 B |    570 ns |             31 / 1.1 KiB |
| msgpack    | medium  |      2.0 KiB | 2.64 µs |            8 / 8.0 KiB |   4.22 µs |             70 / 3.8 KiB |
| json       | medium  |      2.2 KiB | 2.96 µs |            4 / 2.5 KiB |   5.56 µs |             34 / 4.3 KiB |
| protobuf   | medium  |      1.3 KiB | 1.35 µs |           13 / 1.6 KiB |   2.23 µs |            106 / 4.5 KiB |
| msgpack    | large   |       64 KiB | 67.6 µs |           12 / 128 KiB |    125 µs |           1628 / 116 KiB |
| json       | large   |       69 KiB | 90.4 µs |             4 / 72 KiB |    169 µs |           1476 / 151 KiB |
| protobuf   | large   |       42 KiB | 31.9 µs |            13 / 48 KiB |   57.3 µs |           2448 / 128 KiB |

### Choosing a Compression Codec

* **Don't compress small values.** No codec shrank the 343 B order, some made it larger, and the slower levels spent up to 20 µs trying. Compression starts paying off at a couple of KiB: the 2 KiB order shrank 1.75-2.2x. The codec applies to every value of a cache, so decide based on the typical size of its values.
* **Don't compress incompressible data**, such as images, encrypted, or already compressed values. Every codec left the random bytes at their original size, while spending up to 51 µs per 4 KiB.
* **S2 is the best choice when latency or CPU matters.** It's the fastest to compress and decompress, 1.2 µs and 0.4 µs for 2 KiB, 1.8 GB/s and 5 GB/s for 64 KiB, and shrinks medium values 1.75x and large values 3.3x. Snappy compresses 3-6% smaller but 1.4-1.6x slower. LZ4 compresses about as well, but is slower than S2 to compress and decompress.
* **Zstandard is the best choice when the memory used by Redis or network bandwidth matters**, especially for large values. At its default level it stores the 64 KiB order in 13 KiB (4.9x) instead of 19 KiB with S2, for about 3x the compression and 2.3x the decompression time. `SpeedFastest` compresses these values almost as well with 12% less CPU, `SpeedBetterCompression` shrinks large values another 7%, and `SpeedBestCompression` shrinks them 11% more for 6.6x the compression time, which only pays off for large values that are written once and read often.
* **Gzip and Flate have no advantage over Zstandard.** At their default level they compress within 4% of Zstandard's default level, but compress 1.5-1.7x slower, decompress 2.3-3.4x slower, and allocate 25-60 objects per decompression. Zstandard's `SpeedBetterCompression` matches or beats Gzip's default level on size, compression, and decompression time. Their best level takes over 1 ms for 64 KiB. Use them to keep reading values written with them, see `DecodeCompression`.
* **Set a decompression limit** when the Redis is shared with other services, see [Decompression Limits](#decompression-limits).

| Codec         | Ratio | Compressed |           Compress | Compress allocs / bytes |         Decompress | Decompress allocs / bytes |
|---------------|------:|-----------:|-------------------:|------------------------:|-------------------:|--------------------------:|
| lz4           | 1.77x |    1.1 KiB | 1.94 µs (1.1 GB/s) |             4 / 3.6 KiB |  643 ns (3.2 GB/s) |               3 / 2.4 KiB |
| s2            | 1.75x |    1.1 KiB | 1.19 µs (1.7 GB/s) |             1 / 2.3 KiB |  421 ns (4.9 GB/s) |               1 / 2.2 KiB |
| snappy        | 1.81x |    1.1 KiB | 1.93 µs (1.1 GB/s) |             1 / 2.3 KiB |  409 ns (5.0 GB/s) |               1 / 2.2 KiB |
| zstd-fastest  | 2.06x |      993 B | 7.18 µs (285 MB/s) |             1 / 2.2 KiB | 3.58 µs (573 MB/s) |               1 / 2.3 KiB |
| zstd-default  | 2.14x |      957 B | 8.22 µs (249 MB/s) |             1 / 2.2 KiB | 3.48 µs (588 MB/s) |               1 / 2.2 KiB |
| zstd-better   | 2.16x |      948 B | 9.25 µs (222 MB/s) |             1 / 2.2 KiB | 3.45 µs (594 MB/s) |               1 / 2.2 KiB |
| zstd-best     | 2.17x |      943 B |  25.4 µs (81 MB/s) |             1 / 2.2 KiB | 3.39 µs (604 MB/s) |               1 / 2.2 KiB |
| flate-speed   | 2.02x |     1017 B | 11.3 µs (181 MB/s) |             4 / 1.9 KiB | 8.36 µs (245 MB/s) |              27 / 5.9 KiB |
| flate-default | 2.20x |      932 B | 13.8 µs (149 MB/s) |             4 / 1.9 KiB | 7.65 µs (268 MB/s) |              29 / 6.0 KiB |
| flate-best    | 2.21x |      929 B |  34.7 µs (59 MB/s) |             4 / 1.9 KiB | 7.74 µs (265 MB/s) |              29 / 6.0 KiB |
| gzip-speed    | 1.98x |    1.0 KiB | 11.8 µs (173 MB/s) |             6 / 3.9 KiB | 8.56 µs (240 MB/s) |              25 / 3.0 KiB |
| gzip-default  | 2.16x |      950 B | 14.2 µs (145 MB/s) |             5 / 1.9 KiB | 7.99 µs (257 MB/s) |              27 / 3.1 KiB |
| gzip-best     | 2.17x |      947 B |  35.4 µs (58 MB/s) |             5 / 2.0 KiB | 7.87 µs (260 MB/s) |              27 / 3.1 KiB |

| Codec         | Ratio | Compressed |           Compress | Compress allocs / bytes |         Decompress | Decompress allocs / bytes |
|---------------|------:|-----------:|-------------------:|------------------------:|-------------------:|--------------------------:|
| lz4           | 3.51x |     18 KiB | 44.2 µs (1.5 GB/s) |              3 / 19 KiB | 15.6 µs (4.2 GB/s) |                3 / 64 KiB |
| s2            | 3.34x |     19 KiB | 35.4 µs (1.8 GB/s) |              1 / 64 KiB | 13.2 µs (5.0 GB/s) |                1 / 64 KiB |
| snappy        | 3.54x |     18 KiB | 50.4 µs (1.3 GB/s) |              1 / 64 KiB | 12.9 µs (5.0 GB/s) |                1 / 64 KiB |
| zstd-fastest  | 4.83x |     13 KiB | 88.2 µs (738 MB/s) |              1 / 64 KiB | 31.0 µs (2.1 GB/s) |                1 / 64 KiB |
| zstd-default  | 4.86x |     13 KiB |  100 µs (650 MB/s) |              1 / 64 KiB | 29.7 µs (2.2 GB/s) |                1 / 64 KiB |
| zstd-better   | 5.20x |     12 KiB |  143 µs (456 MB/s) |              1 / 64 KiB | 26.7 µs (2.4 GB/s) |                1 / 64 KiB |
| zstd-best     | 5.38x |     12 KiB |   658 µs (99 MB/s) |              1 / 64 KiB | 26.3 µs (2.5 GB/s) |                1 / 64 KiB |
| flate-speed   | 4.49x |     14 KiB | 79.1 µs (823 MB/s) |              8 / 32 KiB |  112 µs (580 MB/s) |              52 / 206 KiB |
| flate-default | 5.06x |     13 KiB |  144 µs (453 MB/s) |              8 / 33 KiB |  102 µs (642 MB/s) |              59 / 204 KiB |
| flate-best    | 5.67x |     11 KiB |  1.09 ms (59 MB/s) |              8 / 34 KiB | 87.8 µs (742 MB/s) |              56 / 204 KiB |
| gzip-speed    | 4.48x |     14 KiB | 85.0 µs (766 MB/s) |              9 / 32 KiB |  112 µs (583 MB/s) |               46 / 71 KiB |
| gzip-default  | 5.05x |     13 KiB |  152 µs (427 MB/s) |              9 / 33 KiB | 99.5 µs (654 MB/s) |               53 / 68 KiB |
| gzip-best     | 5.66x |     11 KiB |  1.11 ms (59 MB/s) |              9 / 34 KiB | 85.4 µs (763 MB/s) |               50 / 68 KiB |

<details>
<summary>Small values and incompressible data</summary>

A ratio below 1.00x means the compressed value is larger than the original.

| Codec         | Ratio, small (343 B) | Compress | Decompress | Ratio, random (4 KiB) | Compress | Decompress |
|---------------|---------------------:|---------:|-----------:|----------------------:|---------:|-----------:|
| lz4           |                0.95x |   472 ns |     195 ns |                 1.00x |  1.69 µs |     811 ns |
| s2            |                0.99x |   113 ns |      50 ns |                 1.00x |   714 ns |     473 ns |
| snappy        |                0.99x |   170 ns |      49 ns |                 1.00x |  1.43 µs |     481 ns |
| zstd-fastest  |                0.96x |   317 ns |     105 ns |                 1.00x |   936 ns |     396 ns |
| zstd-default  |                0.96x |   856 ns |     103 ns |                 1.00x |  1.83 µs |     364 ns |
| zstd-better   |                1.01x |  4.39 µs |    1.95 µs |                 1.00x |  4.41 µs |     355 ns |
| zstd-best     |                1.01x |  9.01 µs |    1.95 µs |                 1.00x |  18.5 µs |     358 ns |
| flate-speed   |                0.98x |   218 ns |     206 ns |                 1.00x |   910 ns |    1.62 µs |
| flate-default |                1.05x |  6.26 µs |    3.37 µs |                 1.00x |  1.65 µs |    1.62 µs |
| flate-best    |                1.05x |  19.5 µs |    3.51 µs |                 1.00x |  50.6 µs |    1.63 µs |
| gzip-speed    |                0.93x |   265 ns |     263 ns |                 0.99x |  1.22 µs |     952 ns |
| gzip-default  |                0.99x |  6.40 µs |    3.45 µs |                 0.99x |  2.03 µs |     951 ns |
| gzip-best     |                0.99x |  19.7 µs |    3.65 µs |                 0.99x |  51.5 µs |     924 ns |

</details>

### Cost per Cache Operation

`TypedCache.Set` and `TypedCache.Get` include serialization, compression, the format header, and building the go-redis command. They are measured with a client storing values in memory, so the network isn't included. The library adds less than 0.5 µs on top of serialization. With the default configuration, setting a 2 KiB value takes 2.9 µs and getting it 4.7 µs. S2 adds 1.3 µs and 0.4 µs, Zstandard 8.2 µs and 3.5 µs, and Gzip 14 µs and 8 µs. Compression also narrows the size differences between serializers: with Zstandard, the JSON encoding of the medium order is smaller (890 B) than the MessagePack encoding (961 B).

<details>
<summary>Set and Get with every serializer and common codecs</summary>

| Serializer | Codec        |  Stored |     Set | Set allocs / bytes |     Get | Get allocs / bytes |
|------------|--------------|--------:|--------:|-------------------:|--------:|-------------------:|
| msgpack    | none         | 2.0 KiB | 2.91 µs |       13 / 8.4 KiB | 4.67 µs |       74 / 6.3 KiB |
| msgpack    | lz4          | 1.1 KiB | 4.84 µs |        16 / 11 KiB | 5.24 µs |       77 / 7.8 KiB |
| msgpack    | s2           | 1.1 KiB | 4.20 µs |        15 / 11 KiB | 5.10 µs |       75 / 7.5 KiB |
| msgpack    | zstd-default |   961 B | 11.1 µs |        15 / 11 KiB | 8.14 µs |       75 / 7.3 KiB |
| msgpack    | gzip-default |   954 B | 17.3 µs |        18 / 11 KiB | 12.5 µs |      101 / 8.1 KiB |
| json       | none         | 2.2 KiB | 3.19 µs |       11 / 5.2 KiB | 5.74 µs |       38 / 6.8 KiB |
| json       | lz4          | 1.2 KiB | 5.06 µs |       12 / 4.3 KiB | 6.24 µs |       41 / 8.1 KiB |
| json       | s2           | 1.2 KiB | 4.46 µs |       11 / 5.6 KiB | 6.23 µs |       39 / 8.0 KiB |
| json       | zstd-default |   890 B | 10.3 µs |       11 / 5.2 KiB | 9.00 µs |       39 / 8.0 KiB |
| json       | gzip-default |   862 B | 14.1 µs |       14 / 5.1 KiB | 13.2 µs |       45 / 7.8 KiB |
| protobuf   | none         | 1.3 KiB | 1.46 µs |       19 / 3.2 KiB | 2.33 µs |      111 / 6.1 KiB |
| protobuf   | lz4          | 1.0 KiB | 2.98 µs |       21 / 4.9 KiB | 2.73 µs |      114 / 7.3 KiB |
| protobuf   | s2           |   963 B | 2.34 µs |       19 / 3.2 KiB | 2.53 µs |      112 / 7.1 KiB |
| protobuf   | zstd-default |   864 B | 9.04 µs |       19 / 3.2 KiB | 5.59 µs |      112 / 6.9 KiB |
| protobuf   | gzip-default |   861 B | 14.4 µs |       22 / 3.9 KiB | 9.74 µs |      136 / 7.9 KiB |

| Serializer | Codec        | Stored |     Set | Set allocs / bytes |     Get | Get allocs / bytes |
|------------|--------------|-------:|--------:|-------------------:|--------:|-------------------:|
| msgpack    | none         | 64 KiB | 66.4 µs |       17 / 128 KiB |  128 µs |     1632 / 180 KiB |
| msgpack    | lz4          | 18 KiB |  111 µs |       20 / 149 KiB |  142 µs |     1635 / 200 KiB |
| msgpack    | s2           | 19 KiB |  101 µs |       19 / 193 KiB |  138 µs |     1633 / 200 KiB |
| msgpack    | zstd-default | 13 KiB |  172 µs |       19 / 192 KiB |  158 µs |     1633 / 194 KiB |
| msgpack    | gzip-default | 13 KiB |  218 µs |       26 / 171 KiB |  228 µs |     1685 / 198 KiB |
| json       | none         | 69 KiB | 94.7 µs |       11 / 145 KiB |  171 µs |     1480 / 223 KiB |
| json       | lz4          | 19 KiB |  139 µs |       13 / 132 KiB |  185 µs |     1484 / 365 KiB |
| json       | s2           | 20 KiB |  128 µs |       11 / 146 KiB |  180 µs |     1481 / 243 KiB |
| json       | zstd-default | 12 KiB |  197 µs |       11 / 146 KiB |  200 µs |     1481 / 237 KiB |
| json       | gzip-default | 11 KiB |  241 µs |       18 / 114 KiB |  269 µs |     1505 / 238 KiB |
| protobuf   | none         | 42 KiB | 33.2 µs |        19 / 96 KiB | 55.4 µs |     2453 / 177 KiB |
| protobuf   | lz4          | 17 KiB | 68.6 µs |        20 / 67 KiB | 64.2 µs |     2456 / 195 KiB |
| protobuf   | s2           | 17 KiB | 59.5 µs |        19 / 97 KiB | 63.5 µs |     2454 / 195 KiB |
| protobuf   | zstd-default | 12 KiB |  129 µs |        19 / 96 KiB | 82.3 µs |     2454 / 190 KiB |
| protobuf   | gzip-default | 13 KiB |  155 µs |        26 / 88 KiB |  151 µs |     2503 / 194 KiB |

</details>

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