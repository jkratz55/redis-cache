# Release Notes

## v2.0.0

#### Breaking Changes

* Module path updated to `github.com/jkratz55/redis-cache/v2`.
* `Codec` interface has been renamed to `CompressionCodec`. Methods `Flate` and `Deflate` have been renamed to `Compress` and `Decompress` respectively.
* `Marshaller` and `Unmarshaller` function types have been replaced by the `Serializer` interface.
* `ThroughCache` has been removed and replaced with a more robust `ReadThrough` implementation.
* Refactored `cacheotel` package for OpenTelemetry metrics, simplifying configuration and removing several internal components.
* Updated minimum Go version to 1.24.
* Removed Brotli compression support. The `compression/brotli` package and the `github.com/andybalholm/brotli` dependency have been removed because Brotli is too slow for caching workloads. Switch to `compression/zstd` or `compression/s2` instead. Data previously stored with Brotli compression cannot be read by other codecs and should be expired or rewritten.
* **Versioned value format**: every value written by `Cache` and `TypedCache` is now prefixed with a 4-byte header (`0xC1`, version `0x01`, serializer ID, codec ID). Values written by v1.x and v2.0.0 release candidates (no header) are still read using the configured serializer and codec. However, **older versions of the library can't read values written with the header**. When multiple services share a Redis instance, upgrade every reader before, or together with, the writers.
* **Custom serializers/codecs require a format ID**: `New`, `NewTyped`, `cacheotel.NewInstrumentedSerializer`, and `cacheotel.NewInstrumentedCompressionCodec` now panic if a `Serializer` or `CompressionCodec` doesn't have a format ID. Built-in implementations already have one. Custom implementations must implement `FormatID() uint8` returning an ID in the range 128-255, or be wrapped with `cache.SerializerWithID` / `cache.CodecWithID`:

    ```go
    // Before
    c := cache.New(client, cache.Serialization(MySerializer{}))

    // After: implement the interface on a type you own...
    func (MySerializer) FormatID() uint8 { return 200 }

    // ...or wrap a type you can't modify.
    c := cache.New(client, cache.Serialization(cache.SerializerWithID(200, MySerializer{})))
    ```

  Pick IDs carefully, once values have been written with an ID it must never be reused for a different format.
* `JsonSerializer` and `MessagePackSerializer` use value receivers since they are stateless. Both `cache.JsonSerializer{}` and `&cache.JsonSerializer{}` can be used and are treated as the same implementation.
* **`RedisClient` requires `Persist`, `PExpire` and `PTTL`**: these methods were added to the `RedisClient` interface to support millisecond TTLs and removing TTLs. All go-redis clients and `FailoverClient` already implement them; custom implementations must add them.
* **`Expire` semantics changed**: `Expire(ctx, key, InfiniteTTL)` and `Expire(ctx, key, 0)` now remove the TTL (`PERSIST`) instead of deleting the key. Other negative TTLs return the new `ErrInvalidTTL`. `Expire` and `TTL` now use millisecond precision (`PEXPIRE`/`PTTL`) instead of rounding to seconds.
* **`NewFailoverClient` validates options**: user-provided `FailoverOpts` are now merged with the defaults before being validated. `NewFailoverClient` panics if `CheckInterval`, `CheckTimeout`, `FailureThreshold` or `SuccessThreshold` is negative. A `CheckInterval` below one second is rounded up to one second and a `CheckTimeout` greater than or equal to `CheckInterval` is set to half of `CheckInterval`, as documented.
* **Codec levels are validated**: `gzip.NewCodec` and the new `flate.NewCodec` panic if the compression level is invalid. Previously an invalid gzip level failed every `Compress` call.
* **Stricter decoding of headered values**: a value with a valid header that references an unregistered serializer or codec now always returns `ErrUnsupportedFormat` and is no longer retried as a legacy value. When decoding fails, the destination is reset to its zero value instead of holding partially decoded data.
* Built-in codecs and `cacheproto.ProtobufSerializer` now import the root `cache` package to return the shared format ID constants.

#### New Features

* **Read-Through Caching**: New `ReadThrough` and `ReadThroughSingleFlight` functions provide built-in support for read-through caching patterns. `ReadThroughSingleFlight` utilizes `golang.org/x/sync/singleflight` to prevent cache stampedes.
* **MGetMap**: Added `MGetMap` to both `Cache` and `TypedCache`, returning a `MultiResult` map for easier access to multiple keys.
* **Upsert**: Added `Upsert` function to `Cache` for atomic update operations.
* **S2 Compression**: Added support for S2 compression algorithm.
* **Protobuf Pointer Support**: `TypedCache` now has improved support for Protobuf messages when using pointers.
* **Change Serialization/Compression Safely**: values record the serializer and codec used to write them, so the configured `Serialization` and `Compression` can be changed without flushing Redis. Formats only needed for reading existing entries can be registered with the new `DecodeSerializers(...)` and `DecodeCompression(...)` options. Reading a value written with an unregistered format returns an error wrapping the new `ErrUnsupportedFormat`.
* **Format IDs**: added `FormatIdentifier`, `FormatIDOf`, `SerializerWithID`, `CodecWithID`, and the reserved ID constants (`SerializerMsgPack`, `SerializerJSON`, `SerializerProtobuf`, `CodecNone`, `CodecFlate`, `CodecGzip`, `CodecLZ4`, `CodecS2`, `CodecSnappy`, `CodecZstd`). The `cacheotel` instrumented serializer and codec report the ID of the component they wrap. See `examples/customserializer`.
* **zstd resource management**: `zstd.Codec` has a new `Close` method that releases the encoder and decoder, and `zstd.NewCodec` accepts options. `zstd.WithMaxDecodedSize(n)` limits the size of decompressed values to protect against corrupt or malicious values; there is no limit by default.
* **flate constructor**: added `flate.NewCodec(level)`, which validates the level. The flate codec now pools writers and readers.
* **ReadThroughOptions.LoaderTimeout**: bounds the loader and cache update shared by all callers of `ReadThroughSingleFlight`.
* **ErrInvalidTTL**: returned by `Expire` for negative TTLs other than `InfiniteTTL`.

#### Bug Fixes

* `InfiniteTTL` is now accepted by every method that takes a TTL. Previously `SetIfAbsent` and `SetIfPresent` failed with an invalid expire time error, and `TypedCache.GetAndExpire` didn't remove the TTL.
* Fixed a data race in `Scan` on Redis Cluster that could lose or corrupt keys. `Scan` returns no keys when it fails, also scans every master of a cluster client wrapped by a `FailoverClient`, and `TypedCache.Scan` no longer panics for a negative batch.
* `FailoverClient` is now safe for concurrent use while it switches between clients. It no longer fails over to a secondary that is failing its health checks, `Close` waits for the monitoring goroutine so no callbacks run after `Close` returns, and a panicking callback no longer crashes the process.
* `IsRetryable` now detects retryable errors that have been wrapped, and the zero value of `RetryableError` no longer panics.
* Redis errors from `Delete`, `Flush`, `FlushAsync`, `FlushDB` and `FlushDBAsync` are now prefixed with `redis: `. `Upsert` no longer double-prefixes errors, reads the current value on the watched connection, and returns an error for a nil callback. Decode errors from multi-key operations include the key.
* `Serialization`, `Compression`, `DecodeSerializers`, `DecodeCompression`, `SerializerWithID` and `CodecWithID` now panic for typed nil pointers.
* Asynchronous cache updates in `ReadThrough`, `ReadThroughSingleFlight` and `WriteThrough` keep the caller's context values, such as trace information, while no longer being canceled with the caller's context.
* `ReadThroughSingleFlight` no longer fails every waiting caller when the caller that started the load cancels its context, invokes `OnCacheHit`/`OnCacheMiss` exactly once per caller, and returns a panic in the loader as an error.
* The flate codec now returns the error from closing the writer.
* The zstd codec no longer leaks its encoder when creating the decoder fails.

#### Improvements

* Updated dependencies:
    * `github.com/redis/go-redis/v9` to `v9.18.0`
    * `go.opentelemetry.io/otel` to `v1.43.0`
    * `google.golang.org/protobuf` to `1.36.11`
    * `github.com/klauspost/compress` to `v1.18.2`
* Significant increase in unit test coverage.
* Improved examples in the `examples/` directory.
