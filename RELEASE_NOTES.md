# Release Notes

## v2.0.0

#### Breaking Changes

* Module path updated to `github.com/jkratz55/redis-cache/v2`.
* `Codec` interface has been renamed to `CompressionCodec`. Methods `Flate` and `Deflate` have been renamed to `Compress` and `Decompress` respectively.
* `Marshaller` and `Unmarshaller` function types have been replaced by the `Serializer` interface. `DefaultMarshaller` and `DefaultUnmarshaller` have been removed, the default is `MessagePackSerializer{}`.
* `NewCache`, deprecated since v1.x, has been removed. Use `New`.
* The `JSON`, `GZip`, `Flate`, `LZ4`, `Snappy`, and `Brotli` options have been removed. Configure the serializer and codec explicitly, for example `cache.Serialization(cache.JsonSerializer{})` or `cache.Compression(gzip.NewCodec(6))`.
* **`Set` takes a TTL**: `Cache.Set` now takes a TTL like `TypedCache.Set`, and `SetWithTTL` has been removed. Pass `0` or `InfiniteTTL` for no expiration:

    ```go
    // v1.x
    err := c.Set(ctx, key, v)
    err = c.SetWithTTL(ctx, key, v, time.Minute)

    // v2
    err := c.Set(ctx, key, v, 0)
    err = c.Set(ctx, key, v, time.Minute)
    ```
* **`MSetWithTTL`, `CommandError`, and `CommandErrors` have been removed**: `MSet` sets the entries without a TTL, as before. To set several entries with a TTL, pipeline `SET` commands on `Client().Pipeline()` or call `Set` for each key. The errors of individual keys are now reported by `*MultiKeyError`, see New Features.
* **`Upsert` takes a TTL**: `Upsert` now takes the TTL that `UpsertTTL` took, and `UpsertTTL` has been removed. Pass `0` or `InfiniteTTL` to keep the previous behavior of `Upsert`.
* `Cache.Healthy` has been removed. Use `Client().Ping(ctx).Err()`, or `FailoverClient` for automatic failover.
* `Cache.ExtendTTL` has been removed. Read the remaining TTL with `TTL` and set the new one with `Expire`.
* `ScanKeys`, deprecated since v1.x, and the generic `Scan[T]` function, which fetched the values of every key matching a pattern, have been removed. Use the `Scan` method to list the keys and `MGet` to fetch their values.
* The read-through and write-through helpers `Cacheable`, `Write`, and `Delete` have been removed. `Cacheable` is replaced by `ReadThrough` and `ReadThroughSingleFlight`, and `Write` by `WriteThrough`, which take an options struct and report cache failures through callbacks and `*WriteThroughError` instead of failing the call. `Delete` has no replacement: delete from the source of truth, then call `Cache.Delete`.
* OpenTelemetry instrumentation has moved to the new `cacheotel` package, replacing the `otel` and `prometheus` packages, the `Hook` interface, and `Cache.AddHook`. `cacheotel` records serialization and compression metrics by wrapping the `Serializer` and `CompressionCodec` with `cacheotel.NewInstrumentedSerializer` and `cacheotel.NewInstrumentedCompressionCodec`. Metrics and tracing for Redis commands come from instrumenting the Redis client, for example with `redisotel-native` for metrics and `redisotel` for tracing. See `examples/otel`.
* Updated minimum Go version to 1.27.
* Removed Brotli compression support. The `compression/brotli` package and the `github.com/andybalholm/brotli` dependency have been removed because Brotli is too slow for caching workloads. Switch to `compression/zstd` or `compression/s2` instead. Data previously stored with Brotli compression cannot be read by other codecs and should be expired or rewritten.
* **Versioned value format**: every value written by `Cache` and `TypedCache` is now prefixed with a 4-byte header (`0xC1`, version `0x01`, serializer ID, codec ID). Values written by v1.x and by the release candidates rc1 and rc2, which have no header, are still read using the configured serializer and codec. rc3 already writes the header. However, **older versions of the library can't read values written with the header**. When multiple services share a Redis instance, upgrade every reader before, or together with, the writers.
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
* *Release candidates only:* `JsonSerializer` and `MessagePackSerializer` use value receivers since they are stateless. Both `cache.JsonSerializer{}` and `&cache.JsonSerializer{}` can be used and are treated as the same implementation.
* **`RedisClient` requires `Persist`, `PExpire`, `PTTL`, and `Close`**: these methods were added to the `RedisClient` interface to support millisecond TTLs, removing TTLs, and closing the clients owned by `FailoverClient`. All go-redis clients and `FailoverClient` already implement them; custom implementations must add them.
* **`Expire` semantics changed**: `Expire(ctx, key, InfiniteTTL)` and `Expire(ctx, key, 0)` now remove the TTL (`PERSIST`) instead of deleting the key. Other negative TTLs return the new `ErrInvalidTTL`. `Expire` and `TTL` now use millisecond precision (`PEXPIRE`/`PTTL`) instead of rounding to seconds.
* *Release candidates only:* **`NewFailoverClient` validates options**: user-provided `FailoverOpts` are now merged with the defaults before being validated. `NewFailoverClient` panics if `CheckInterval`, `CheckTimeout`, `FailureThreshold` or `SuccessThreshold` is negative. A `CheckInterval` below one second is rounded up to one second and a `CheckTimeout` greater than or equal to `CheckInterval` is set to half of `CheckInterval`, as documented.
* **Codec levels are validated**: `gzip.NewCodec` and the new `flate.NewCodec` panic if the compression level is invalid. Previously an invalid gzip level failed every `Compress` call.
* `flate.Codec` and `snappy.Codec` now use pointer receivers, so a `flate.Codec{}` or `snappy.Codec{}` value no longer implements `CompressionCodec`. Use `NewCodec` or a pointer to the struct.
* **Decoding of values with a header**: values written by older versions can start with the same bytes as the header. So a value with a header that can't be decoded, including one that references an unregistered serializer or codec, is decoded once more as a value without a header. That attempt uses the new `StrictUnmarshaler` interface if the serializer implements it, as `cacheproto.ProtobufSerializer` does, so a corrupt value or a value in an unsupported format isn't mistaken for an old value. If it fails too, the original error is returned, which wraps `ErrUnsupportedFormat` if the format isn't registered. When decoding fails, the destination is reset to its zero value instead of holding partially decoded data.
* **Negative TTLs are rejected**: every method that takes a TTL returns `ErrInvalidTTL` for a negative TTL other than `InfiniteTTL` and `KeepTTL`. Previously `Set` stored the value without an expiration, `SetIfAbsent` and `SetIfPresent` failed with a Redis error, and `GetAndExpire` left the TTL unchanged.
* **`BatchMultiGets` validates the batch size**: it panics if the batch size is negative. Previously a negative batch size made `TypedCache.MGet` panic.
* Built-in codecs and `cacheproto.ProtobufSerializer` now import the root `cache` package to return the shared format ID constants.
* **`JsonSerializer` uses `encoding/json/v2`**: `JsonSerializer` now uses `encoding/json/v2` with its default options instead of `encoding/json`, which was used by the JSON option of v1.x and by the release candidates rc1 and rc2. The defaults differ: `time.Duration` can't be marshaled or unmarshaled without a format option, object names are matched case-sensitively when unmarshaling, so differently cased names are silently ignored, and nil slices and maps are marshaled as `[]` and `{}` instead of `null`. See the `encoding/json/v2` documentation for every difference. JSON values written by older versions may fail to decode or decode differently, which `ReadThrough` turns into a load from the source of truth on every request. To keep the behavior of `encoding/json`, use your own serializer with a custom format ID. Values written by older versions have no header, so they are decoded with it:

    ```go
    type JSONv1Serializer struct{}

    func (JSONv1Serializer) Marshal(v any) ([]byte, error)    { return json.Marshal(v) } // encoding/json
    func (JSONv1Serializer) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
    func (JSONv1Serializer) FormatID() uint8                 { return 200 }

    c := cache.New(client, cache.Serialization(JSONv1Serializer{}))
    ```
* **`Cache.Client` and `TypedCache.Client` return `RedisClient`**: both returned `redis.UniversalClient`, and `TypedCache.Client` panicked for any other `RedisClient`. They now return the client provided to `New` or `NewTyped`. Use a type assertion if you need a method `RedisClient` doesn't have.
* **`MGet` returns a `MultiResult` like the reply to `MGET`**: the `MGet` function of `Cache` and `TypedCache.MGet` return a `MultiResult[T]` with an entry for every requested key, in the order of the keys, including keys that don't exist. `At(i)` returns the value of the i-th key and whether it was found, `All` and `Values` return the keys and values that were found in the order of the keys, `Map` returns them by key, and `Missing` returns the keys that don't exist, for example to load them from the source of truth. In v1.x, `MGet` returned `MultiResult` as a `map[string]T` and the `MGetValues` function returned the values that were found as a slice. `MultiResult` is now a struct, so indexing it, ranging over it, and calling `len` on it don't compile. Its `Get`, `Keys`, and `IsEmpty` methods and `MGetValues` have been removed:

    ```go
    // v1.x
    m, err := cache.MGet[User](ctx, c, keys...)
    user, ok := m.Get("user:1")
    values, err := cache.MGetValues[User](ctx, c, keys...)

    // v2
    res, err := cache.MGet[User](ctx, c, keys...)
    user, ok := res.At(0) // the value of keys[0], or use res.Map()["user:1"]
    values := res.Values()
    ```

  When some values can't be decoded, the values of the other keys are returned along with a `*MultiKeyError` holding the error of every key that failed, instead of no values. Those keys are neither found nor missing, so loading the missing keys doesn't overwrite values written in a format the service can't read. Code checking `err != nil` keeps working. A Redis error returns an empty `MultiResult`. The `MGet` function of `Cache` called without keys returns an empty `MultiResult` without sending a command, like `TypedCache.MGet`, instead of failing with a Redis error.

#### New Features

* **Read-Through Caching**: New `ReadThrough` and `ReadThroughSingleFlight` functions provide built-in support for read-through caching patterns. `ReadThroughSingleFlight` deduplicates concurrent loads of the same key to prevent cache stampedes.
* **Write-Through Caching**: the new `WriteThrough` function updates the source of truth and then the cache, synchronously or asynchronously, and returns a `*WriteThroughError` reporting whether the source of truth was updated.
* **Failover**: the new `FailoverClient` wraps a primary and a secondary `RedisClient`, health checks both in the background, and sends every command to the primary until it fails `FailureThreshold` consecutive checks, then to the secondary until the primary passes `SuccessThreshold` checks. It is configured with `FailoverOptions()` and reports switches through `OnFailover`, `OnRecovered`, and `OnCheckFailure`. The primary and secondary must be active-active replicated. `cacheotel.ObserveFailoverClient` reports which client is active.
* **`GetAndDelete` on `Cache`**: `Cache.GetAndDelete` retrieves and deletes a key with a single `GETDEL` command, like `TypedCache.GetAndDelete`.
* **S2 Compression**: Added support for S2 compression algorithm.
* **Protobuf Serialization**: added the `cacheproto` package with `ProtobufSerializer` for types implementing `proto.Message`. A `TypedCache` instantiated with a pointer type, such as `TypedCache[*pb.User]`, allocates the message for every decoded value, which is the recommended way to cache Protobuf messages.
* **Change Serialization/Compression Safely**: values record the serializer and codec used to write them, so the configured `Serialization` and `Compression` can be changed without flushing Redis. Formats only needed for reading existing entries can be registered with the new `DecodeSerializers(...)` and `DecodeCompression(...)` options. Reading a value written with an unregistered format returns an error wrapping the new `ErrUnsupportedFormat`.
* **Format IDs**: added `FormatIdentifier`, `FormatIDOf`, `SerializerWithID`, `CodecWithID`, and the reserved ID constants (`SerializerMsgPack`, `SerializerJSON`, `SerializerProtobuf`, `CodecNone`, `CodecFlate`, `CodecGzip`, `CodecLZ4`, `CodecS2`, `CodecSnappy`, `CodecZstd`). Wrappers that don't alter the bytes report the ID of the component they wrap and implement `Unwrap()`, which allows them to wrap built-in implementations, as the `cacheotel` instrumented serializer and codec do. See `examples/customserializer`.
* **Zstandard Compression**: added the `compression/zstd` package. `zstd.NewCodec(level, opts...)` returns an error if the encoder or decoder can't be created, and `Close` releases them: it waits for the calls in flight, after which `Compress` and `Decompress` return `zstd.ErrClosed`. `zstd.WithMaxDecodedSize(n)` limits the size of decompressed values to protect against corrupt or malicious values; there is no limit by default.
* **flate constructor**: added `flate.NewCodec(level)`, which validates the level. The flate codec now pools writers and readers.
* **ReadThroughOptions.LoaderTimeout**: bounds the loader and cache update shared by all callers of `ReadThroughSingleFlight`. The shared work is also canceled once no caller is waiting for it anymore, and the next caller starts a new load.
* **ErrInvalidTTL**: returned for negative TTLs other than `InfiniteTTL` and `KeepTTL`, and by `Expire` for `KeepTTL`.
* **Decompression limits**: every built-in codec accepts a `WithMaxDecodedSize(n)` option (`flate.NewCodec`, `gzip.NewCodec`, `lz4.NewCodec`, `s2.NewCodec`, and `snappy.NewCodec` now take options, `zstd.NewCodec` already did). Values that would decompress to more than `n` bytes fail with an error wrapping the new `cache.ErrMaxDecodedSizeExceeded`, which protects services sharing a Redis against corrupt or malicious values exhausting memory. For example, without a limit a 6-byte value can make the S2 and Snappy codecs allocate 4 GiB. There is no limit by default.
* **Instrumentation hooks**: the new `Instrumentation` option configures `Hooks` invoked on cache lookups (hits and misses), when values are encoded and decoded (sizes and decode errors), and when `ReadThroughSingleFlight` starts or joins a load. Hooks receive the context of the operation and their panics are recovered.
* **cacheotel metrics**: `cacheotel.NewHooks` records `cache.lookups`, `cache.value.size`, `cache.value.serialized_size`, `cache.decode.errors` (by reason: `unsupported_format`, `size_limit`, or `invalid`), and `cache.singleflight.calls`. `cacheotel.ObserveFailoverClient` reports which client of a `FailoverClient` is active with the `cache.failover.active` gauge. `cacheotel.WithAttributes` adds attributes, such as a cache name, to every measurement, and `cacheotel.WithSizeBoundaries` sets the buckets of the size histograms.
* **MultiKeyError**: returned by `MGet` with the error of every key that couldn't be decoded, by key. `errors.Is` and `errors.As` match the error of any key.
* **Inspection CLI**: `rediscache-inspect` (`cmd/rediscache-inspect`) fetches values and prints their encoding, compression, TTL, and decoded data, since tools such as Redis Insight show the header and the compressed payload as raw bytes. Values without a header are detected or decoded as set by flags, and `-o json` prints one JSON object per key. See the README.
* **FailoverOpts.HealthCheck**: replaces the default `PING` health check, for example with a write to a canary key, which also detects a Redis that answers `PING` but rejects writes. A panic in the health check counts as a failed check, and so does a check that doesn't return within `CheckTimeout` plus 100 ms. Such a check isn't started again until it returns, and `Close` doesn't wait for it, so a check that ignores its context can't stop failover or hang `Close`.

#### Bug Fixes

Fixes to features that didn't exist in v1.x, such as `FailoverClient`, `ReadThrough`, `ReadThroughSingleFlight`, `WriteThrough`, and the zstd codec, only affect the release candidates.

* `InfiniteTTL` is now accepted by every method that takes a TTL. Previously `SetIfAbsent` and `SetIfPresent` failed with an invalid expire time error, and `TypedCache.GetAndExpire` didn't remove the TTL.
* Fixed a data race in `Scan` on Redis Cluster that could lose or corrupt keys. `Scan` returns no keys when it fails, also scans every master of a cluster client wrapped by a `FailoverClient`, and `TypedCache.Scan` no longer panics for a negative batch.
* `FailoverClient` is now safe for concurrent use while it switches between clients. It no longer fails over to a secondary that is failing its health checks, no callbacks are invoked once `Close` has been called, a callback can call `Close`, and a panicking callback no longer crashes the process.
* `IsRetryable` now detects retryable errors that have been wrapped, and the zero value of `RetryableError` no longer panics.
* Redis errors from `Delete`, `Flush`, `FlushAsync`, `FlushDB` and `FlushDBAsync` are now prefixed with `redis: `. `Upsert` no longer double-prefixes errors, reads the current value on the watched connection, and returns an error for a nil callback. Decode errors from multi-key operations include the key.
* `Serialization`, `Compression`, `DecodeSerializers`, `DecodeCompression`, `SerializerWithID` and `CodecWithID` now panic for typed nil pointers.
* Asynchronous cache updates in `ReadThrough`, `ReadThroughSingleFlight` and `WriteThrough` keep the caller's context values, such as trace information, while no longer being canceled with the caller's context.
* `ReadThroughSingleFlight` no longer fails every waiting caller when the caller that started the load cancels its context, invokes `OnCacheHit`/`OnCacheMiss` exactly once per caller, and returns a panic in the loader as an error.
* The flate codec now returns the error from closing the writer.
* The zstd codec no longer leaks its encoder when creating the decoder fails. Values exceeding its `WithMaxDecodedSize` limit now fail with an error wrapping `cache.ErrMaxDecodedSizeExceeded` as well as the error of the zstd decoder.
* `ReadThroughSingleFlight` no longer shares loads between different `Cache` instances or different types. The release candidates shared one load per key across the whole process, so a caller could receive a value loaded for another cache, possibly backed by a different Redis, without its own loader running or its cache being populated, and a caller loading a different type for the same key panicked. A loader returning a nil interface value no longer panics either.
* `WriteThrough` deletes the key when updating the cache fails after the source of truth was updated, so the previous value isn't served anymore. This is best effort, the delete can fail too, in which case the error wraps both errors. The update and the delete share `CacheWriteTimeout`, so an unavailable Redis delays a write once, not twice.
* `ReadThroughSingleFlight` reads Redis once per call. The shared load no longer reads Redis again before invoking the `LoaderFunc`, which cost every miss another round trip and reported it twice to `Hooks.OnLookup`.
* `ReadThrough` and `ReadThroughSingleFlight` no longer replace a cached value written in a format that isn't registered, such as a value written by a newer service with a codec this one doesn't register yet. They return the loaded value without caching it, so services migrating to a new format don't keep replacing each other's values and loading from the source of truth. Set the new `ReadThroughOptions.OverwriteUnsupportedFormat` to replace such values. Corrupt values are still replaced. Values that can't be decoded are reported to `OnCacheError` with the new `RedisOperationDecode` instead of `RedisOperationGET`, so they can be told apart from Redis failing.
* A panic in `OnCacheError` or `OnAsyncUpdateCompleted` invoked from a goroutine started by `ReadThrough`, `ReadThroughSingleFlight`, or `WriteThrough` is recovered instead of crashing the process.
* `FailoverClient.IsPrimary`, `IsSecondary`, and `Active` are derived from a single atomic state, so they no longer briefly disagree while `FailoverClient` switches clients.

#### Improvements

* Updated dependencies:
    * `github.com/redis/go-redis/v9` to `v9.23.0`
    * `go.opentelemetry.io/otel` to `v1.47.0`
    * `google.golang.org/protobuf` to `v1.36.12`
    * `github.com/klauspost/compress` to `v1.20.3`
    * `github.com/pierrec/lz4/v4` to `v4.1.33`
* `go.uber.org/multierr` and `github.com/andybalholm/brotli` are no longer dependencies. `google.golang.org/protobuf` is now a direct dependency, used by `cacheproto`.
* Values are encoded directly after the format header, which saves an allocation and a copy of the value on every write. Serializers and codecs can implement the new optional `MarshalAppender` and `CompressAppender` interfaces to benefit; `MessagePackSerializer` and every built-in codec do. With the default configuration, encoding a 1 KiB value is about 24% faster. The gzip codec sizes its buffer from the size declared by the value, bounded to 8 times the compressed size and 1 MiB since a corrupt value can declare any size, and the flate and gzip codecs allocate up to half as much memory when decompressing.
* The lz4 codec writes frames with 64 KiB blocks instead of the LZ4 default of 4 MiB. With 4 MiB blocks, the first compression and decompression after every garbage collection allocated more than 8 MiB each, and warm pools kept 4 MiB buffers per CPU. With 64 KiB blocks they allocate less than 300 KiB, at the cost of decompressing 10-15% slower with warm pools. Values written with 4 MiB blocks remain readable.
* Documentation: `FailoverClient` requires the primary and secondary to be active-active replicated, such as Redis Enterprise Active-Active databases, since commands are only sent to the active client. Without replication, values served after switching are stale. `FailoverClient` takes ownership of both clients. Multi-key commands and `Flush` on Redis Cluster are documented, see the README.
* Every exported identifier is documented.
* Significant increase in unit test coverage, a fuzz test for decoding values (`FuzzDecode`), and benchmarks for encoding and decoding with every codec.
* New examples for read-through, write-through, Protobuf, and custom serializers in the `examples/` directory. `examples/prometheus` has been removed along with the `prometheus` package, see `examples/otel`.

#### Upgrading from a Release Candidate

Values written by rc1 and rc2 have no format header and are read like values written by v1.x. rc3 already writes the header and already uses `encoding/json/v2`, so the breaking changes for both only apply when upgrading from rc1 or rc2.

The cache-level instrumentation in `cacheotel` has been removed. `cacheotel.Init`, `InstrumentCache` and `InstrumentedCache`, `InstrumentTypedCache` and `InstrumentedTypedCache`, the `cacheotel.TypedCache` interface, `cacheotel.MGet`, `cacheotel.MGetMap`, `cacheotel.Upsert`, `cacheotel.Option`, `WithTraceProvider` and `WithMgetKeyBoundaries` no longer exist. Use `cache.Cache` and `cache.TypedCache` directly with `cacheotel.NewInstrumentedSerializer` and `cacheotel.NewInstrumentedCompressionCodec`, and instrument the Redis client for command metrics and tracing, for example with `redisotel-native` and `redisotel`. See `examples/otel`.

`MGetMap` has been removed, and `MGet` returns a `MultiResult[T]` instead of a slice of the values that were found. Use `res.Values()` for the slice `MGet` returned and `res.Map()` for the map `MGetMap` returned, or `res.At(i)` and `res.All()`, which don't allocate. See the breaking changes for `MultiResult`.

The `cache.execution_duration` and `cache.mget_keys` metrics, and the spans for each operation with the `cache.hit` attribute, are no longer recorded. Cache hits and misses are recorded by `cacheotel.NewHooks` as `cache.lookups`, and values written in a format that isn't registered as `cache.decode.errors`.

The default buckets of `cache.serialization.duration` and `cache.compression.duration` now range from 10µs to 50ms instead of 5ms to 100ms, since serialization and compression usually take microseconds. Both metrics have a new `cache.serializer` or `cache.codec` attribute.

The `Logger` interface, `SlogLogger`, and `NewSlogLogger` have been removed since nothing in the library used them.
