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
* `JsonSerializer` and `MessagePackSerializer` use pointer receivers and must be passed as pointers (e.g. `cache.Serialization(&cache.JsonSerializer{})`).

#### New Features

* **Read-Through Caching**: New `ReadThrough` and `ReadThroughSingleFlight` functions provide built-in support for read-through caching patterns. `ReadThroughSingleFlight` utilizes `golang.org/x/sync/singleflight` to prevent cache stampedes.
* **MGetMap**: Added `MGetMap` to both `Cache` and `TypedCache`, returning a `MultiResult` map for easier access to multiple keys.
* **Upsert**: Added `Upsert` function to `Cache` for atomic update operations.
* **S2 Compression**: Added support for S2 compression algorithm.
* **Protobuf Pointer Support**: `TypedCache` now has improved support for Protobuf messages when using pointers.
* **Change Serialization/Compression Safely**: values record the serializer and codec used to write them, so the configured `Serialization` and `Compression` can be changed without flushing Redis. Formats only needed for reading existing entries can be registered with the new `DecodeSerializers(...)` and `DecodeCompression(...)` options. Reading a value written with an unregistered format returns an error wrapping the new `ErrUnsupportedFormat`.
* **Format IDs**: added `FormatIdentifier`, `FormatIDOf`, `SerializerWithID`, `CodecWithID`, and the reserved ID constants (`SerializerMsgPack`, `SerializerJSON`, `SerializerProtobuf`, `CodecNone`, `CodecFlate`, `CodecGzip`, `CodecLZ4`, `CodecS2`, `CodecSnappy`, `CodecZstd`). The `cacheotel` instrumented serializer and codec report the ID of the component they wrap. See `examples/customserializer`.

#### Improvements

* Updated dependencies:
    * `github.com/redis/go-redis/v9` to `v9.18.0`
    * `go.opentelemetry.io/otel` to `v1.43.0`
    * `google.golang.org/protobuf` to `1.36.11`
    * `github.com/klauspost/compress` to `v1.18.2`
* Significant increase in unit test coverage.
* Improved examples in the `examples/` directory.
