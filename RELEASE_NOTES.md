# Release Notes

## v2.0.0

#### Breaking Changes

* Module path updated to `github.com/jkratz55/redis-cache/v2`.
* `Codec` interface has been renamed to `CompressionCodec`. Methods `Flate` and `Deflate` have been renamed to `Compress` and `Decompress` respectively.
* `Marshaller` and `Unmarshaller` function types have been replaced by the `Serializer` interface.
* `ThroughCache` has been removed and replaced with a more robust `ReadThrough` implementation.
* Refactored `cacheotel` package for OpenTelemetry metrics, simplifying configuration and removing several internal components.
* Updated minimum Go version to 1.24.

#### New Features

* **Read-Through Caching**: New `ReadThrough` and `ReadThroughSingleFlight` functions provide built-in support for read-through caching patterns. `ReadThroughSingleFlight` utilizes `golang.org/x/sync/singleflight` to prevent cache stampedes.
* **MGetMap**: Added `MGetMap` to both `Cache` and `TypedCache`, returning a `MultiResult` map for easier access to multiple keys.
* **Upsert**: Added `Upsert` function to `Cache` for atomic update operations.
* **S2 Compression**: Added support for S2 compression algorithm.
* **Protobuf Pointer Support**: `TypedCache` now has improved support for Protobuf messages when using pointers.

#### Improvements

* Updated dependencies:
    * `github.com/redis/go-redis/v9` to `v9.18.0`
    * `go.opentelemetry.io/otel` to `v1.43.0`
    * `google.golang.org/protobuf` to `1.36.11`
    * `github.com/klauspost/compress` to `v1.18.2`
* Significant increase in unit test coverage.
* Improved examples in the `examples/` directory.
