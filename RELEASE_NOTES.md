# Release Notes

## v1.5.0

Nothing much with this release. Just updated the dependencies to the latest versions.

## v1.4.0

* Upgraded OpenTelemetry dependencies
* Upgraded go-redis client/driver
* Upgraded other third party dependencies like msgpack, etc.

## v1.3.0

* [BREAKING] - MSet no longer accepts a TTL value. The keys and values passed to MSet will not have an expiration/ttl. The way ttl was implemented in v1.2.0 was non-ideal in retrospective.
* Added MSetWithTLL method on the `Cache` type to set multiple keys with a TTL in a single operation. MSetWithTTL uses pipelining in Redis under the hood to provide good performance while still supporting TTL values.
* Updated msgpack dependency version

## v1.2.0

* Added support for MSET operations
* Updated dependencies except OTEL

## v1.1.0

* Added support for Brotli for compression/decompression
* Added instrumentation for Redis Client and Cache for both Prometheus and OpenTelemetry
* Added Hooks mechanism that instrumentation utilizes, but it can be used for other purposes as well
* Added `GetAndExpire` method on the `Cache` type allowing TTL to be extended when retrieving a key
* Added `TTL` method on `Cache` to get the TTL for a key
* Added `Expire` method on `Cache` to allow the TTL for a key to be updated/set.
* Performance improvement to reuse `gzip.Writer` from compression with Gzip.
* [BREAKING] - Removed NewCodec function and instead created new helper functions for compression options
* [BREAKING] - Renamed `SetTTL` to `SetWithTTL` to remove confusion
