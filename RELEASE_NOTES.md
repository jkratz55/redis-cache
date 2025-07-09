# Release Notes

## v1.8.0

* Methods and functions that rely on Redis SCAN command such as `Keys`, `ScanKeys`, and `Scan` now properly handle the use-case when the underlying client is `redis.ClusterClient`. Before those methods would only scan a single master and not all the masters.
* Updated dependencies

## v1.7.0

* Adding helper functions to make write-through and read-through caching easier to implement

## v1.6.0

* Cache type can now be configured to split MGet with a large number of keys into multiple MGET commands executed within a pipeline which under some conditions can significantly reduce latency.
* Added method to get the underlying Redis client from the `Cache` type
* Updated dependencies

## v1.5.3

* Added MSetValues to allow fetching values only with multiple keys.

## v1.5.2

* Update dependencies

## v1.5.1

* Added Scan method on `Cache` to fetch multiple keys by a pattern
* Fixes a bug where Prometheus cache hits metric was being incremented when there was a cache miss
* Adds additional metrics to track how many keys are being sent in MGET commands

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
