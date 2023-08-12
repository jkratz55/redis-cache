# Release Notes

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
