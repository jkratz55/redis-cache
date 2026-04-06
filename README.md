# Redis Cache

Redis Cache is a thin abstraction layer on top of the go-redis client. The goal of this library is to improve developer experience and ergonmics when storing and retreiving custom types/data between GO and Redis. I created this library because I found myself consistently writing the same boilerplate code for handling encoding and compression between my types in GO and Redis. This library does make a few assumptions about how you want to store your data in Redis. It assumes you have structures defined in GO (or built in structures/types like strings, maps, slices, etc) and that you want to store them in Redis as bytes (STRING type in Redis). The data can be encoded however you want, you just need to implement the `Serializer` interface. By default, Redis Cache uses msgpack to marshal/unmarshal data, but also provides implementions of the `Serializer` interface for json and protobuf. Compression is also supported by doesn't compress by default. You can use whatever compression you want, you just need to implement the `CompressionCodec` interface. Out of the box, there are implementations for gzip, flate, lz4, brotli, s2, and snappy.

Redis Cache provides two cores types, `Cache` and `TypedCache`. They are functionally identical, but have different APIs. The `Cache` type is more generic and flexible, allowing a single instance to be used with multiple types. It's API is similar to built in Marshal/Unmarshal functions in the GO ecosystem. It expects a pointer to a struct to be passed in, and will marshal/unmarshal the struct into bytes. Meanwhile, the `TypedCache` type is more specific and only works with a single type. However, it simplifes the API, and is type-safe thanks to generics. Unlike the `Cache` type, the `TypedCache` type allows us to support MGet as a method, rather than a standalone function. `TypedCache` also works much better when using Protobuf for encoding/serialization. Which one to choose largely depends on your use case and preferences.

Instrumentation is key to any production ready application. Redis Cache provides OpenTelemetry tracing and metrics out of the box which the `cacheotel` pacakge. The `cacheotel` package provides wrapped versions of the `Cache` and `TypedCache` types that automatically instrument the calls with OpenTelemetry. When used in combination with tracing/metrics on the Redis client itself, this can provide a very rich observability story. Specifically, you can see how long the client takes to get a response from Redis, and how long it takes to handle encoding/compression.

## Features

* Save/Load any data structure that can be represented as bytes/string
* Flexible serialization options 
* Flexible compression options
* Tracing/Metrics via OpenTelemetry

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