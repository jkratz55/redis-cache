# AGENTS.md - Developer & Agent Guide for `redis-cache`

This document serves as the operational manual and architectural guide for AI agents and developers working in the `redis-cache` codebase.

---

## 1. Project Overview & Architecture

`redis-cache` (`github.com/jkratz55/redis-cache/v2`) is a thin, high-performance abstraction layer built on top of the official Go Redis client (`github.com/redis/go-redis/v9`). It simplifies storing and retrieving structured Go data in Redis by automating serialization, compression, pipelined batching, metrics, and tracing.

### Key Capabilities
- **Dual Cache APIs**:
  - `Cache`: Dynamic, pointer-based cache supporting multiple types across a single instance.
  - `TypedCache[T]`: Generic, compile-time type-safe cache dedicated to a single type `T`.
- **Pluggable Serialization**: MessagePack (default via `vmihailenco/msgpack/v5`), JSON (`encoding/json/v2`), and Protobuf (via `cacheproto`).
- **Pluggable Compression**: Codecs for Deflate/Flate, Gzip, LZ4, S2, Snappy, and Zstandard.
- **Pipelined Batching**: Optional transparent batch chunking for `MGet` and `MGetMap` operations via `BatchMultiGets(batchSize)`.
- **Cache Patterns**: Built-in support for Read-Through (with optional single-flight thundering-herd protection) and Write-Through (synchronous or asynchronous).
- **High Availability**: Built-in `FailoverClient` providing automatic failover and recovery across primary and secondary Redis instances.
- **Observability**: OpenTelemetry metrics for serialization and compression via the `cacheotel` package. Redis command tracing and metrics come from instrumenting the Redis client itself (e.g. `redisotel`).

---

## 2. Repository Layout

```
.
├── cache.go              # Core Cache implementation, RedisClient interface, sentinel errors, and constants
├── cache_test.go         # Unit tests for Cache using miniredis
├── typedcache.go         # TypedCache[T] generic implementation
├── typedcache_test.go    # Unit tests for TypedCache[T]
├── serialization.go      # Serializer interface, JsonSerializer, MessagePackSerializer
├── codec.go              # CompressionCodec interface and nopCodec default
├── format.go             # Value header, format IDs, and the serializer/codec registry
├── format_test.go        # Format tests (external cache_test package, uses the built-in codecs)
├── export_test.go        # Exposes unexported identifiers to external tests (contains no tests)
├── option.go             # Functional options for Cache and TypedCache configuration
├── option_test.go        # Unit tests for options
├── readthrough.go        # ReadThrough and ReadThroughSingleFlight implementations
├── readthrough_test.go   # Unit tests for read-through patterns
├── writethrough.go       # WriteThrough implementation and WriteThroughError
├── writethrough_test.go  # Unit tests for write-through patterns
├── failover.go           # FailoverClient for primary/secondary Redis redundancy
├── failover_test.go      # Unit tests for FailoverClient
├── logger.go             # Logger interface and SlogLogger adapter
├── errors.go             # Retryable error interfaces and helpers (IsRetryable)
├── errors_test.go        # Unit tests for error helpers
├── version.go            # Library version string (v2.0.0)
├── doc.go                # Package documentation
├── cacheproto/           # Protobuf serializer (ProtobufSerializer)
│   ├── proto.go
│   └── proto_test.go
├── cacheotel/            # OpenTelemetry instrumentation package
│   ├── serialization.go  # InstrumentedSerializer
│   ├── compression.go    # InstrumentedCompressionCodec
│   ├── options.go        # Telemetry configuration options
│   └── otel.go           # Instrumentation scope name
├── compression/          # Compression codec implementations
│   ├── flate/            # Deflate/Flate codec
│   ├── gzip/             # Gzip codec
│   ├── lz4/              # LZ4 codec
│   ├── s2/               # S2 codec
│   ├── snappy/           # Snappy codec
│   └── zstd/             # Zstandard codec
├── examples/             # Runnable examples demonstrating various configurations
├── CONTRIBUTING.adoc     # Contribution guidelines
├── RELEASE_NOTES.md      # Changelog and migration notes
├── go.mod                # Module definitions and dependencies
└── go.sum                # Checksums
```

---

## 3. Core Abstractions & APIs

### 3.1 `Cache` vs `TypedCache[T]`
| Aspect | `Cache` | `TypedCache[T]` |
|---|---|---|
| **Instantiation** | `cache.New(client, opts...)` | `cache.NewTyped[T](client, opts...)` |
| **Type Handling** | Accepts any pointer destination `v any` per call | Parameterized to a single type `T` (supports values or pointers) |
| **Get Signature** | `Get(ctx, key, &dest) error` | `Get(ctx, key) (T, error)` |
| **MGet Signature** | Package-level function or method returning `MultiResult` | Method returning `[]T, error` or `MGetMap` returning `MultiResult[T]` |
| **Protobuf Fit** | Requires pointer handling per call | Optimal when instantiated as `*TypedCache[*MyProtoMsg]` |

Both types require initialization via their constructor functions (`New` / `NewTyped`); their zero values are not usable. Both panic if passed a `nil` `RedisClient`.

### 3.2 Configuration Options
Both `Cache` and `TypedCache` accept functional options implementing `Option func(c configurable)`:
- `cache.Serialization(s Serializer)`: Sets serialization strategy (default: `MessagePackSerializer`).
- `cache.Compression(c CompressionCodec)`: Sets compression codec (default: `nopCodec`).
- `cache.BatchMultiGets(batchSize int)`: Configures automatic chunking of large multi-key get commands via Redis pipelines.

### 3.3 Serialization (`Serializer`)
Defined in `serialization.go`:
```go
type Serializer interface {
    Marshal(any) ([]byte, error)
    Unmarshal([]byte, any) error
}
```
- `MessagePackSerializer`: Fast binary serialization using MessagePack.
- `JsonSerializer`: Standard JSON serialization.
- `cacheproto.ProtobufSerializer`: Serializes types implementing `google.golang.org/protobuf/proto.Message`.

Serializers whose `Unmarshal` tolerates input it didn't produce (e.g. Protobuf keeps unknown fields) should also implement the optional `StrictUnmarshaler` interface. It is used when a value with a format header fails to decode and is retried as a headerless legacy value, so corrupt values aren't mistaken for legacy ones. Wrappers that don't alter the bytes (e.g. the `cacheotel` wrappers) must forward the wrapped component's format ID, implement `Unwrap()`, and forward `UnmarshalStrict`.

### 3.4 Compression (`CompressionCodec`)
Defined in `codec.go`:
```go
type CompressionCodec interface {
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
}
```
Available implementations are located under `compression/<algorithm>/` (e.g., `compression/gzip.New()`, `compression/zstd.New()`, etc.).

### 3.5 Read-Through & Write-Through
- **`ReadThrough[T]`**: Synchronous fetch from Redis. On cache miss, executes `LoaderFunc[T]`, caches the result, and returns. Redis errors do not fail the call (invokes `OnCacheError` hook).
- **`ReadThroughSingleFlight[T]`**: Deduplicates concurrent loads of the same cache key, preventing duplicate backend queries during cache stampedes. The shared load is canceled once no caller is waiting for it anymore.
- **`WriteThrough[T]`**: Updates system of record via `WriteThroughWriter[T]`, then updates Redis synchronously or asynchronously (`UpdateCacheAsync: true`). Returns `*WriteThroughError` allowing callers to verify if the primary source succeeded despite cache failures (`err.SourceUpdated`).

### 3.6 Failover Client (`FailoverClient`)
Wraps a primary and secondary `RedisClient`. Conducts periodic health checks (`Ping`) in a background goroutine. Automatically switches traffic to secondary upon `FailureThreshold` consecutive check failures, and recovers back to primary after `SuccessThreshold` consecutive successes.

### 3.7 OpenTelemetry Instrumentation (`cacheotel`)
Wraps `Serializer` and `CompressionCodec` instances (`NewInstrumentedSerializer`, `NewInstrumentedCompressionCodec`) to record:
- Histograms: `cache.serialization.duration` and `cache.compression.duration` with attributes `operation` and `success`.
- Counters: `cache.serialization.errors` and `cache.compression.errors` with attribute `operation`.

The wrappers forward the format ID of the component they wrap, so values they write are readable by uninstrumented instances.

---

## 4. Development Workflow & Tooling

### Prerequisites
- **Go**: Version 1.27+ (configured in `go.mod`).
- **Redis**: Redis 7+ compatibility target.

### Standard Commands
```bash
# Verify dependencies
go mod verify

# Build all packages and examples
go build ./...

# Run static analysis
go vet ./...

# Run full test suite with race detector
go test -race ./...

# Run specific package tests
go test -v -race ./compression/zstd
go test -v -race .
```

### Testing Guidelines
- **Unit Testing**: Unit tests should not depend on external Redis instances. Use `github.com/alicebob/miniredis/v2` to spin up an in-memory mock Redis server within tests:
  ```go
  server, err := miniredis.Run()
  if err != nil {
      t.Fatal(err)
  }
  defer server.Close()

  client := redis.NewClient(&redis.Options{Addr: server.Addr()})
  defer client.Close()
  ```
- **Assertions**: Standardize on `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require`.
- **Error Conditions**: Test cache miss (`ErrKeyNotFound`), corrupt data unmarshaling errors, nil client panics, and TTL expiration behavior.
- **Test File Layout**: Test files mirror implementation files. Every test for `foo.go` lives in `foo_test.go`, e.g. `format.go` → `format_test.go`. Don't add files such as `foo_external_test.go` or `foo_legacy_test.go`.
  - A file has a single package clause, so if any test for `foo.go` needs the external `cache_test` package, the whole `foo_test.go` uses `package cache_test`. This is required for tests that import `compression/*` or `cacheproto`, since those packages import `cache`, and for tests that need types declared outside the module's packages.
  - `export_test.go` is the only exception. It contains no tests and only exposes unexported identifiers to external test files.

---

## 5. Coding Standards & Agent Best Practices

When modifying or extending this codebase, adhere to the following rules:

1. **Context Propagation**:
   - Every cache operation must accept `ctx context.Context` as its first parameter and pass it to underlying Redis calls or tracer spans.

2. **Concurrency Safety**:
   - All `Serializer` and `CompressionCodec` implementations must be safe for concurrent use across multiple goroutines.
   - Cache instances (`Cache`, `TypedCache`, `FailoverClient`) must remain completely thread-safe.

3. **API Parity**:
   - When introducing new caching operations (e.g., `GetAndExpire`, `SetIfAbsent`), ensure parity across:
     - `Cache` in `cache.go`
     - `TypedCache` in `typedcache.go`
     - Unit tests in `cache_test.go` and `typedcache_test.go`

4. **Error Handling Conventions**:
   - Sentinel error `ErrKeyNotFound` is returned when a requested key does not exist. Do not leak raw `redis.Nil`.
   - Wrap internal Redis and serialization errors using `fmt.Errorf("redis: %w", err)` or `fmt.Errorf("marshal value: %w", err)` to preserve unwrap chains.

5. **Panics vs. Returned Errors**:
   - Constructors (`New`, `NewTyped`, `Serialization(nil)`, `Compression(nil)`) should panic on invalid setup/nil arguments to fail fast at startup.
   - Runtime operational methods (like `Get`, `Set`, `Delete`) must return `error` and never panic.

6. **Pointer Receiver Consistency**:
   - Struct methods modifying or implementing core interfaces should use consistent receiver semantics (pointer receivers for mutable state or large structs, value receivers only for stateless value types).
