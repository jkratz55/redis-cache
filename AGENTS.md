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
- **Pipelined Batching**: Optional transparent batch chunking for `MGet` operations via `BatchMultiGets(batchSize)`.
- **Cache Patterns**: Built-in support for Read-Through (with optional single-flight thundering-herd protection) and Write-Through (synchronous or asynchronous).
- **High Availability**: Built-in `FailoverClient` providing automatic failover and recovery across primary and secondary Redis instances.
- **Observability**: `Hooks` (configured with the `Instrumentation` option) report cache hits and misses, value sizes, decode errors, and single-flight loads. The `cacheotel` package records them, serialization and compression durations, and the active `FailoverClient` side as OpenTelemetry metrics. Redis command tracing and metrics come from instrumenting the Redis client itself (e.g. `redisotel`).

---

## 2. Repository Layout

```
.
├── cache.go              # Core Cache implementation, RedisClient interface, sentinel errors, and constants
├── cache_test.go         # Unit tests for Cache using miniredis
├── typedcache.go         # TypedCache[T] generic implementation
├── typedcache_test.go    # Unit tests for TypedCache[T]
├── serialization.go      # Serializer interface, optional MarshalAppender/StrictUnmarshaler, JsonSerializer, MessagePackSerializer
├── serialization_test.go # Unit tests for serializers
├── codec.go              # CompressionCodec interface, optional CompressAppender, ErrMaxDecodedSizeExceeded, nopCodec default
├── codec_test.go         # Unit tests for codec helpers
├── format.go             # Value header, format IDs, and the serializer/codec registry
├── format_test.go        # Format tests (external cache_test package, uses the built-in codecs)
├── export_test.go        # Exposes unexported identifiers to external tests (contains no tests)
├── option.go             # Functional options for Cache and TypedCache configuration
├── option_test.go        # Unit tests for options
├── hooks.go              # Hooks invoked by Cache and TypedCache for instrumentation
├── hooks_test.go         # Unit tests for hooks
├── readthrough.go        # ReadThrough and ReadThroughSingleFlight implementations
├── readthrough_test.go   # Unit tests for read-through patterns
├── writethrough.go       # WriteThrough implementation and WriteThroughError
├── writethrough_test.go  # Unit tests for write-through patterns
├── failover.go           # FailoverClient for primary/secondary Redis redundancy
├── failover_test.go      # Unit tests for FailoverClient
├── errors.go             # RetryableError, IsRetryable, and MultiKeyError
├── errors_test.go        # Unit tests for error helpers
├── version.go            # Library version string (v2.0.0)
├── doc.go                # Package documentation
├── cacheproto/           # Protobuf serializer (ProtobufSerializer)
│   ├── proto.go
│   └── proto_test.go
├── cacheotel/            # OpenTelemetry instrumentation package
│   ├── serialization.go  # InstrumentedSerializer
│   ├── compression.go    # InstrumentedCompressionCodec
│   ├── hooks.go          # NewHooks: cache.Hooks recording metrics
│   ├── failover.go       # ObserveFailoverClient: active FailoverClient side gauge
│   ├── options.go        # Telemetry configuration options
│   └── otel.go           # Package doc, instrumentation scope name, shared helpers
├── cmd/rediscache-inspect/ # CLI printing the encoding, compression, and data of stored values
│   ├── main.go           # Flags, Redis access, and exit status
│   ├── inspect.go        # Header parsing, decompression, and detection of values without a header
│   ├── msgpack.go        # MessagePack to JSON, keeping the order of map keys
│   ├── proto.go          # Protobuf without a schema, like protoc --decode_raw
│   └── render.go         # Text and JSON output
├── compression/          # Compression codec implementations
│   ├── internal/limit/   # Decompressed size limits shared by the codecs
│   ├── flate/            # Deflate/Flate codec
│   ├── gzip/             # Gzip codec
│   ├── lz4/              # LZ4 codec
│   ├── s2/               # S2 codec
│   ├── snappy/           # Snappy codec
│   └── zstd/             # Zstandard codec
├── internal/benchmarks/  # Benchmarks comparing every serializer, codec, and TypedCache with them
│   ├── benchmarks.go       # Package doc and the generated Order payloads
│   ├── benchmarks_test.go  # BenchmarkSerializer, BenchmarkCodec, BenchmarkTypedCache, TestPayloads
│   ├── results.txt         # Raw results published in the README
│   └── benchpb/          # Protobuf mirror of Order (generated, see go:generate in benchmarks.go)
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
| **MGet Signature** | Package-level function `MGet[T]` returning `MultiResult[T]` | Method returning `MultiResult[T]` |
| **Protobuf Fit** | Requires pointer handling per call | Optimal when instantiated as `*TypedCache[*MyProtoMsg]` |

Both types require initialization via their constructor functions (`New` / `NewTyped`); their zero values are not usable. Both panic if passed a `nil` `RedisClient`.

`MultiResult[T]` mirrors the reply to `MGET`: an entry for every requested key, in request order, including missing and duplicate keys. Every key is found, missing, or failed to decode, in which case its error is in the `*MultiKeyError` returned with the result. `At(i)` reads by position, `All` and `Values` return the found entries in request order, `Map` returns them by key, and `Missing` returns the keys that don't exist. `Missing` excludes keys that failed to decode, so loading and caching the missing keys can't overwrite values written in a format the instance can't read. Found values are stored compactly with an index per key, so `Values` doesn't copy; keep it that way.

### 3.2 Configuration Options
Both `Cache` and `TypedCache` accept functional options implementing `Option func(c configurable)`:
- `cache.Serialization(s Serializer)`: Sets serialization strategy (default: `MessagePackSerializer`).
- `cache.Compression(c CompressionCodec)`: Sets compression codec (default: `nopCodec`).
- `cache.BatchMultiGets(batchSize int)`: Configures automatic chunking of large multi-key get commands via Redis pipelines. Keys are split by count, not by Redis Cluster hash slot.
- `cache.Instrumentation(h Hooks)`: Sets the `Hooks` invoked on lookups, encodes, decodes, and single-flight loads. Hooks run synchronously in the caller's goroutine and their panics are recovered.

### 3.3 Serialization (`Serializer`)
Defined in `serialization.go`:
```go
type Serializer interface {
    Marshal(any) ([]byte, error)
    Unmarshal([]byte, any) error
}
```
- `MessagePackSerializer`: Fast binary serialization using MessagePack.
- `cacheproto.ProtobufSerializer`: Serializes types implementing `google.golang.org/protobuf/proto.Message`.

- `JsonSerializer`: JSON serialization with `encoding/json/v2` default options, which differ from `encoding/json` used by v1.x and the release candidates (e.g. `time.Duration` isn't supported without a format option and names are matched case-sensitively). This is a documented breaking change; teams needing `encoding/json` semantics bring their own serializer with a custom format ID.

Serializers whose `Unmarshal` tolerates input it didn't produce (e.g. Protobuf keeps unknown fields) should also implement the optional `StrictUnmarshaler` interface. It is used when a value with a format header fails to decode and is retried as a headerless legacy value, so corrupt values aren't mistaken for legacy ones. Serializers can implement the optional `MarshalAppender` interface (`AppendMarshal`) so values are serialized directly after the format header without a copy. Wrappers that don't alter the bytes (e.g. the `cacheotel` wrappers) must forward the wrapped component's format ID, implement `Unwrap()`, forward `UnmarshalStrict`, and should forward `AppendMarshal`/`AppendCompress`.

### 3.4 Compression (`CompressionCodec`)
Defined in `codec.go`:
```go
type CompressionCodec interface {
    Compress(data []byte) ([]byte, error)
    Decompress(data []byte) ([]byte, error)
}
```
Available implementations are located under `compression/<algorithm>/`: `flate.NewCodec(level, opts...)`, `gzip.NewCodec(level, opts...)`, `lz4.NewCodec(opts...)`, `s2.NewCodec(opts...)`, `snappy.NewCodec(opts...)`, and `zstd.NewCodec(level, opts...)`, which also returns an error. Every built-in codec:
- Accepts `WithMaxDecodedSize(n)`. There is no limit by default; values exceeding a limit fail with an error wrapping `cache.ErrMaxDecodedSizeExceeded`. Streaming codecs enforce the limit while decompressing through `compression/internal/limit`, block codecs (S2, Snappy) check the declared size before allocating.
- Implements the optional `CompressAppender` interface (`AppendCompress`), so values are compressed directly after the format header without a copy.

The lz4 codec writes 64 KiB blocks: LZ4's default 4 MiB blocks make every compression and decompression after a garbage collection allocate more than 8 MiB. The gzip codec sizes its buffer from the trailer's declared size, bounded to 8 times the compressed size and 1 MiB, since the trailer is only verified at the end. Size hints taken from a value must always be bounded like this.

### 3.5 Read-Through & Write-Through
- **`ReadThrough[T]`**: Synchronous fetch from Redis. On cache miss, executes `LoaderFunc[T]`, caches the result, and returns. Redis errors do not fail the call (invokes `OnCacheError` hook). A cached value that can't be decoded is reported with `RedisOperationDecode` and loaded again. The loaded value replaces corrupt values, but not values in an unregistered format (`ErrUnsupportedFormat`) unless `OverwriteUnsupportedFormat` is set, so services migrating to a new format don't keep replacing each other's values. Both read-through functions share this logic in `readThroughGet`.
- **`ReadThroughSingleFlight[T]`**: Deduplicates concurrent loads of the same cache key, preventing duplicate backend queries during cache stampedes. Loads are only shared per `*Cache` and per type `T` (each `Cache` owns its `flightGroup`, keyed by `flightKey{type, key}`). The shared load is canceled once no caller is waiting for it anymore. Every call reads Redis once; the shared load doesn't read it again before loading.
- **`WriteThrough[T]`**: Updates system of record via `WriteThroughWriter[T]`, then updates Redis synchronously or asynchronously (`UpdateCacheAsync: true`). If the cache update fails, the key is deleted so the previous value isn't served; this is best effort since the delete can fail too. The update and the delete share `CacheWriteTimeout`. Returns `*WriteThroughError` allowing callers to verify if the primary source succeeded despite cache failures (`err.SourceUpdated`).
- User callbacks invoked from goroutines started by the library (async updates, the shared single-flight load) are wrapped with `safeCall`, so a panicking callback can't crash the process.

### 3.6 Failover Client (`FailoverClient`)
Wraps a primary and secondary `RedisClient`. Conducts periodic health checks (`PING` by default, customizable with `FailoverOpts.HealthCheck`) in a background goroutine. Automatically switches traffic to secondary upon `FailureThreshold` consecutive check failures, and recovers back to primary after `SuccessThreshold` consecutive successes. The active client is a single atomic pointer, which `IsPrimary`, `IsSecondary`, and `Active` all derive from. The monitor waits for health checks at most `CheckTimeout` plus `healthCheckGrace`, counts a late check as failed, and doesn't start another check of a client while one is running, so a hung check can't stop monitoring or block `Close`.

Commands are only sent to the active client. The primary and secondary are expected to be active-active replicated (e.g. Redis Enterprise Active-Active); without replication, values served after switching are stale. `FailoverClient` owns both clients and `Close` closes them.

### 3.7 OpenTelemetry Instrumentation (`cacheotel`)
Wraps `Serializer` and `CompressionCodec` instances (`NewInstrumentedSerializer`, `NewInstrumentedCompressionCodec`) to record:
- Histograms: `cache.serialization.duration` and `cache.compression.duration` with attributes `cache.serializer`/`cache.codec`, `operation`, and `success`. Default buckets range from 10µs to 50ms.
- Counters: `cache.serialization.errors` and `cache.compression.errors` with attributes `cache.serializer`/`cache.codec` and `operation`.

`NewHooks` returns `cache.Hooks` recording `cache.lookups`, `cache.value.size`, `cache.value.serialized_size`, `cache.decode.errors`, and `cache.singleflight.calls` with the context of the cache operation. `ObserveFailoverClient` registers the `cache.failover.active` gauge. `WithAttributes` adds attributes, such as a cache name, to every measurement. Attribute sets are computed once at construction so recording doesn't allocate.

The wrappers forward the format ID of the component they wrap, so values they write are readable by uninstrumented instances.

### 3.8 Inspection CLI (`cmd/rediscache-inspect`)
Fetches values and prints their encoding, compression, TTL, and decoded data without the Go types: MessagePack and JSON as JSON (map key order is kept), Protobuf like `protoc --decode_raw`, anything else as text or a hex dump. It redeclares the header constants and names the built-in formats in `inspect.go`, so add new built-in serializers and codecs there. `TestInspect_BuiltInFormats` encodes values through the library, so it fails if the header and the CLI drift apart. Never decode untrusted MessagePack maps or arrays with `DecodeInterface`: msgpack v5.4.1 allocates their declared length up front.

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

# Inspect stored values
go run ./cmd/rediscache-inspect -url redis://localhost:6379/0 user:42

# Fuzz decoding of untrusted values
go test -run '^$' -fuzz '^FuzzDecode$' -fuzztime 1m .

# Benchmark encoding and decoding with every codec
go test -run '^$' -bench 'Encode|Decode' -benchmem .

# Compare every serializer and codec (results are published in the README)
go test -run '^$' -bench . -benchmem -count 6 -benchtime 500ms ./internal/benchmarks > internal/benchmarks/results.txt
```

When a change affects serialization, compression, or encoding performance, rerun the `internal/benchmarks` suite, compare it with `internal/benchmarks/results.txt` (e.g. with `benchstat`), and update the results file and the README Performance section if the numbers change meaningfully. Regenerate `benchpb` with `go generate ./internal/benchmarks` (requires `protoc` and `protoc-gen-go`) after changing `order.proto`, and keep `Order.Proto` in sync with it.

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
- **Assertions**: Standardize on `github.com/stretchr/testify/assert` and `github.com/stretchr/testify/require`. Dependencies are vendored and `require` isn't vendored yet, so use `assert` unless `vendor/` is updated.
- **Error Conditions**: Test cache miss (`ErrKeyNotFound`), corrupt data unmarshaling errors, nil client panics, and TTL expiration behavior.
- **Test File Layout**: Test files mirror implementation files. Every test for `foo.go` lives in `foo_test.go`, e.g. `format.go` → `format_test.go`. Don't add files such as `foo_external_test.go` or `foo_legacy_test.go`.
  - A file has a single package clause, so if any test for `foo.go` needs the external `cache_test` package, the whole `foo_test.go` uses `package cache_test`. This is required for tests that import `compression/*` or `cacheproto`, since those packages import `cache`, and for tests that need types declared outside the module's packages.
  - `export_test.go` is the only exception. It contains no tests and only exposes unexported identifiers to external test files.
  - Benchmarks comparing implementations across packages live in `internal/benchmarks`, not next to the implementations.

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
   - Multi-key reads (`MGet`) return a `MultiResult` with the values that could be decoded along with a `*MultiKeyError` holding the error of each key that couldn't. A Redis error fails the whole call with an empty `MultiResult`.

5. **Panics vs. Returned Errors**:
   - Constructors (`New`, `NewTyped`, `Serialization(nil)`, `Compression(nil)`) should panic on invalid setup/nil arguments to fail fast at startup.
   - Runtime operational methods (like `Get`, `Set`, `Delete`) must return `error` and never panic.

6. **Pointer Receiver Consistency**:
   - Struct methods modifying or implementing core interfaces should use consistent receiver semantics (pointer receivers for mutable state or large structs, value receivers only for stateless value types).
