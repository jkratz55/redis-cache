package cache

import (
	"context"
)

// Hooks are callbacks invoked by Cache and TypedCache so cache operations can be
// instrumented, for example to record cache hits and misses or the size of cached
// values. The cacheotel package records them as OpenTelemetry metrics, see
// cacheotel.NewHooks. Hooks are configured with the Instrumentation option.
//
// Every field is optional. The callbacks are invoked synchronously by the goroutine
// performing the operation, so they must return quickly, must not block, and must be
// safe for concurrent use. A panic in a callback is recovered and ignored, so
// instrumentation can't fail a cache operation.
type Hooks struct {
	// OnLookup is invoked after keys are read from Redis with the number of keys
	// that were found (hits) and not found (misses). Get, GetAndExpire and
	// GetAndDelete report a single hit or miss, MGet and MGetMap report every
	// requested key. A key that was found but can't be decoded counts as a hit, its
	// decode error is reported to OnDecode. OnLookup isn't invoked if the Redis
	// command fails.
	OnLookup func(ctx context.Context, hits, misses int)

	// OnEncode is invoked after a value is encoded to be written to Redis, with the
	// size of the serialized value before compression and the size of the value
	// stored in Redis, which is compressed and includes the format header. OnEncode
	// isn't invoked if the value can't be encoded.
	OnEncode func(ctx context.Context, serializedSize, storedSize int)

	// OnDecode is invoked after a value read from Redis is decoded, with the size of
	// the value stored in Redis and the error if it couldn't be decoded. The error
	// wraps ErrUnsupportedFormat if the value was written with a Serializer or
	// CompressionCodec that isn't registered, and ErrMaxDecodedSizeExceeded if a
	// built-in CompressionCodec rejected it for exceeding its size limit.
	OnDecode func(ctx context.Context, storedSize int, err error)

	// OnSingleFlight is invoked by ReadThroughSingleFlight after a cache miss, with
	// shared set to true if the caller joined a load already in flight instead of
	// starting a new one.
	OnSingleFlight func(ctx context.Context, shared bool)
}

func (h *Hooks) lookup(ctx context.Context, hits, misses int) {
	if h.OnLookup != nil {
		safeCall(func() { h.OnLookup(ctx, hits, misses) })
	}
}

func (h *Hooks) encoded(ctx context.Context, serializedSize, storedSize int) {
	if h.OnEncode != nil {
		safeCall(func() { h.OnEncode(ctx, serializedSize, storedSize) })
	}
}

func (h *Hooks) decoded(ctx context.Context, storedSize int, err error) {
	if h.OnDecode != nil {
		safeCall(func() { h.OnDecode(ctx, storedSize, err) })
	}
}

func (h *Hooks) singleFlight(ctx context.Context, shared bool) {
	if h.OnSingleFlight != nil {
		safeCall(func() { h.OnSingleFlight(ctx, shared) })
	}
}
