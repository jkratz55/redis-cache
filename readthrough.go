package cache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

// flightGroup deduplicates concurrent calls for the same key, like
// singleflight.Group. Unlike singleflight.Group, a call is canceled and forgotten
// once no caller is waiting for its result anymore, so a call stuck on a hung
// dependency doesn't capture every later caller.
//
// Every Cache has its own flightGroup, so callers using different caches, which
// may be backed by different Redis instances, never share a call.
type flightGroup struct {
	mu    sync.Mutex
	calls map[flightKey]*flightCall
}

// flightKey identifies a call of a flightGroup. Calls are keyed by the type of the
// loaded value as well as the cache key, so callers loading different types for
// the same key never share a result.
type flightKey struct {
	typ reflect.Type
	key string
}

// flightCall is an in-flight or completed call of a flightGroup. val and err are
// set before done is closed.
type flightCall struct {
	done    chan struct{}
	val     any
	err     error
	waiters int
	cancel  context.CancelFunc
}

// join returns the in-flight call for key, or starts fn in a new goroutine if
// there is none. shared reports whether an in-flight call was joined. The context
// passed to fn keeps the values of ctx, but it is only canceled once every caller
// that joined the call has left it. Every call to join must be followed by a call
// to leave.
func (g *flightGroup) join(ctx context.Context, key flightKey, fn func(ctx context.Context) (any, error)) (c *flightCall, shared bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if c, ok := g.calls[key]; ok {
		c.waiters++
		return c, true
	}

	callCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	c = &flightCall{done: make(chan struct{}), waiters: 1, cancel: cancel}
	if g.calls == nil {
		g.calls = make(map[flightKey]*flightCall)
	}
	g.calls[key] = c

	go func() {
		// The call is finished from a deferred function so the waiting callers are
		// released even if fn exits the goroutine with runtime.Goexit.
		returned := false
		defer func() {
			if !returned {
				c.val, c.err = nil, errors.New("loader func did not return")
			}
			cancel()
			g.forget(key, c)
			close(c.done)
		}()
		c.val, c.err = fn(callCtx)
		returned = true
	}()

	return c, false
}

// leave removes a caller from c. Once no caller is waiting for c anymore, c is
// canceled and forgotten, so the next caller for the key starts a new call.
func (g *flightGroup) leave(key flightKey, c *flightCall) {
	g.mu.Lock()
	defer g.mu.Unlock()

	c.waiters--
	if c.waiters > 0 {
		return
	}
	// Canceling and forgetting under the lock ensures no caller joins c after it
	// has been abandoned.
	c.cancel()
	g.forgetLocked(key, c)
}

// forget removes c from the group if it is still the call for key.
func (g *flightGroup) forget(key flightKey, c *flightCall) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.forgetLocked(key, c)
}

func (g *flightGroup) forgetLocked(key flightKey, c *flightCall) {
	if g.calls[key] == c {
		delete(g.calls, key)
	}
}

// RedisOperation represents a Redis operation/command
type RedisOperation string

const (
	// RedisOperationGET represents a Redis GET operation
	RedisOperationGET RedisOperation = "GET"
	// RedisOperationSET represents a Redis SET operation
	RedisOperationSET RedisOperation = "SET"
	// RedisOperationDecode represents decoding a value read with GET. It isn't a Redis
	// command: OnCacheError reports it when a value exists but can't be decoded, for
	// example because it's corrupt or was written in a format that isn't registered.
	RedisOperationDecode RedisOperation = "DECODE"

	defaultCacheReadTimeout  = 500 * time.Millisecond
	defaultCacheWriteTimeout = 500 * time.Millisecond
)

// LoaderFunc is a function that retrieves/loads data from a system of record, such as a database.
type LoaderFunc[T any] func(ctx context.Context) (T, error)

// ReadThroughOptions contains options for ReadThrough.
type ReadThroughOptions struct {
	// CacheReadTimeout specifies the maximum duration to wait for a response while reading from the
	// cache.
	//
	// Default is 500ms.
	CacheReadTimeout time.Duration

	// CacheWriteTimeout specifies the maximum duration to wait for a response while writing to the
	// cache.
	//
	// Default is 500ms.
	CacheWriteTimeout time.Duration

	// TTL specifies the time-to-live for cached values.
	//
	// Default is 0, which means the value will never expire.
	TTL time.Duration

	// OnCacheMiss is invoked when a cache miss occurs.
	//
	// Default is a no-op.
	OnCacheMiss func(key string)

	// OnCacheHit is invoked when a cache hit occurs.
	//
	// Default is a no-op.
	OnCacheHit func(key string)

	// OnCacheError is invoked when an error occurs while reading or writing to the cache. A cached
	// value that can't be decoded is reported with RedisOperationDecode, so it can be told apart
	// from Redis failing.
	//
	// OnCacheError can be invoked from a goroutine started by ReadThrough or
	// ReadThroughSingleFlight, such as the asynchronous cache update when UpdateAsync is true.
	// A panic in OnCacheError is recovered and ignored in that case, so it can't crash the
	// process.
	//
	// Default is a no-op.
	OnCacheError func(key string, op RedisOperation, err error)

	// UpdateAsync specifies whether the cache should be updated asynchronously.
	//
	// If true, the cache will be updated asynchronously in a background goroutine and the function
	// will return immediately. Default is false, which will block until the cache is updated or fails.
	//
	// The asynchronous update keeps the values of the caller's context, such as trace information,
	// but isn't canceled when the caller's context is canceled.
	UpdateAsync bool

	// LoaderTimeout bounds the LoaderFunc and cache update shared by all callers in
	// ReadThroughSingleFlight. It is ignored by ReadThrough.
	//
	// ReadThroughSingleFlight runs the shared work with a context that keeps the values of the
	// first caller's context but isn't canceled with it, so one caller canceling doesn't fail the
	// others. The shared work is canceled once every waiting caller's context is done. If
	// LoaderTimeout is 0 (the default) there is no other timeout.
	LoaderTimeout time.Duration

	// OverwriteUnsupportedFormat stores the loaded value over a cached value written in a format
	// that isn't registered on the Cache, such as a Serializer or CompressionCodec that a newer
	// service started writing before this one registered it with DecodeSerializers or
	// DecodeCompression.
	//
	// By default such a value is left in place and the loaded value is returned without being
	// cached, so services using different formats don't keep replacing each other's values while
	// they are upgraded. Values that can't be decoded for any other reason, such as corrupt values,
	// are always overwritten, which repairs the cache.
	OverwriteUnsupportedFormat bool
}

// safeCallbacks returns a copy of o whose OnCacheError recovers panics. It is used
// for the work ReadThroughSingleFlight runs in its own goroutine, where a panicking
// callback would crash the process. OnCacheHit and OnCacheMiss are only invoked by
// the caller's goroutine.
func (o ReadThroughOptions) safeCallbacks() ReadThroughOptions {
	onCacheError := o.OnCacheError
	o.OnCacheError = func(key string, op RedisOperation, err error) {
		safeCall(func() { onCacheError(key, op, err) })
	}
	return o
}

func (o *ReadThroughOptions) init() {
	if o.CacheReadTimeout == 0 {
		o.CacheReadTimeout = defaultCacheReadTimeout
	}
	if o.CacheWriteTimeout == 0 {
		o.CacheWriteTimeout = defaultCacheWriteTimeout
	}
	if o.OnCacheMiss == nil {
		o.OnCacheMiss = func(key string) {}
	}
	if o.OnCacheHit == nil {
		o.OnCacheHit = func(key string) {}
	}
	if o.OnCacheError == nil {
		o.OnCacheError = func(key string, op RedisOperation, err error) {}
	}
}

// ReadThrough handle read-through caching. ReadThrough first attempts to retrieve the value from
// Redis by the key. If the value is found in Redis it is returned immediately. Otherwise, the
// LoaderFunc is invoked to retrieve the value from the source of truth. The value is then stored
// in Redis for future requests.
//
// It is important to note that ReadThrough does not guarantee the cache (Redis) will be updated.
// If the SET operation fails (including timeout), ReadThrough will still return the value with
// a nil error. However, since the value wasn't cached in Redis, the next request will invoke the
// LoaderFunc.
//
// Errors from Redis are not bubbled up to the caller through the error value returned by ReadThrough.
// However, hooks/callbacks are provided via ReadThroughOptions OnCacheError to allow for logging or
// instrumentation. Additionally, there are hooks for cache misses and hits via ReadThroughOptions.
//
// If the cached value can't be decoded, the error is reported to OnCacheError with RedisOperationDecode
// and the LoaderFunc is invoked. The loaded value replaces a corrupt value, but not a value written in a
// format that isn't registered on rdb, unless ReadThroughOptions.OverwriteUnsupportedFormat is set.
//
// Since all operations ReadThrough performs are synchronous, it is recommended to use short timeouts
// for Redis operations to avoid blocking the caller. The default for GET and SET operations is 500ms
// to be conservative.
//
// ReadThrough is susceptible to the thundering herd problem. If many concurrent requests are made for
// the same key, all of them may have a cache miss and invoke the LoaderFunc. This can be mitigated by
// using ReadThroughSingleFlight instead, which uses singleflight to prevent duplicate work.
func ReadThrough[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	opts ReadThroughOptions,
	loader LoaderFunc[T]) (T, error) {

	opts.init()

	result, found, store := readThroughGet[T](ctx, rdb, key, opts)
	if found {
		return result, nil
	}

	result, err := loader(ctx)
	if err != nil {
		return result, fmt.Errorf("loader func: %w", err)
	}

	if store {
		readThroughStore(ctx, rdb, key, result, opts)
	}

	return result, nil
}

// readThroughGet reads key from the cache and reports the outcome to the callbacks of
// opts. found reports whether the value was read and decoded. If it wasn't, store
// reports whether the loaded value should be cached: a value written in a format that
// isn't registered is only overwritten if OverwriteUnsupportedFormat is set.
func readThroughGet[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	opts ReadThroughOptions) (result T, found, store bool) {

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheReadTimeout)
	defer redisCancel()

	// Reading and decoding are separate steps, unlike Get, so a value that can't be
	// decoded is reported differently from Redis failing.
	data, err := readValue(redisCtx, rdb.formats, rdb.redis.Get(redisCtx, key))
	switch {
	case errors.Is(err, ErrKeyNotFound):
		opts.OnCacheMiss(key)
		return result, false, true
	case err != nil:
		opts.OnCacheError(key, RedisOperationGET, err)
		return result, false, true
	}

	if err := rdb.formats.decode(redisCtx, data, &result); err != nil {
		opts.OnCacheError(key, RedisOperationDecode, err)
		// A value in a format that isn't registered was most likely written by a newer
		// service. Replacing it would make that service and this one keep replacing
		// each other's values and loading from the source of truth.
		return result, false, opts.OverwriteUnsupportedFormat || !errors.Is(err, ErrUnsupportedFormat)
	}

	opts.OnCacheHit(key)
	return result, true, false
}

// readThroughStore stores the loaded value in the cache, either synchronously or in a
// background goroutine if UpdateAsync is enabled. Errors are reported to OnCacheError.
func readThroughStore[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	val T,
	opts ReadThroughOptions) {

	if opts.UpdateAsync {
		// Keep the context values, such as tracing information, but don't cancel
		// the update when the caller's context is canceled.
		asyncCtx := context.WithoutCancel(ctx)
		go func() {
			err := readThroughUpdate(asyncCtx, rdb, key, val, opts)
			if err != nil {
				// A panic in the callback would crash the process since this
				// goroutine was started by the library.
				safeCall(func() { opts.OnCacheError(key, RedisOperationSET, err) })
			}
		}()
		return
	}

	err := readThroughUpdate(ctx, rdb, key, val, opts)
	if err != nil {
		opts.OnCacheError(key, RedisOperationSET, err)
	}
}

func readThroughUpdate[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	val T,
	opts ReadThroughOptions) error {

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheWriteTimeout)
	defer redisCancel()

	// Set already wraps Redis errors.
	return rdb.Set(redisCtx, key, val, opts.TTL)
}

// ReadThroughSingleFlight handle read-through caching. ReadThroughSingleFlight first attempts to
// retrieve the value from Redis by the key. If the value is found in Redis it is returned immediately.
// Otherwise, the LoaderFunc is invoked to retrieve the value from the source of truth. The value is
// then stored in Redis for future requests.
//
// It is important to note that ReadThroughSingleFlight does not guarantee the cache (Redis) will be updated.
// If the SET operation fails (including timeout), ReadThroughSingleFlight will still return the value with
// a nil error. However, since the value wasn't cached in Redis, the next request will invoke the
// LoaderFunc.
//
// Errors from Redis are not bubbled up to the caller through the error value returned by ReadThroughSingleFlight.
// However, hooks/callbacks are provided via ReadThroughOptions OnCacheError to allow for logging or
// instrumentation. Additionally, there are hooks for cache misses and hits via ReadThroughOptions.
//
// If the cached value can't be decoded, the error is reported to OnCacheError with RedisOperationDecode
// and the LoaderFunc is invoked. The loaded value replaces a corrupt value, but not a value written in a
// format that isn't registered on rdb, unless ReadThroughOptions.OverwriteUnsupportedFormat is set. A
// shared load caches its value based on the lookup of the caller that started it.
//
// Since all operations ReadThroughSingleFlight performs are synchronous, it is recommended to use short
// timeouts for Redis operations to avoid blocking the caller. The default for GET and SET
// operations is 500ms to be conservative.
//
// ReadThroughSingleFlight is similar to ReadThrough, but it uses singleflight to prevent duplicate work.
// ReadThroughSingleFlight will only invoke the LoaderFunc once for a given key, and all other subsequent
// requests will wait for the first request to complete, and the result will be shared with all waiting
// callers. The result is shared regardless of the outcome, meaning if the operation results in an error,
// all waiting callers will error.
//
// Loads are only shared between callers using the same Cache and the same type T. Callers using a
// different Cache, or loading a different type for the same key, start their own load.
//
// The shared LoaderFunc and cache update run with a context that keeps the values of the first caller's
// context but isn't canceled with it, optionally bounded by ReadThroughOptions.LoaderTimeout. Each caller
// waits for the shared result until its own context is done, in which case the context error is returned
// to that caller only. Once no caller is waiting for the shared result anymore, the shared context is
// canceled and the next caller for the key starts a new load. A panic in the LoaderFunc is returned as
// an error to all waiting callers.
//
// OnCacheHit or OnCacheMiss is invoked exactly once per caller, based on the caller's own cache lookup.
// Every caller reads Redis once. The shared load doesn't read Redis again before invoking the LoaderFunc,
// so a caller that missed just before another load completed and stored the value starts a new load.
func ReadThroughSingleFlight[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	opts ReadThroughOptions,
	loader LoaderFunc[T]) (T, error) {

	opts.init()

	result, found, store := readThroughGet[T](ctx, rdb, key, opts)
	if found {
		return result, nil
	}

	// The shared work runs in its own goroutine, where a panicking callback would
	// crash the process.
	flightOpts := opts.safeCallbacks()

	// The shared work isn't canceled when the caller that started it goes away,
	// otherwise every waiting caller would fail with that caller's error. It is
	// canceled once no caller is waiting for it anymore.
	fk := flightKey{typ: reflect.TypeFor[T](), key: key}
	call, shared := rdb.flights.join(ctx, fk, func(flightCtx context.Context) (val any, err error) {
		// The call runs in its own goroutine, so an unrecovered panic would crash
		// the process instead of propagating to the caller.
		defer func() {
			if r := recover(); r != nil {
				val, err = nil, fmt.Errorf("loader func panic: %v", r)
			}
		}()

		loaderCtx := flightCtx
		if flightOpts.LoaderTimeout > 0 {
			var cancel context.CancelFunc
			loaderCtx, cancel = context.WithTimeout(flightCtx, flightOpts.LoaderTimeout)
			defer cancel()
		}

		// Redis isn't read again before loading: the caller that starts the load
		// just missed, and reading again would cost every miss another round trip
		// for the rare case of a load that completed in between.
		result, err := loader(loaderCtx)
		if err != nil {
			return result, fmt.Errorf("loader func: %w", err)
		}

		// Whether the value is cached depends on the lookup of the caller that
		// started the load.
		if store {
			readThroughStore(loaderCtx, rdb, key, result, flightOpts)
		}

		return result, nil
	})

	defer rdb.flights.leave(fk, call)
	rdb.formats.hooks.singleFlight(ctx, shared)

	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case <-call.done:
		if call.err != nil {
			var zero T
			return zero, fmt.Errorf("singleflight: %w", call.err)
		}
		// The result is always a T since calls are keyed by type, or nil if T is an
		// interface type and the loader returned a nil interface value.
		val, _ := call.val.(T)
		return val, nil
	}
}
