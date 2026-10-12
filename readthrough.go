package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/singleflight"
)

var (
	readThroughGroup = singleflight.Group{}
)

// RedisOperation represents a Redis operation/command
type RedisOperation string

const (
	// RedisOperationGET represents a Redis GET operation
	RedisOperationGET RedisOperation = "GET"
	// RedisOperationSET represents a Redis SET operation
	RedisOperationSET RedisOperation = "SET"

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

	// OnCacheError is invoked when an error occurs while reading or writing to the cache.
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
	// others. If LoaderTimeout is 0 (the default) there is no timeout, and a LoaderFunc that
	// doesn't return may outlive all callers.
	LoaderTimeout time.Duration
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

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheReadTimeout)
	defer redisCancel()

	var result T
	err := rdb.Get(redisCtx, key, &result)
	if err == nil {
		opts.OnCacheHit(key)
		return result, nil
	}

	if errors.Is(err, ErrKeyNotFound) {
		opts.OnCacheMiss(key)
	} else {
		opts.OnCacheError(key, RedisOperationGET, err)
	}

	result, err = loader(ctx)
	if err != nil {
		return result, fmt.Errorf("loader func: %w", err)
	}

	readThroughStore(ctx, rdb, key, result, opts)

	return result, nil
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
				opts.OnCacheError(key, RedisOperationSET, err)
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
// The shared LoaderFunc and cache update run with a context that keeps the values of the first caller's
// context but isn't canceled with it, optionally bounded by ReadThroughOptions.LoaderTimeout. Each caller
// waits for the shared result until its own context is done, in which case the context error is returned
// to that caller only. A panic in the LoaderFunc is returned as an error to all waiting callers.
//
// OnCacheHit or OnCacheMiss is invoked exactly once per caller, based on the caller's own cache lookup.
func ReadThroughSingleFlight[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	opts ReadThroughOptions,
	loader LoaderFunc[T]) (T, error) {

	opts.init()

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheReadTimeout)
	defer redisCancel()

	var result T
	err := rdb.Get(redisCtx, key, &result)
	if err == nil {
		opts.OnCacheHit(key)
		return result, nil
	}

	if errors.Is(err, ErrKeyNotFound) {
		opts.OnCacheMiss(key)
	} else {
		opts.OnCacheError(key, RedisOperationGET, err)
	}

	// The shared work must not be canceled when the caller that started it goes
	// away, otherwise every waiting caller would fail with that caller's error.
	flightCtx := context.WithoutCancel(ctx)

	ch := readThroughGroup.DoChan(key, func() (val any, err error) {
		// DoChan runs fn in its own goroutine, so an unrecovered panic would crash
		// the process instead of propagating to the caller.
		defer func() {
			if r := recover(); r != nil {
				val, err = nil, fmt.Errorf("loader func panic: %v", r)
			}
		}()

		loaderCtx := flightCtx
		if opts.LoaderTimeout > 0 {
			var cancel context.CancelFunc
			loaderCtx, cancel = context.WithTimeout(flightCtx, opts.LoaderTimeout)
			defer cancel()
		}

		// Check the cache again, another flight may have populated it since this
		// caller's lookup. Hit and miss hooks were already invoked for this caller,
		// so only errors are reported here.
		redisCtx, redisCancel := context.WithTimeout(loaderCtx, opts.CacheReadTimeout)
		defer redisCancel()

		var result T
		err = rdb.Get(redisCtx, key, &result)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, ErrKeyNotFound) {
			opts.OnCacheError(key, RedisOperationGET, err)
		}

		result, err = loader(loaderCtx)
		if err != nil {
			return result, fmt.Errorf("loader func: %w", err)
		}

		readThroughStore(loaderCtx, rdb, key, result, opts)

		return result, nil
	})

	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			var zero T
			return zero, fmt.Errorf("singleflight: %w", res.Err)
		}
		return res.Val.(T), nil
	}
}
