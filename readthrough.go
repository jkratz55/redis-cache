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
	UpdateAsync bool
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
// It is important to note that ReadThrough does not guarantee the Cahce (Redis) will be updated.
// If the RedisOperationSET operation fails (including timeout), ReadThrough will still return the value with
// a nil error. However, since the value wasn't cached in Redis, the next request will invoke the
// LoaderFunc.
//
// Errors from Redis are not bubbled up to the caller through the error value returned by ReadThrough.
// However, hooks/callbacks are provided via ReadThroughOptions OnCacheError to allow for logging or
// instrumentation. Additionally, there are hooks for cache misses and hits via ReadThroughOptions.
//
// Since all operations ReadThrough performs are synchronous, it is recommended to use short timeouts
// for Redis operations to avoid blocking the caller. The default for RedisOperationGET and RedisOperationSET operations is 500ms
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

	if opts.UpdateAsync {
		go func() {
			err := readThroughUpdate(context.Background(), rdb, key, result, opts)
			if err != nil {
				opts.OnCacheError(key, RedisOperationSET, err)
			}
		}()
	} else {
		err := readThroughUpdate(ctx, rdb, key, result, opts)
		if err != nil {
			opts.OnCacheError(key, RedisOperationSET, err)
		}
	}

	return result, nil
}

func readThroughUpdate[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	val T,
	opts ReadThroughOptions) error {

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheWriteTimeout)
	defer redisCancel()

	err := rdb.Set(redisCtx, key, val, opts.TTL)
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	return nil
}

// ReadThroughSingleFlight handle read-through caching. ReadThroughSingleFlight first attempts to
// retrieve the value from Redis by the key. If the value is found in Redis it is returned immediately.
// Otherwise, the LoaderFunc is invoked to retrieve the value from the source of truth. The value is
// then stored in Redis for future requests.
//
// It is important to note that ReadThroughSingleFlight does not guarantee the Cahce (Redis) will be updated.
// If the SET operation fails (including timeout), ReadThroughSingleFlight will still return the value with
// a nil error. However, since the value wasn't cached in Redis, the next request will invoke the
// LoaderFunc.
//
// Errors from Redis are not bubbled up to the caller through the error value returned by ReadThroughSingleFlight.
// However, hooks/callbacks are provided via ReadThroughOptions OnCacheError to allow for logging or
// instrumentation. Additionally, there are hooks for cache misses and hits via ReadThroughOptions.
//
// Since all operations ReadThroughSingleFlight performs are synchronous, it is recommended to use short
// timeouts for Redis operations to avoid blocking the caller. The default for RedisOperationGET and SET
// operations is 500ms to be conservative.
//
// ReadThroughSingleFlight is similar to ReadThrough, but it uses singleflight to prevent duplicate work.
// ReadThroughSingleFlight will only invoke the LoaderFunc once for a given key, and all other sequent
// requests will wait for the first request to complete, and the result will be shared with all waiting
// callers. The result is shared regardless of the outcome, meaning if the operation results in an error,
// all waiting callers will error.
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

	sfVal, err, _ := readThroughGroup.Do(key, func() (any, error) {

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

		if opts.UpdateAsync {
			go func() {
				err := readThroughUpdate(context.Background(), rdb, key, result, opts)
				if err != nil {
					opts.OnCacheError(key, RedisOperationSET, err)
				}
			}()
		} else {
			err := readThroughUpdate(ctx, rdb, key, result, opts)
			if err != nil {
				opts.OnCacheError(key, RedisOperationSET, err)
			}
		}

		return result, nil
	})

	if err != nil {
		return result, fmt.Errorf("singleflight: %w", err)
	}
	return sfVal.(T), nil
}
