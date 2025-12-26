package cache

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/singleflight"
)

// LoaderFunc is a function that loads a value from the source of truth.
//
// A LoaderFunc should be idempotent and safe for concurrent use.
type LoaderFunc[T any] func(ctx context.Context) (T, error)

type ReadThroughCache[T any] struct {
	rdb          *Cache
	loader       LoaderFunc[T]
	readTimeout  time.Duration
	writeTimeout time.Duration
	logger       Logger
	sf           singleflight.Group
	sfEnabled    bool
}

func NewReadThroughCache[T any](rdb *Cache, loader LoaderFunc[T], readTimeout time.Duration) *ReadThroughCache[T] {
	return &ReadThroughCache[T]{
		rdb:          rdb,
		loader:       loader,
		readTimeout:  readTimeout,
		writeTimeout: time.Second * 1,
	}
}

// GetOrLoad returns the value associated with the provided key by first attempting to read it from
// the Redis. If the value is not found or the operation fails, the configured LoaderFunc is invoked.
// If the LoaderFunc succeeds, the value is then stored in the Redis under the provided key for future
// requests. If the LoaderFunc fails, the error is returned.
//
// If the result from then LoaderFunc fails to be stored in Redis, an error will be logged but not
// returned to the caller. The caller of this function can still proceed normally, even if the set
// operation fails.
//
// It is best practice to configure ReadThroughCache with relatively low read and write timeout to
// Redis, so that slow commands don't block the caller.
func (r *ReadThroughCache[T]) GetOrLoad(ctx context.Context, key string, ttl time.Duration) (T, error) {

	var val T

	redisCtx, cancel := context.WithTimeout(ctx, r.readTimeout)
	defer cancel()

	err := r.rdb.Get(redisCtx, key, &val)
	if err == nil {
		return val, nil
	}

	if r.sfEnabled {
		return r.loadSingleFlight(ctx, key, ttl)
	}

	val, err = r.loader(ctx)
	if err != nil {
		return val, err
	}

	writeCtx, writeCancel := context.WithTimeout(ctx, r.writeTimeout)
	defer writeCancel()

	_, err = r.rdb.SetIfAbsent(writeCtx, key, val, ttl)
	if err != nil {
		r.logger.Error("failed to set value in redis", "err", err)
	}

	return val, nil
}

func (r *ReadThroughCache[T]) loadSingleFlight(ctx context.Context, key string, ttl time.Duration) (T, error) {
	var zero T

	val, err, _ := r.sf.Do(key, func() (any, error) {
		var v T

		redisCtx, cancel := context.WithTimeout(ctx, r.readTimeout)
		if err := r.rdb.Get(redisCtx, key, &v); err == nil {
			cancel()
			return v, nil
		}
		cancel()

		v, err := r.loader(ctx)
		if err != nil {
			return v, err
		}

		writeCtx, cancel := context.WithTimeout(ctx, r.writeTimeout)
		if _, err := r.rdb.SetIfAbsent(writeCtx, key, v, ttl); err != nil {
			r.logger.Error("failed to set value in redis", "err", err)
		}
		cancel()

		return v, nil
	})

	if err != nil {
		return zero, err
	}

	return val.(T), nil
}

type WriterFunc[T any] func(ctx context.Context, key string, val T) error

// WriteThroughCache updates the source of truth by invoking the provided WriterFunc, followed by
// setting the value in Redis synchronously.
//
// WriteThroughCache is not atomic. It is possible that the source of truth is updated, but the value
// is not set in Redis. The source of truth must successfully update first or an error will be returned.
// This ensures the source of truth is always correct, but Redis may contain stale data for a period.
// It is recommended to use short TTLs to avoid cases where Redis contains stale data for long periods
// of time.
type WriteThroughCache[T any] struct {
	rdb *Cache
	wr  WriterFunc[T]
	ttl time.Duration
}

func (w *WriteThroughCache[T]) Write(ctx context.Context, key string, val T) error {
	err := w.wr(ctx, key, val)
	if err != nil {
		return err
	}

	err = w.rdb.Set(ctx, key, val, w.ttl)
	if err != nil {
		// todo: log warning but don't bubble the error up
	}

	return nil
}

// WriteBehindCache sets a value in Redis and asynchronously writes the value to the source of truth
// by invoking the provided WriterFunc.
//
// WriteBehindCache is not atomic. It is possible that the value is written to Redis but the source of
// truth is not updated. This is a trade-off for the performance benefits of write-behind caching. There
// is no guarantee regarding data integrity or order of updates either. As an example, two quick successive
// calls to Write may result in the source of truth being updated twice, but the order of updates is non
// deterministic. It is possible that the second update gets overwritten by the first update in the source
// of truth since the writes happen asynchronously.
//
// WriteBehindCache is a poor choice if data integrity is important. It trades data consistency and
// integrity for raw performance.
type WriteBehindCache[T any] struct {
	rdb          *Cache
	wr           WriterFunc[T]
	ttl          time.Duration
	writeTimeout time.Duration
	maxAttempts  int
	backoff      time.Duration
	onError      func(err error)
}

func (w *WriteBehindCache[T]) Write(ctx context.Context, key string, val T) error {
	err := w.rdb.Set(ctx, key, val, w.ttl)
	if err != nil {
		return err
	}

	go func() {
		var err error

		for attempts := 1; attempts <= w.maxAttempts; attempts++ {
			writeCtx, writeCancel := context.WithTimeout(context.Background(), w.writeTimeout)

			err = w.wr(writeCtx, key, val)
			writeCancel()

			if err == nil {
				return
			}

			time.Sleep(w.backoff)
		}

		if err != nil {
			w.onError(err)
		}
	}()

	return nil
}

// Cacheable attempts to read a value from the cache for a given key. On a cache
// miss or read error, it executes the provided function (fn) to retrieve or
// compute the value. If successful, the value is then asynchronously stored in
// the cache with the specified TTL (time-to-live) for future requests.
//
// This function implements a read-through cache pattern, where the cache is
// updated after a cache miss. Cacheable only returns an error if the value cannot
// be retrieved or computed by the provided function.
//
// Errors encountered while storing the value in the cache are logged, but not
// returned to the caller, and the cache set operation occurs in a non-blocking
// goroutine.
//
// The cache read operation is subject to a readTimeout, which defines the
// maximum duration for waiting on a cache response. If the cache read exceeds
// this timeout or fails, the provided function is called to compute the value.
func Cacheable[T any](
	ctx context.Context,
	c *Cache,
	key string,
	readTimeout time.Duration,
	ttl time.Duration,
	fn func(ctx context.Context) (T, error)) (T, error) {

	var val T

	// Create a context with a timeout for the read operation. The purpose of
	// this is to bypass the cache if the read from cache is slow.
	readCtx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	// Try to read the value from cache first. If the retrieval is successful
	// return the value as there is no need to go to the source system.
	if err := c.Get(readCtx, key, &val); err == nil {
		return val, nil
	}

	// Either we've encountered a cache miss or an error. In either case, we
	// need to go to the source system to retrieve the value or recompute the
	// result.
	val, err := fn(ctx)
	if err != nil {
		// If func to retrieve or compute the value fails nothing further can
		// be done. Return the error.
		return val, err
	}

	// If the value was successfully retrieved or computed, store it in the
	// cache with the configured TTL in a background goroutine to avoid blocking
	// returning the result to the caller.
	go func() {
		setCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err = c.SetIfAbsent(setCtx, key, val, ttl)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to update cache for key %s", key),
				slog.Any("err", err))
		}
	}()

	return val, nil
}

// Write first invokes the provided function to write the value to the
// source of truth. If the write operation is successful, the value is then
// synchronously stored in the cache with the specified TTL (time-to-live) for
// future requests.
//
// Write implements a write-through cache pattern, where the cache is
// updated after the source of truth is updated. Write is NOT atomic.
// It is possible that the source of truth is updated but the cache is not.
// This is a trade-off for the performance benefits of write-through caching.
// If either write operation fails, an error is returned.
func Write[T any](
	ctx context.Context,
	c *Cache,
	key string,
	val T,
	ttl time.Duration,
	fn func(ctx context.Context, v T) error) error {

	// Write the value to the source of truth first. If this operation fails bail
	err := fn(ctx, val)
	if err != nil {
		return err
	}

	if err := c.SetWithTTL(ctx, key, val, ttl); err != nil {
		return fmt.Errorf("redis: SET %s failed: %w", key, err)
	}

	return nil
}

// Delete removes the value associated with the given key from both the source
// of truth and the cache. It first attempts to delete the value from the source
// of truth using the provided function. If this operation is successful, it then
// removes the corresponding entry from the cache.
//
// Delete is designed to ensure that both the source of truth and the cache remain
// in sync. If the source of truth is updated but the cache is not, the system may
// end up in an inconsistent state. Therefore, it is essential to call this function
// whenever a value needs to be deleted from both the source of truth and the cache.
//
// If either the delete operation from the source of truth or the cache fails, an
// error is returned, providing information on where the failure occurred.
func Delete[T any](
	ctx context.Context,
	c *Cache,
	key string,
	val T,
	fn func(ctx context.Context, val T) error) error {

	// Delete the value from the source of truth first. If this operation fails bail
	err := fn(ctx, val)
	if err != nil {
		return err
	}

	if err := c.Delete(ctx, key); err != nil {
		return fmt.Errorf("redis: DEL %s failed: %w", key, err)
	}

	return nil
}
