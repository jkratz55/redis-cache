package cache

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

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
