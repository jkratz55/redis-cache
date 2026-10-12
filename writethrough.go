package cache

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// WriteThroughWriter is a function that writes a value to the source of truth.
type WriteThroughWriter[T any] func(ctx context.Context, key string, val T) error

// WriteThroughOptions contains options for WriteThrough.
type WriteThroughOptions struct {

	// CacheWriteTimeout specifies the maximum duration to wait for the cache write operation to
	// complete.
	//
	// Default is 500ms.
	CacheWriteTimeout time.Duration

	// TTL is the time-to-live for the key in the cache.
	//
	// Default is 0, which means the key will never expire.
	TTL time.Duration

	// UpdateCacheAsync indicates whether the cache update should be performed asynchronously.
	//
	// Default is false, the cache update will be performed synchronously.
	UpdateCacheAsync bool

	// OnAsyncUpdateCompleted is called when an async cache update is completed. When UpdateCacheAsync
	// is false, this callback is never invoked.
	//
	// OnAsyncUpdateCompleted is useful when UpdateCacheAsync is true, and you want to be notified when
	// the cache update has completed, and if it succeeded or failed.
	//
	// OnAsyncUpdateCompleted is invoked from a goroutine started by WriteThrough. A panic in
	// OnAsyncUpdateCompleted is recovered and ignored, so it can't crash the process.
	//
	// Default is a no-op.
	OnAsyncUpdateCompleted func(key string, err error)
}

func (o *WriteThroughOptions) init() {
	if o.CacheWriteTimeout == 0 {
		o.CacheWriteTimeout = defaultCacheWriteTimeout
	}
	if o.OnAsyncUpdateCompleted == nil {
		o.OnAsyncUpdateCompleted = func(key string, err error) {}
	}
}

// WriteThrough writes a value to the system of record and then updates the cache (Redis) with the
// new value.
//
// The behavior of WriteThrough differs depending on the value of UpdateCacheAsync in WriteThroughOptions:
//
// When UpdateCacheAsync is false, WriteThrough will block until the cache update is complete or times out
// by exceeding CacheWriteTimeout.
//
// When UpdateCacheAsync is true, WriteThrough will return immediately and the cache update will be
// performed asynchronously. The asynchronous update keeps the values of ctx, such as trace information,
// but isn't canceled when ctx is canceled. The OnAsyncUpdateCompleted callback will be invoked when the
// cache update completes. If no callback is provided, OnAsyncUpdateCompleted and the cache update becomes
// a fire-and-forget operation.
//
// When WriteThrough encounters an error it returns *WriteThroughError, which wraps the original error
// and indicates whether the source of truth was updated. This is useful, especially when UpdateCacheAsync
// is false, to determine if the cache update succeeded or failed. In some cases, even though the function
// returns an error due to the cache update failing, the application may simply elect to ignore the error.
//
// Keeping the cache consistent with the source of truth is best effort:
//
//   - If the cache update fails after the source of truth was updated, WriteThrough deletes the key so
//     the previous value isn't served anymore and the next read loads the new value. The delete can
//     fail as well, for example if Redis is unavailable, in which case the previous value stays cached
//     and the returned error wraps both errors.
//   - Concurrent writes of the same key can update the source of truth and the cache in a different
//     order, leaving an older value cached. UpdateCacheAsync makes this more likely.
//
// Set a TTL so a stale value eventually expires.
func WriteThrough[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	val T,
	opts WriteThroughOptions,
	updater WriteThroughWriter[T]) error {

	opts.init()

	err := updater(ctx, key, val)
	if err != nil {
		return &WriteThroughError{
			Key:           key,
			SourceUpdated: false,
			error:         err,
		}
	}

	if opts.UpdateCacheAsync {
		// Keep the context values, such as tracing information, but don't cancel
		// the update when the caller's context is canceled.
		asyncCtx := context.WithoutCancel(ctx)
		go func() {
			err := writeThroughUpdate(asyncCtx, rdb, key, val, opts)
			if err != nil {
				err = &WriteThroughError{
					Key:           key,
					SourceUpdated: true,
					error:         err,
				}
			}
			// A panic in the callback would crash the process since this goroutine
			// was started by the library.
			safeCall(func() { opts.OnAsyncUpdateCompleted(key, err) })
		}()
		return nil
	}

	err = writeThroughUpdate(ctx, rdb, key, val, opts)
	if err != nil {
		return &WriteThroughError{
			Key:           key,
			SourceUpdated: true,
			error:         err,
		}
	}

	return nil
}

// writeThroughUpdate sets the cached value of key to val. If that fails, the key is
// deleted so the value cached before the source of truth was updated isn't served
// anymore. The delete is best effort, if it fails too both errors are returned.
func writeThroughUpdate[T any](
	ctx context.Context,
	rdb *Cache,
	key string,
	val T,
	opts WriteThroughOptions) error {

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheWriteTimeout)
	defer redisCancel()

	err := rdb.Set(redisCtx, key, val, opts.TTL)
	if err == nil {
		return nil
	}

	// The delete gets its own timeout and isn't canceled with ctx, since the update
	// may have failed because ctx was canceled or its deadline exceeded.
	delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), opts.CacheWriteTimeout)
	defer delCancel()

	if delErr := rdb.Delete(delCtx, key); delErr != nil {
		return errors.Join(err, fmt.Errorf("invalidate cached value: %w", delErr))
	}
	return err
}

// WriteThroughError wraps an error returned by WriteThrough and indicates whether the source of truth
// was updated.
type WriteThroughError struct {
	Key           string
	SourceUpdated bool
	error
}

// Error returns the message of the underlying error.
func (e *WriteThroughError) Error() string {
	return e.error.Error()
}

// Unwrap returns the underlying error.
func (e *WriteThroughError) Unwrap() error {
	return e.error
}
