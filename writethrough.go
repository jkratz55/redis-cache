package cache

import (
	"context"
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
// performed asynchronously. The OnAsyncUpdateCompleted callback will be invoked when the cache update
// completes. If no callback is provided, OnAsyncUpdateCompleted and the cache update becomes a
// fire-and-forget operation.
//
// When WriteThrough encounters an error it returns *WriteThroughError, which wraps the original error
// and indicates whether the source of truth was updated. This is useful, especially when UpdateCacheAsync
// is false, to determine if the cache update succeeded or failed. In some cases, even though the function
// returns an error due to the cache update failing, the application may simply elect to ignore the error.
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
		go func() {
			redisCtx, redisCancel := context.WithTimeout(context.Background(), opts.CacheWriteTimeout)
			defer redisCancel()

			err := rdb.Set(redisCtx, key, val, opts.TTL)
			if err != nil {
				err = &WriteThroughError{
					Key:           key,
					SourceUpdated: true,
					error:         err,
				}
			}
			opts.OnAsyncUpdateCompleted(key, err)
		}()
		return nil
	}

	redisCtx, redisCancel := context.WithTimeout(ctx, opts.CacheWriteTimeout)
	defer redisCancel()

	err = rdb.Set(redisCtx, key, val, opts.TTL)
	if err != nil {
		return &WriteThroughError{
			Key:           key,
			SourceUpdated: true,
			error:         err,
		}
	}

	return nil
}

// WriteThroughError wraps an error returned by WriteThrough and indicates whether the source of truth
// was updated.
type WriteThroughError struct {
	Key           string
	SourceUpdated bool
	error
}

func (e *WriteThroughError) Error() string {
	return e.error.Error()
}

func (e *WriteThroughError) Unwrap() error {
	return e.error
}
