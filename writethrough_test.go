package cache

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func TestWriteThrough_AsyncUpdateAfterCancel(t *testing.T) {
	setup()
	defer tearDown()

	capture := &ctxCaptureClient{RedisClient: client, sets: make(chan ctxCapture, 1)}
	rdb := New(capture)

	completed := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "trace-id"))
	err := WriteThrough[string](ctx, rdb, "hello", "world", WriteThroughOptions{
		UpdateCacheAsync:       true,
		OnAsyncUpdateCompleted: func(key string, err error) { completed <- err },
	}, func(ctx context.Context, key string, val string) error {
		// The caller goes away right after the source of truth is updated.
		cancel()
		return nil
	})
	assert.NoError(t, err)

	select {
	case got := <-capture.sets:
		assert.Equal(t, "trace-id", got.value)
		assert.NoError(t, got.err)
	case <-time.After(time.Second):
		t.Fatal("async cache update was not performed")
	}
	select {
	case err := <-completed:
		assert.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("OnAsyncUpdateCompleted was not invoked")
	}

	var cached string
	assert.NoError(t, New(client).Get(context.Background(), "hello", &cached))
	assert.Equal(t, "world", cached)
}

func TestWriteThrough(t *testing.T) {

	type test struct {
		name          string
		key           string
		val           string
		opts          WriteThroughOptions
		updaterErr    error
		redisSetErr   error
		expectErr     bool
		sourceUpdated bool
	}

	tests := []test{
		{
			name:          "Sync Update Success",
			key:           "hello",
			val:           "world",
			opts:          WriteThroughOptions{},
			expectErr:     false,
			sourceUpdated: true,
		},
		{
			name: "Async Update Success",
			key:  "hello",
			val:  "world",
			opts: WriteThroughOptions{
				UpdateCacheAsync: true,
			},
			expectErr:     false,
			sourceUpdated: true,
		},
		{
			name:          "Updater Error",
			key:           "hello",
			val:           "world",
			opts:          WriteThroughOptions{},
			updaterErr:    errors.New("updater error"),
			expectErr:     true,
			sourceUpdated: false,
		},
		{
			name:          "Redis Sync Error",
			key:           "hello",
			val:           "world",
			opts:          WriteThroughOptions{},
			redisSetErr:   errors.New("redis error"),
			expectErr:     true,
			sourceUpdated: true,
		},
		{
			name: "Redis Async Error",
			key:  "hello",
			val:  "world",
			opts: WriteThroughOptions{
				UpdateCacheAsync: true,
			},
			redisSetErr:   errors.New("redis error"),
			expectErr:     false,
			sourceUpdated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup()
			defer tearDown()

			updater := func(ctx context.Context, key string, val string) error {
				return tt.updaterErr
			}

			var asyncErr error
			asyncDone := make(chan struct{})
			if tt.opts.UpdateCacheAsync {
				tt.opts.OnAsyncUpdateCompleted = func(key string, err error) {
					asyncErr = err
					close(asyncDone)
				}
			}

			var rdbClient RedisClient = client
			if tt.redisSetErr != nil {
				rdbClient = &errorRedisClient{RedisClient: client, setErr: tt.redisSetErr}
			}
			rdb := New(rdbClient)

			err := WriteThrough[string](context.Background(), rdb, tt.key, tt.val, tt.opts, updater)

			if tt.expectErr {
				assert.Error(t, err)
				if wtErr, ok := errors.AsType[*WriteThroughError](err); ok {
					assert.Equal(t, tt.sourceUpdated, wtErr.SourceUpdated)
				} else {
					t.Errorf("expected *WriteThroughError, got %T", err)
				}
			} else {
				assert.NoError(t, err)
			}

			if tt.opts.UpdateCacheAsync {
				select {
				case <-asyncDone:
					if tt.redisSetErr != nil {
						assert.Error(t, asyncErr)
						var wtErr *WriteThroughError
						assert.True(t, errors.As(asyncErr, &wtErr))
						assert.True(t, wtErr.SourceUpdated)
					} else {
						assert.NoError(t, asyncErr)
					}
				case <-time.After(time.Second):
					t.Errorf("async update timeout")
				}
			}

			if !tt.expectErr && !tt.opts.UpdateCacheAsync {
				var cachedVal string
				err = rdb.Get(context.Background(), tt.key, &cachedVal)
				assert.NoError(t, err)
				assert.Equal(t, tt.val, cachedVal)
			}
		})
	}
}

func TestWriteThrough_InvalidatesOnFailedUpdate(t *testing.T) {
	setErr := errors.New("set error")

	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			setup()
			defer tearDown()

			ctx := context.Background()
			assert.NoError(t, New(client).Set(ctx, "price", 100, 0))

			completed := make(chan error, 1)
			rdb := New(&errorRedisClient{RedisClient: client, setErr: setErr})
			err := WriteThrough[int](ctx, rdb, "price", 80, WriteThroughOptions{
				UpdateCacheAsync:       async,
				OnAsyncUpdateCompleted: func(key string, err error) { completed <- err },
			}, func(ctx context.Context, key string, val int) error { return nil })
			if async {
				assert.NoError(t, err)
				err = <-completed
			}

			wtErr, ok := errors.AsType[*WriteThroughError](err)
			if assert.True(t, ok, "expected *WriteThroughError, got %T", err) {
				assert.True(t, wtErr.SourceUpdated)
			}
			assert.ErrorIs(t, err, setErr)

			// The value cached before the source of truth was updated is deleted.
			assert.False(t, server.Exists("price"))
		})
	}
}

func TestWriteThrough_InvalidateFails(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	assert.NoError(t, New(client).Set(ctx, "price", 100, 0))

	setErr := errors.New("set error")
	delErr := errors.New("del error")
	rdb := New(&errorRedisClient{RedisClient: client, setErr: setErr, delErr: delErr})
	err := WriteThrough[int](ctx, rdb, "price", 80, WriteThroughOptions{},
		func(ctx context.Context, key string, val int) error { return nil })

	// Invalidating is best effort, the error reports both failures.
	assert.ErrorIs(t, err, setErr)
	assert.ErrorIs(t, err, delErr)
	assert.True(t, server.Exists("price"))
}

func TestWriteThrough_InvalidatesAfterCallerCanceled(t *testing.T) {
	setup()
	defer tearDown()

	assert.NoError(t, New(client).Set(context.Background(), "price", 100, 0))

	// The caller's context is canceled once the source of truth is updated, so the
	// cache update fails. The delete isn't canceled with it.
	ctx, cancel := context.WithCancel(context.Background())
	err := WriteThrough[int](ctx, New(client), "price", 80, WriteThroughOptions{},
		func(ctx context.Context, key string, val int) error {
			cancel()
			return nil
		})
	assert.ErrorIs(t, err, context.Canceled)
	assert.False(t, server.Exists("price"))
}

func TestWriteThrough_AsyncCallbackPanic(t *testing.T) {
	setup()
	defer tearDown()

	called := make(chan struct{})
	err := WriteThrough[string](context.Background(), New(client), "hello", "world", WriteThroughOptions{
		UpdateCacheAsync: true,
		OnAsyncUpdateCompleted: func(key string, err error) {
			close(called)
			panic("boom")
		},
	}, func(ctx context.Context, key string, val string) error { return nil })
	assert.NoError(t, err)

	// The process would crash if the panic in the background goroutine wasn't
	// recovered.
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("OnAsyncUpdateCompleted was not invoked")
	}
	time.Sleep(20 * time.Millisecond)
}

// hangingClient simulates an unreachable Redis: Set and Del block until their
// context is done.
type hangingClient struct {
	RedisClient
	dels atomic.Int64
}

func (c *hangingClient) Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd {
	<-ctx.Done()
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetErr(ctx.Err())
	return cmd
}

func (c *hangingClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	c.dels.Add(1)
	<-ctx.Done()
	cmd := redis.NewIntCmd(ctx)
	cmd.SetErr(ctx.Err())
	return cmd
}

func TestWriteThrough_UpdateAndInvalidateShareTimeout(t *testing.T) {
	setup()
	defer tearDown()

	const timeout = 100 * time.Millisecond
	hanging := &hangingClient{RedisClient: client}
	start := time.Now()
	err := WriteThrough[string](context.Background(), New(hanging), "key", "value",
		WriteThroughOptions{CacheWriteTimeout: timeout},
		func(ctx context.Context, key string, val string) error { return nil })
	elapsed := time.Since(start)

	// The update times out, and the delete doesn't wait for Redis again.
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.ErrorContains(t, err, "invalidate cached value")
	assert.Equal(t, int64(1), hanging.dels.Load())
	assert.Less(t, elapsed, timeout+timeout/2)
}
