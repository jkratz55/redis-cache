package cache

import (
	"context"
	"errors"
	"testing"
	"time"

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
