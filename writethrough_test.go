package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

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
				var wtErr *WriteThroughError
				if errors.As(err, &wtErr) {
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
