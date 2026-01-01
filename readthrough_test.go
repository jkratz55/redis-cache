package cache

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReadThrough(t *testing.T) {

	type test struct {
		name                string
		cacheInit           func(cache *Cache)
		key                 string
		opts                ReadThroughOptions
		expectedLoaderCalls int
		expectedValue       string
	}

	tests := []test{
		{
			name:                "Cache Miss: Loader Success with Sync Update",
			cacheInit:           func(cache *Cache) {},
			key:                 "hello",
			expectedLoaderCalls: 1,
			expectedValue:       "world",
			opts:                ReadThroughOptions{},
		},
		{
			name:      "Cache Miss: Loader Success with Async Update",
			cacheInit: func(cache *Cache) {},
			key:       "hello",
			opts: ReadThroughOptions{
				OnCacheError: func(key string, op RedisOperation, err error) {
					assert.NoError(t, err)
				},
				UpdateAsync: true,
			},
			expectedLoaderCalls: 1,
			expectedValue:       "world",
		},
		{
			name: "Cache Hit: No Loader",
			cacheInit: func(cache *Cache) {
				err := cache.Set(context.Background(), "hello", "world", 0)
				assert.NoError(t, err)
			},
			key:                 "hello",
			opts:                ReadThroughOptions{},
			expectedLoaderCalls: 0,
			expectedValue:       "world",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup()
			defer tearDown()
			loaderCalls := 0

			loader := func(ctx context.Context) (string, error) {
				loaderCalls++
				return tt.expectedValue, nil
			}

			rdb := New(client)
			tt.cacheInit(rdb)

			val, err := ReadThrough[string](context.Background(), rdb, tt.key, tt.opts, loader)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedValue, val)
			assert.Equal(t, tt.expectedLoaderCalls, loaderCalls)

			if tt.opts.UpdateAsync {
				time.Sleep(time.Millisecond * 50)
			}

			var cachedVal string
			err = rdb.Get(context.Background(), tt.key, &cachedVal)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedValue, cachedVal)
		})
	}
}
