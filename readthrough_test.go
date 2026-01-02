package cache

import (
	"context"
	"errors"
	"sync"
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
		loaderErr           error
		expectErr           bool
		redisGetErr         error
		redisSetErr         error
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
		{
			name:                "Loader Failure",
			cacheInit:           func(cache *Cache) {},
			key:                 "hello",
			expectedLoaderCalls: 1,
			expectedValue:       "world",
			loaderErr:           errors.New("loader error"),
			expectErr:           true,
			opts:                ReadThroughOptions{},
		},
		{
			name:                "Cache GET Error",
			cacheInit:           func(cache *Cache) {},
			key:                 "hello",
			expectedLoaderCalls: 1,
			expectedValue:       "world",
			redisGetErr:         errors.New("redis error"),
			opts: ReadThroughOptions{
				OnCacheError: func(key string, op RedisOperation, err error) {
					assert.Equal(t, "hello", key)
					assert.Equal(t, RedisOperationGET, op)
				},
			},
		},
		{
			name:                "Cache SET Error",
			cacheInit:           func(cache *Cache) {},
			key:                 "hello",
			expectedLoaderCalls: 1,
			expectedValue:       "world",
			redisSetErr:         errors.New("redis error"),
			opts: ReadThroughOptions{
				OnCacheError: func(key string, op RedisOperation, err error) {
					assert.Equal(t, "hello", key)
					assert.Equal(t, RedisOperationSET, op)
				},
			},
		},
		{
			name:                "Cache SET Error Async",
			cacheInit:           func(cache *Cache) {},
			key:                 "hello",
			expectedLoaderCalls: 1,
			expectedValue:       "world",
			redisSetErr:         errors.New("redis error"),
			opts: ReadThroughOptions{
				UpdateAsync: true,
				OnCacheError: func(key string, op RedisOperation, err error) {
					assert.Equal(t, "hello", key)
					assert.Equal(t, RedisOperationSET, op)
				},
			},
		},
		{
			name:                "TTL respect",
			cacheInit:           func(cache *Cache) {},
			key:                 "hello",
			expectedLoaderCalls: 1,
			expectedValue:       "world",
			opts: ReadThroughOptions{
				TTL: time.Minute,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			setup()
			defer tearDown()
			loaderCalls := 0

			loader := func(ctx context.Context) (string, error) {
				loaderCalls++
				return tt.expectedValue, tt.loaderErr
			}

			var rdbClient RedisClient = client
			if tt.redisGetErr != nil || tt.redisSetErr != nil {
				rdbClient = &errorRedisClient{RedisClient: client, getErr: tt.redisGetErr, setErr: tt.redisSetErr}
			}
			rdb := New(rdbClient)
			tt.cacheInit(rdb)

			val, err := ReadThrough[string](context.Background(), rdb, tt.key, tt.opts, loader)
			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedValue, val)
			}
			assert.Equal(t, tt.expectedLoaderCalls, loaderCalls)

			if tt.opts.UpdateAsync {
				time.Sleep(time.Millisecond * 100)
			}

			if !tt.expectErr && tt.redisSetErr == nil {
				var cachedVal string
				verifyCache := New(client)
				err = verifyCache.Get(context.Background(), tt.key, &cachedVal)
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedValue, cachedVal)
				if tt.opts.TTL > 0 {
					ttl, err := client.TTL(context.Background(), tt.key).Result()
					assert.NoError(t, err)
					assert.True(t, ttl > 0 && ttl <= tt.opts.TTL)
				}
			}
		})
	}
}

func TestReadThroughSingleFlight(t *testing.T) {
	setup()
	defer tearDown()

	loaderCalls := 0
	loader := func(ctx context.Context) (string, error) {
		loaderCalls++
		time.Sleep(time.Millisecond * 100)
		return "world", nil
	}

	rdb := New(client)
	var wg sync.WaitGroup
	numRequests := 10
	results := make([]string, numRequests)
	errs := make([]error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			val, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hello", ReadThroughOptions{}, loader)
			results[i] = val
			errs[i] = err
		}(i)
	}

	wg.Wait()

	assert.Equal(t, 1, loaderCalls)
	for i := 0; i < numRequests; i++ {
		assert.NoError(t, errs[i])
		assert.Equal(t, "world", results[i])
	}
}

func TestReadThroughSingleFlight_Hit(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	err := rdb.Set(context.Background(), "hello", "world", 0)
	assert.NoError(t, err)

	loaderCalls := 0
	loader := func(ctx context.Context) (string, error) {
		loaderCalls++
		return "world", nil
	}

	val, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hello", ReadThroughOptions{}, loader)
	assert.NoError(t, err)
	assert.Equal(t, "world", val)
	assert.Equal(t, 0, loaderCalls)
}

func TestReadThroughSingleFlight_LoaderError(t *testing.T) {
	setup()
	defer tearDown()

	loaderCalls := 0
	expectedErr := errors.New("loader error")
	loader := func(ctx context.Context) (string, error) {
		loaderCalls++
		time.Sleep(time.Millisecond * 100)
		return "", expectedErr
	}

	rdb := New(client)
	var wg sync.WaitGroup
	numRequests := 5
	errs := make([]error, numRequests)

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hello", ReadThroughOptions{}, loader)
			errs[i] = err
		}(i)
	}

	wg.Wait()

	assert.Equal(t, 1, loaderCalls)
	for i := 0; i < numRequests; i++ {
		assert.Error(t, errs[i])
		assert.Contains(t, errs[i].Error(), expectedErr.Error())
	}
}
