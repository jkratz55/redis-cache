package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
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

	for i := range numRequests {
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
	for i := range numRequests {
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

	for i := range numRequests {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hello", ReadThroughOptions{}, loader)
			errs[i] = err
		}(i)
	}

	wg.Wait()

	assert.Equal(t, 1, loaderCalls)
	for i := range numRequests {
		assert.Error(t, errs[i])
		assert.Contains(t, errs[i].Error(), expectedErr.Error())
	}
}

type ctxKey struct{}

// ctxCaptureClient records the context value stored under ctxKey and the context
// error observed by Set.
type ctxCaptureClient struct {
	RedisClient
	sets chan ctxCapture
}

type ctxCapture struct {
	value any
	err   error
}

func (c *ctxCaptureClient) Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd {
	c.sets <- ctxCapture{value: ctx.Value(ctxKey{}), err: ctx.Err()}
	return c.RedisClient.Set(ctx, key, val, ttl)
}

func TestReadThrough_AsyncUpdateAfterCancel(t *testing.T) {
	setup()
	defer tearDown()

	capture := &ctxCaptureClient{RedisClient: client, sets: make(chan ctxCapture, 1)}
	rdb := New(capture)

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "trace-id"))
	val, err := ReadThrough[string](ctx, rdb, "hello", ReadThroughOptions{UpdateAsync: true},
		func(ctx context.Context) (string, error) {
			// The caller goes away before the async update runs.
			cancel()
			return "world", nil
		})
	assert.NoError(t, err)
	assert.Equal(t, "world", val)

	select {
	case got := <-capture.sets:
		assert.Equal(t, "trace-id", got.value)
		assert.NoError(t, got.err)
	case <-time.After(time.Second):
		t.Fatal("async cache update was not performed")
	}
	assert.Eventually(t, func() bool { return server.Exists("hello") }, time.Second, 5*time.Millisecond)
}

func TestReadThroughSingleFlight_AsyncUpdateAfterCancel(t *testing.T) {
	setup()
	defer tearDown()

	capture := &ctxCaptureClient{RedisClient: client, sets: make(chan ctxCapture, 1)}
	rdb := New(capture)

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "trace-id"))
	defer cancel()
	val, err := ReadThroughSingleFlight[string](ctx, rdb, "async-sf", ReadThroughOptions{UpdateAsync: true},
		func(ctx context.Context) (string, error) {
			return "world", nil
		})
	assert.NoError(t, err)
	assert.Equal(t, "world", val)
	cancel()

	select {
	case got := <-capture.sets:
		assert.Equal(t, "trace-id", got.value)
		assert.NoError(t, got.err)
	case <-time.After(time.Second):
		t.Fatal("async cache update was not performed")
	}
}

func TestReadThroughSingleFlight_CancellationIsolation(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	const key = "isolation"

	release := make(chan struct{})
	loaderStarted := make(chan struct{})
	var loaderCalls atomic.Int64
	var loaderCtxErr atomic.Value
	loader := func(ctx context.Context) (string, error) {
		loaderCalls.Add(1)
		close(loaderStarted)
		<-release
		loaderCtxErr.Store(fmt.Sprint(ctx.Err()))
		assert.Equal(t, "trace-id", ctx.Value(ctxKey{}))
		return "world", nil
	}

	// First caller starts the flight and is canceled while the loader runs.
	ctxA, cancelA := context.WithCancel(context.WithValue(context.Background(), ctxKey{}, "trace-id"))
	errA := make(chan error, 1)
	go func() {
		_, err := ReadThroughSingleFlight[string](ctxA, rdb, key, ReadThroughOptions{}, loader)
		errA <- err
	}()
	<-loaderStarted

	// Second caller joins the in-flight load.
	joined := make(chan struct{})
	type result struct {
		val string
		err error
	}
	resB := make(chan result, 1)
	go func() {
		opts := ReadThroughOptions{OnCacheMiss: func(string) { close(joined) }}
		val, err := ReadThroughSingleFlight[string](context.Background(), rdb, key, opts, loader)
		resB <- result{val, err}
	}()
	<-joined
	time.Sleep(20 * time.Millisecond)

	cancelA()
	select {
	case err := <-errA:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("canceled caller did not return")
	}

	close(release)
	select {
	case res := <-resB:
		assert.NoError(t, res.err)
		assert.Equal(t, "world", res.val)
	case <-time.After(time.Second):
		t.Fatal("second caller did not return")
	}

	assert.Equal(t, int64(1), loaderCalls.Load())
	assert.Equal(t, "<nil>", loaderCtxErr.Load())

	var cached string
	assert.NoError(t, rdb.Get(context.Background(), key, &cached))
	assert.Equal(t, "world", cached)
}

func TestReadThroughSingleFlight_HooksOncePerCaller(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	var hits, misses, cacheErrs atomic.Int64
	opts := ReadThroughOptions{
		OnCacheHit:   func(string) { hits.Add(1) },
		OnCacheMiss:  func(string) { misses.Add(1) },
		OnCacheError: func(string, RedisOperation, error) { cacheErrs.Add(1) },
	}
	loader := func(ctx context.Context) (string, error) {
		time.Sleep(50 * time.Millisecond)
		return "world", nil
	}

	const numRequests = 10
	var wg sync.WaitGroup
	for range numRequests {
		wg.Go(func() {
			val, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hooks", opts, loader)
			assert.NoError(t, err)
			assert.Equal(t, "world", val)
		})
	}
	wg.Wait()

	assert.Equal(t, int64(numRequests), misses.Load())
	assert.Equal(t, int64(0), hits.Load())
	assert.Equal(t, int64(0), cacheErrs.Load())

	_, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hooks", opts, loader)
	assert.NoError(t, err)
	assert.Equal(t, int64(1), hits.Load())
	assert.Equal(t, int64(numRequests), misses.Load())
}

func TestReadThroughSingleFlight_LoaderTimeout(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	opts := ReadThroughOptions{LoaderTimeout: 20 * time.Millisecond}
	_, err := ReadThroughSingleFlight[string](context.Background(), rdb, "timeout", opts,
		func(ctx context.Context) (string, error) {
			<-ctx.Done()
			return "", ctx.Err()
		})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, server.Exists("timeout"))
}

func TestReadThroughSingleFlight_LoaderPanic(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	_, err := ReadThroughSingleFlight[string](context.Background(), rdb, "panic", ReadThroughOptions{},
		func(ctx context.Context) (string, error) {
			panic("boom")
		})
	assert.ErrorContains(t, err, "loader func panic: boom")
}
