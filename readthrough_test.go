package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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

func TestReadThroughSingleFlight_AbandonedLoad(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)

	t.Run("is canceled", func(t *testing.T) {
		canceled := make(chan struct{})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := ReadThroughSingleFlight[string](ctx, rdb, "abandoned", ReadThroughOptions{},
			func(ctx context.Context) (string, error) {
				<-ctx.Done()
				close(canceled)
				return "", ctx.Err()
			})
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		select {
		case <-canceled:
		case <-time.After(time.Second):
			t.Fatal("load wasn't canceled once no caller was waiting for it")
		}
	})

	t.Run("doesn't capture later callers", func(t *testing.T) {
		// The loader is stuck on a hung dependency and ignores cancellation.
		hung := make(chan struct{})
		defer close(hung)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := ReadThroughSingleFlight[string](ctx, rdb, "hung", ReadThroughOptions{},
			func(ctx context.Context) (string, error) {
				<-hung
				return "", errors.New("hung")
			})
		assert.ErrorIs(t, err, context.DeadlineExceeded)

		// The dependency recovered, the next caller starts a new load instead of
		// joining the abandoned one.
		val, err := ReadThroughSingleFlight[string](context.Background(), rdb, "hung", ReadThroughOptions{},
			func(ctx context.Context) (string, error) {
				return "loaded", nil
			})
		assert.NoError(t, err)
		assert.Equal(t, "loaded", val)
	})
}

func TestReadThroughSingleFlight_LoaderGoexit(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	_, err := ReadThroughSingleFlight[string](context.Background(), rdb, "goexit", ReadThroughOptions{},
		func(ctx context.Context) (string, error) {
			runtime.Goexit()
			return "", nil
		})
	assert.ErrorContains(t, err, "loader func did not return")
}

// awaitWhileBlocked waits for result, which must be produced while release is
// open. If result isn't produced within a second, the caller is assumed to wait
// for the blocked load, so release is closed to observe what the caller receives.
// release is always closed when awaitWhileBlocked returns.
func awaitWhileBlocked[T any](result <-chan T, release chan struct{}) (res T, blocked bool) {
	select {
	case res = <-result:
		close(release)
		return res, false
	case <-time.After(time.Second):
		close(release)
		return <-result, true
	}
}

// singleFlightResult is the result of a ReadThroughSingleFlight call made in a
// goroutine, including a panic.
type singleFlightResult[T any] struct {
	val      T
	err      error
	panicked any
}

func readThroughSingleFlightAsync[T any](ctx context.Context, rdb *Cache, key string, loader LoaderFunc[T]) <-chan singleFlightResult[T] {
	res := make(chan singleFlightResult[T], 1)
	go func() {
		var r singleFlightResult[T]
		defer func() {
			r.panicked = recover()
			res <- r
		}()
		r.val, r.err = ReadThroughSingleFlight[T](ctx, rdb, key, ReadThroughOptions{}, loader)
	}()
	return res
}

func TestReadThroughSingleFlight_IsolatedPerCache(t *testing.T) {
	ctx := context.Background()
	serverA := miniredis.RunT(t)
	serverB := miniredis.RunT(t)
	clientA := redis.NewClient(&redis.Options{Addr: serverA.Addr()})
	clientB := redis.NewClient(&redis.Options{Addr: serverB.Addr()})
	t.Cleanup(func() {
		_ = clientA.Close()
		_ = clientB.Close()
	})
	cacheA, cacheB := New(clientA), New(clientB)

	// Cache A's load is in flight while cache B loads the same key.
	started := make(chan struct{})
	release := make(chan struct{})
	resA := readThroughSingleFlightAsync(ctx, cacheA, "session:1", func(ctx context.Context) (string, error) {
		close(started)
		<-release
		return "tenant-a", nil
	})
	<-started

	// Cache B starts its own load instead of joining cache A's, so it completes
	// while cache A's load is still blocked.
	resB, blocked := awaitWhileBlocked(readThroughSingleFlightAsync(ctx, cacheB, "session:1",
		func(ctx context.Context) (string, error) {
			return "tenant-b", nil
		}), release)
	assert.False(t, blocked, "cache B joined the load of cache A")
	assert.NoError(t, resB.err)
	assert.Equal(t, "tenant-b", resB.val)
	assert.Equal(t, "tenant-a", (<-resA).val)

	var cached string
	assert.NoError(t, cacheA.Get(ctx, "session:1", &cached))
	assert.Equal(t, "tenant-a", cached)
	assert.NoError(t, cacheB.Get(ctx, "session:1", &cached))
	assert.Equal(t, "tenant-b", cached)
}

func TestReadThroughSingleFlight_IsolatedPerType(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	rdb := New(client)

	// A load of a string is in flight while an int is loaded for the same key.
	started := make(chan struct{})
	release := make(chan struct{})
	resString := readThroughSingleFlightAsync(ctx, rdb, "user:42", func(ctx context.Context) (string, error) {
		close(started)
		<-release
		return "alice", nil
	})
	<-started

	// Joining the string load would make the type assertion of its result panic.
	resInt, blocked := awaitWhileBlocked(readThroughSingleFlightAsync(ctx, rdb, "user:42",
		func(ctx context.Context) (int, error) {
			return 7, nil
		}), release)
	assert.False(t, blocked, "the int load joined the string load")
	assert.Nil(t, resInt.panicked)
	assert.NoError(t, resInt.err)
	assert.Equal(t, 7, resInt.val)
	assert.Equal(t, "alice", (<-resString).val)
}

func TestReadThroughSingleFlight_NilInterface(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)
	var val fmt.Stringer
	assert.NotPanics(t, func() {
		var err error
		val, err = ReadThroughSingleFlight[fmt.Stringer](context.Background(), rdb, "nil", ReadThroughOptions{},
			func(ctx context.Context) (fmt.Stringer, error) {
				return nil, nil
			})
		assert.NoError(t, err)
	})
	assert.Nil(t, val)
}

func TestReadThrough_AsyncCallbackPanic(t *testing.T) {
	setup()
	defer tearDown()

	failing := &errorRedisClient{RedisClient: client, setErr: errors.New("redis error")}
	rdb := New(failing)

	called := make(chan struct{})
	opts := ReadThroughOptions{
		UpdateAsync: true,
		OnCacheError: func(key string, op RedisOperation, err error) {
			close(called)
			panic("boom")
		},
	}
	val, err := ReadThrough[string](context.Background(), rdb, "panic", opts,
		func(ctx context.Context) (string, error) {
			return "world", nil
		})
	assert.NoError(t, err)
	assert.Equal(t, "world", val)

	// The process would crash if the panic in the background goroutine wasn't
	// recovered.
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("OnCacheError was not invoked")
	}
	time.Sleep(20 * time.Millisecond)
}

func TestReadThroughSingleFlight_CallbackPanic(t *testing.T) {
	setup()
	defer tearDown()

	failing := &errorRedisClient{RedisClient: client, setErr: errors.New("redis error")}
	rdb := New(failing)

	for _, async := range []bool{false, true} {
		t.Run(fmt.Sprintf("async=%t", async), func(t *testing.T) {
			var calls atomic.Int64
			opts := ReadThroughOptions{
				UpdateAsync: async,
				OnCacheError: func(key string, op RedisOperation, err error) {
					calls.Add(1)
					if op == RedisOperationSET {
						panic("boom")
					}
				},
			}
			// The panic in the callback, invoked by the shared load, neither crashes
			// the process nor fails the load.
			val, err := ReadThroughSingleFlight[string](context.Background(), rdb, "panic", opts,
				func(ctx context.Context) (string, error) {
					return "world", nil
				})
			assert.NoError(t, err)
			assert.Equal(t, "world", val)
			assert.Eventually(t, func() bool { return calls.Load() == 1 }, time.Second, 5*time.Millisecond)
		})
	}
}
