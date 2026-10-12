package cache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

// controlledClient is a RedisClient implementation that allows simulating failures for Ping.
type controlledClient struct {
	*redis.Client
	pingErr   error
	pingDelay time.Duration
	mu        sync.Mutex
}

func (c *controlledClient) Ping(ctx context.Context) *redis.StatusCmd {
	c.mu.Lock()
	pingErr := c.pingErr
	pingDelay := c.pingDelay
	c.mu.Unlock()

	if pingErr != nil {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(pingErr)
		return cmd
	}

	if pingDelay > 0 {
		select {
		case <-time.After(pingDelay):
		case <-ctx.Done():
			cmd := redis.NewStatusCmd(ctx)
			cmd.SetErr(ctx.Err())
			return cmd
		}
	}

	return c.Client.Ping(ctx)
}

func (c *controlledClient) setPingErr(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pingErr = err
}

func (c *controlledClient) setPingDelay(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pingDelay = delay
}

func TestNewFailoverClient(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := redis.NewClient(&redis.Options{Addr: s1.Addr()})
	c2 := redis.NewClient(&redis.Options{Addr: s2.Addr()})

	t.Run("panic on nil primary", func(t *testing.T) {
		assert.Panics(t, func() {
			NewFailoverClient(nil, c2)
		})
	})

	t.Run("panic on nil secondary", func(t *testing.T) {
		assert.Panics(t, func() {
			NewFailoverClient(c1, nil)
		})
	})

	t.Run("success with default options", func(t *testing.T) {
		fc := NewFailoverClient(c1, c2)
		assert.NotNil(t, fc)
		assert.True(t, fc.IsPrimary())
		assert.False(t, fc.IsSecondary())
		assert.Equal(t, c1, fc.Active())
		fc.Close()
	})

	t.Run("success with custom options", func(t *testing.T) {
		opts := FailoverOptions().SetCheckInterval(10 * time.Second)
		fc := NewFailoverClient(c1, c2, opts)
		assert.NotNil(t, fc)
		assert.Equal(t, 10*time.Second, fc.config.CheckInterval)
		fc.Close()
	})
}

func TestFailoverOpts(t *testing.T) {
	t.Run("default values", func(t *testing.T) {
		opts := FailoverOptions()
		assert.Equal(t, 5*time.Second, opts.CheckInterval)
		assert.Equal(t, 1*time.Second, opts.CheckTimeout)
		assert.Equal(t, 3, opts.FailureThreshold)
		assert.Equal(t, 2, opts.SuccessThreshold)
	})

	t.Run("init normalizes values", func(t *testing.T) {
		opts := &FailoverOpts{
			CheckInterval: 500 * time.Millisecond,
			CheckTimeout:  2 * time.Second,
		}
		opts.init()
		assert.Equal(t, time.Second, opts.CheckInterval)
		assert.Equal(t, 500*time.Millisecond, opts.CheckTimeout)

		opts2 := &FailoverOpts{
			CheckInterval: 0,
			CheckTimeout:  0,
		}
		opts2.init()
		assert.Equal(t, 5*time.Second, opts2.CheckInterval)
		assert.Equal(t, 1*time.Second, opts2.CheckTimeout)
	})

	t.Run("builder methods", func(t *testing.T) {
		var failoverCalled, recoveredCalled, checkFailureCalled bool
		opts := FailoverOptions().
			SetCheckInterval(10 * time.Second).
			SetCheckTimeout(2 * time.Second).
			SetFailureThreshold(5).
			SetSuccessThreshold(3).
			SetOnFailover(func() { failoverCalled = true }).
			SetOnRecovered(func() { recoveredCalled = true }).
			SetOnCheckFailure(func(client string, err error) { checkFailureCalled = true })

		assert.Equal(t, 10*time.Second, opts.CheckInterval)
		assert.Equal(t, 2*time.Second, opts.CheckTimeout)
		assert.Equal(t, 5, opts.FailureThreshold)
		assert.Equal(t, 3, opts.SuccessThreshold)

		opts.OnFailover()
		assert.True(t, failoverCalled)
		opts.OnRecovered()
		assert.True(t, recoveredCalled)
		opts.OnCheckFailure("test", nil)
		assert.True(t, checkFailureCalled)
	})
}

func TestFailoverClient_FailoverAndRecovery(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}

	var failedOver atomic.Int64
	var recovered atomic.Int64
	var primaryCheckFailures atomic.Int64
	var secondaryCheckFailures atomic.Int64

	opts := FailoverOptions().
		SetCheckInterval(10 * time.Millisecond).
		SetCheckTimeout(5 * time.Millisecond).
		SetFailureThreshold(2).
		SetSuccessThreshold(2).
		SetOnFailover(func() { failedOver.Add(1) }).
		SetOnRecovered(func() { recovered.Add(1) }).
		SetOnCheckFailure(func(client string, err error) {
			if client == "primary" {
				primaryCheckFailures.Add(1)
			} else if client == "secondary" {
				secondaryCheckFailures.Add(1)
			}
		})

	fc := newFailoverClient(c1, c2, time.Millisecond, opts)
	defer fc.Close()

	assert.True(t, fc.IsPrimary())

	// Fail primary
	c1.setPingErr(errors.New("primary down"))

	// Wait for failover (2 failures needed)
	assert.Eventually(t, func() bool {
		return fc.IsSecondary()
	}, 1*time.Second, 20*time.Millisecond)

	assert.Equal(t, int64(1), failedOver.Load())
	assert.Equal(t, c2, fc.Active())
	assert.GreaterOrEqual(t, primaryCheckFailures.Load(), int64(2))

	// Recover primary
	c1.setPingErr(nil)

	// Wait for recovery (2 successes needed)
	assert.Eventually(t, func() bool {
		return fc.IsPrimary()
	}, 1*time.Second, 20*time.Millisecond)

	assert.Equal(t, int64(1), recovered.Load())
	assert.Equal(t, c1, fc.Active())

	// Test secondary failure
	c2.setPingErr(errors.New("secondary down"))
	assert.Eventually(t, func() bool {
		return secondaryCheckFailures.Load() > 0
	}, 1*time.Second, 20*time.Millisecond)

	// Test primary timeout
	c2.setPingErr(nil)
	c1.setPingDelay(100 * time.Millisecond)
	// SuccessThreshold is 2, so it will take some time to recover first
	assert.Eventually(t, func() bool {
		return fc.IsSecondary()
	}, 1*time.Second, 20*time.Millisecond)
}

func TestFailoverClient_Delegation(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := redis.NewClient(&redis.Options{Addr: s1.Addr()})
	c2 := redis.NewClient(&redis.Options{Addr: s2.Addr()})

	fc := NewFailoverClient(c1, c2, FailoverOptions().SetCheckInterval(time.Hour))
	defer fc.Close()

	ctx := context.Background()

	// Set
	err := fc.Set(ctx, "k1", "v1", 0).Err()
	assert.NoError(t, err)

	// Get
	val, err := fc.Get(ctx, "k1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	// GetEx
	val, err = fc.GetEx(ctx, "k1", time.Minute).Result()
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	// GetDel
	val, err = fc.GetDel(ctx, "k1").Result()
	assert.NoError(t, err)
	assert.Equal(t, "v1", val)

	// MSet
	err = fc.MSet(ctx, "k2", "v2", "k3", "v3").Err()
	assert.NoError(t, err)

	// MGet
	vals, err := fc.MGet(ctx, "k2", "k3").Result()
	assert.NoError(t, err)
	assert.Equal(t, []any{"v2", "v3"}, vals)

	// SetNX
	ok, err := fc.SetNX(ctx, "k4", "v4", 0).Result()
	assert.NoError(t, err)
	assert.True(t, ok)

	// SetXX
	ok, err = fc.SetXX(ctx, "k4", "v4-new", 0).Result()
	assert.NoError(t, err)
	assert.True(t, ok)

	// Del
	count, err := fc.Del(ctx, "k4").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// TTL
	fc.Set(ctx, "k5", "v5", time.Minute)
	ttl, err := fc.TTL(ctx, "k5").Result()
	assert.NoError(t, err)
	assert.Greater(t, ttl, time.Duration(0))

	// Expire
	ok, err = fc.Expire(ctx, "k5", time.Hour).Result()
	assert.NoError(t, err)
	assert.True(t, ok)

	// Scan
	keys, cursor, err := fc.Scan(ctx, 0, "*", 10).Result()
	assert.NoError(t, err)
	assert.Contains(t, keys, "k2")
	assert.Contains(t, keys, "k3")
	assert.Equal(t, uint64(0), cursor)

	// Ping
	res, err := fc.Ping(ctx).Result()
	assert.NoError(t, err)
	assert.Equal(t, "PONG", res)

	// FlushDB
	err = fc.FlushDB(ctx).Err()
	assert.NoError(t, err)

	// FlushDBAsync
	err = fc.FlushDBAsync(ctx).Err()
	assert.NoError(t, err)

	// Verify primary was used
	assert.Empty(t, s2.DB(0).Keys())
}

func TestFailoverClient_PipelineAndWatch(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := redis.NewClient(&redis.Options{Addr: s1.Addr()})
	c2 := redis.NewClient(&redis.Options{Addr: s2.Addr()})

	fc := NewFailoverClient(c1, c2, FailoverOptions().SetCheckInterval(time.Hour))
	defer fc.Close()

	ctx := context.Background()

	t.Run("Pipeline", func(t *testing.T) {
		pipe := fc.Pipeline()
		pipe.Set(ctx, "pk1", "pv1", 0)
		pipe.Get(ctx, "pk1")
		cmds, err := pipe.Exec(ctx)
		assert.NoError(t, err)
		assert.Len(t, cmds, 2)
		assert.Equal(t, "pv1", cmds[1].(*redis.StringCmd).Val())
	})

	t.Run("Watch", func(t *testing.T) {
		err := fc.Watch(ctx, func(tx *redis.Tx) error {
			_, err := tx.Set(ctx, "wk1", "wv1", 0).Result()
			return err
		}, "wk1")
		assert.NoError(t, err)

		val, _ := s1.Get("wk1")
		assert.Equal(t, "wv1", val)
	})
}

func TestNewFailoverClient_InvalidOptions(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := redis.NewClient(&redis.Options{Addr: s1.Addr()})
	c2 := redis.NewClient(&redis.Options{Addr: s2.Addr()})
	defer c1.Close()
	defer c2.Close()

	tests := []struct {
		name string
		opts *FailoverOpts
	}{
		{name: "negative CheckInterval", opts: &FailoverOpts{CheckInterval: -time.Second}},
		{name: "negative CheckTimeout", opts: &FailoverOpts{CheckTimeout: -time.Second}},
		{name: "negative FailureThreshold", opts: &FailoverOpts{FailureThreshold: -1}},
		{name: "negative SuccessThreshold", opts: &FailoverOpts{SuccessThreshold: -1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Panics(t, func() {
				NewFailoverClient(c1, c2, tt.opts)
			})
		})
	}
}

func TestNewFailoverClient_NormalizesMergedOptions(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := redis.NewClient(&redis.Options{Addr: s1.Addr()})
	c2 := redis.NewClient(&redis.Options{Addr: s2.Addr()})

	t.Run("sub-second interval is rounded up", func(t *testing.T) {
		fc := NewFailoverClient(c1, c2, &FailoverOpts{CheckInterval: 100 * time.Millisecond})
		assert.Equal(t, time.Second, fc.config.CheckInterval)
		assert.Equal(t, 500*time.Millisecond, fc.config.CheckTimeout)
		close(fc.closeChan)
		<-fc.done
	})

	t.Run("timeout greater than or equal to interval is adjusted", func(t *testing.T) {
		fc := NewFailoverClient(c1, c2, &FailoverOpts{
			CheckInterval: 2 * time.Second,
			CheckTimeout:  3 * time.Second,
		})
		assert.Equal(t, 2*time.Second, fc.config.CheckInterval)
		assert.Equal(t, time.Second, fc.config.CheckTimeout)
		close(fc.closeChan)
		<-fc.done
	})

	t.Run("zero values keep defaults", func(t *testing.T) {
		fc := NewFailoverClient(c1, c2, &FailoverOpts{FailureThreshold: 7}, nil)
		assert.Equal(t, 5*time.Second, fc.config.CheckInterval)
		assert.Equal(t, time.Second, fc.config.CheckTimeout)
		assert.Equal(t, 7, fc.config.FailureThreshold)
		assert.Equal(t, 2, fc.config.SuccessThreshold)
		fc.Close()
	})
}

func TestFailoverClient_NoFailoverToUnhealthySecondary(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}

	var failovers, secondaryFailures atomic.Int64
	opts := FailoverOptions().
		SetCheckInterval(5 * time.Millisecond).
		SetCheckTimeout(2 * time.Millisecond).
		SetFailureThreshold(1).
		SetOnFailover(func() { failovers.Add(1) }).
		SetOnCheckFailure(func(client string, err error) {
			if client == "secondary" {
				secondaryFailures.Add(1)
			}
		})

	c1.setPingErr(errors.New("primary down"))
	c2.setPingErr(errors.New("secondary down"))

	fc := newFailoverClient(c1, c2, time.Millisecond, opts)
	defer fc.Close()

	assert.Eventually(t, func() bool {
		return secondaryFailures.Load() >= 5
	}, time.Second, 5*time.Millisecond)
	assert.True(t, fc.IsPrimary())
	assert.Equal(t, int64(0), failovers.Load())

	// Once the secondary is healthy again failover happens.
	c2.setPingErr(nil)
	assert.Eventually(t, func() bool {
		return fc.IsSecondary()
	}, time.Second, 5*time.Millisecond)
	assert.Equal(t, int64(1), failovers.Load())
}

func TestFailoverClient_PanickingCallbacks(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}

	opts := FailoverOptions().
		SetCheckInterval(5 * time.Millisecond).
		SetCheckTimeout(2 * time.Millisecond).
		SetFailureThreshold(1).
		SetSuccessThreshold(1).
		SetOnFailover(func() { panic("failover") }).
		SetOnRecovered(func() { panic("recovered") }).
		SetOnCheckFailure(func(client string, err error) { panic("check failure") })

	fc := newFailoverClient(c1, c2, time.Millisecond, opts)
	defer fc.Close()

	c1.setPingErr(errors.New("primary down"))
	assert.Eventually(t, fc.IsSecondary, time.Second, 5*time.Millisecond)

	c1.setPingErr(nil)
	assert.Eventually(t, fc.IsPrimary, time.Second, 5*time.Millisecond)
}

func TestFailoverClient_ConcurrentUseDuringSwitch(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}

	var switches atomic.Int64
	opts := FailoverOptions().
		SetCheckInterval(time.Millisecond).
		SetCheckTimeout(500 * time.Microsecond).
		SetFailureThreshold(1).
		SetSuccessThreshold(1).
		SetOnFailover(func() { switches.Add(1) }).
		SetOnRecovered(func() { switches.Add(1) })

	fc := newFailoverClient(c1, c2, time.Microsecond, opts)
	defer fc.Close()

	ctx := context.Background()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
				}
				_ = fc.Get(ctx, "key").Err()
				_ = fc.IsPrimary()
				_ = fc.Active()
			}
		})
	}

	// Toggle the primary health so the active client flips repeatedly.
	deadline := time.Now().Add(2 * time.Second)
	for switches.Load() < 4 && time.Now().Before(deadline) {
		if fc.IsPrimary() {
			c1.setPingErr(errors.New("primary down"))
		} else {
			c1.setPingErr(nil)
		}
		time.Sleep(5 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	assert.GreaterOrEqual(t, switches.Load(), int64(4))
}

func TestFailoverClient_CloseWaitsForMonitor(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	// Health checks are in flight most of the time, and no callback runs.
	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}
	c1.setPingDelay(10 * time.Millisecond)

	opts := FailoverOptions().
		SetCheckInterval(time.Millisecond).
		SetCheckTimeout(100 * time.Millisecond)

	fc := newFailoverClient(c1, c2, time.Microsecond, opts)
	time.Sleep(5 * time.Millisecond)

	assert.NoError(t, fc.Close())

	select {
	case <-fc.done:
	default:
		t.Fatal("monitor goroutine still running after Close returned")
	}
}

func TestFailoverClient_NoCallbacksAfterClose(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}
	c1.setPingErr(errors.New("primary down"))

	var checkFailures atomic.Int64
	opts := FailoverOptions().
		SetCheckInterval(time.Millisecond).
		SetCheckTimeout(500 * time.Microsecond).
		SetOnCheckFailure(func(client string, err error) { checkFailures.Add(1) })

	fc := newFailoverClient(c1, c2, time.Microsecond, opts)
	assert.Eventually(t, func() bool {
		return checkFailures.Load() > 0
	}, time.Second, time.Millisecond)

	assert.NoError(t, fc.Close())

	// Close doesn't wait for a callback that is running when it is called.
	select {
	case <-fc.done:
	case <-time.After(time.Second):
		t.Fatal("monitor goroutine didn't stop after Close")
	}

	afterClose := checkFailures.Load()
	time.Sleep(20 * time.Millisecond)
	assert.Equal(t, afterClose, checkFailures.Load())
}

func TestFailoverClient_CloseFromCallback(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s1.Addr()})}
	c2 := &controlledClient{Client: redis.NewClient(&redis.Options{Addr: s2.Addr()})}
	c1.setPingErr(errors.New("primary down"))

	var fc *FailoverClient
	created := make(chan struct{})
	closed := make(chan error, 1)
	opts := FailoverOptions().
		SetCheckInterval(time.Millisecond).
		SetCheckTimeout(500 * time.Microsecond).
		SetFailureThreshold(1).
		SetOnFailover(func() {
			<-created
			closed <- fc.Close()
		})

	fc = newFailoverClient(c1, c2, time.Microsecond, opts)
	close(created)

	select {
	case err := <-closed:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close called from a callback deadlocked")
	}

	// The monitor goroutine stops once the callback returns, and calling Close
	// again doesn't block.
	select {
	case <-fc.done:
	case <-time.After(time.Second):
		t.Fatal("monitor goroutine didn't stop after Close")
	}
	assert.NoError(t, fc.Close())

	ctx := context.Background()
	assert.Error(t, c1.Client.Ping(ctx).Err())
	assert.Error(t, c2.Client.Ping(ctx).Err())
}

func TestFailoverClient_Close(t *testing.T) {
	s1 := miniredis.RunT(t)
	s2 := miniredis.RunT(t)

	c1 := redis.NewClient(&redis.Options{Addr: s1.Addr()})
	c2 := redis.NewClient(&redis.Options{Addr: s2.Addr()})

	fc := NewFailoverClient(c1, c2, FailoverOptions().SetCheckInterval(10*time.Millisecond))

	err := fc.Close()
	assert.NoError(t, err)

	// Calling multiple times should be safe
	err = fc.Close()
	assert.NoError(t, err)

	// Verification that clients are closed is hard without mocking Close,
	// but redis.Client.Ping will return error after Close.
	ctx := context.Background()
	assert.Error(t, c1.Ping(ctx).Err())
	assert.Error(t, c2.Ping(ctx).Err())
}
