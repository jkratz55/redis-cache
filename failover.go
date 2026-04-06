package cache

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// FailoverOpts configures the behavior of the FailoverClient.
//
// FailoverOpts can be initialized manually, but it is recommended to use FailoverOptions.
type FailoverOpts struct {

	// CheckInterval is the interval at which the FailoverClient will check the health of the primary
	// and secondary RedisClient.
	//
	// CheckInterval must be greater than time.Second. Values lower than time.Second will be rounded
	// up to time.Second.
	//
	// Default is 5 seconds.
	CheckInterval time.Duration

	// CheckTimeout is the timeout for the health check.
	//
	// CheckTimeout must be less than CheckInterval. If CheckTimeout is greater than or equal to CheckInterval,
	// it will be adjusted to be less than CheckInterval.
	//
	// Default is 1 second.
	CheckTimeout time.Duration

	// FailureThreshold is the number of consecutive failures that must occur before FailoverClient
	// will failover to the secondary RedisClient.
	//
	// Default is 3.
	FailureThreshold int

	// SuccessThreshold is the number of consecutive successes that must occur before FailoverClient
	// will return to the primary RedisClient after failing over to the secondary RedisClient.
	//
	// Default is 2.
	SuccessThreshold int

	// OnFailover is a callback invoked when FailoverClient detects the primary RedisClient is not
	// available and is falling back to the secondary RedisClient.
	//
	// Default is a no-op.
	OnFailover func()

	// OnRecovered is a callback invoked when FailoverClient detects the primary RedisClient is
	// available again and flips back to the primary RedisClient.
	//
	// Default is a no-op.
	OnRecovered func()

	// OnCheckFailure is a callback invoked when FailoverClient detects the health check for either
	// the primary or secondary RedisClient has failed.
	//
	// Default is a no-op.
	OnCheckFailure func(client string, err error)
}

// FailoverOptions returns a new FailoverOpts with default values.
func FailoverOptions() *FailoverOpts {
	return &FailoverOpts{
		CheckInterval:    5 * time.Second,
		CheckTimeout:     1 * time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OnFailover:       func() {},
		OnRecovered:      func() {},
		OnCheckFailure:   func(client string, err error) {},
	}
}

func (f *FailoverOpts) init() {
	if f.CheckInterval <= 0 {
		f.CheckInterval = 5 * time.Second
	}
	if f.CheckInterval < time.Second {
		f.CheckInterval = time.Second
	}

	if f.CheckTimeout <= 0 {
		f.CheckTimeout = 1 * time.Second
	}
	if f.CheckTimeout >= f.CheckInterval {
		f.CheckTimeout = f.CheckInterval / 2
	}

	if f.FailureThreshold <= 0 {
		f.FailureThreshold = 3
	}
	if f.SuccessThreshold <= 0 {
		f.SuccessThreshold = 2
	}
	if f.OnFailover == nil {
		f.OnFailover = func() {}
	}
	if f.OnRecovered == nil {
		f.OnRecovered = func() {}
	}
	if f.OnCheckFailure == nil {
		f.OnCheckFailure = func(client string, err error) {}
	}
}

// SetCheckInterval sets the interval at which the FailoverClient will check the health of the primary
// and secondary RedisClient.
//
// CheckInterval must be greater than time.Second. Values lower than time.Second will be rounded
// up to time.Second.
//
// Default is 5 seconds.
func (f *FailoverOpts) SetCheckInterval(interval time.Duration) *FailoverOpts {
	f.CheckInterval = interval
	return f
}

// SetCheckTimeout sets the timeout for the health check.
//
// CheckTimeout must be less than CheckInterval. If CheckTimeout is greater than or equal to CheckInterval,
// it will be adjusted to be less than CheckInterval.
//
// Default is 1 second.
func (f *FailoverOpts) SetCheckTimeout(timeout time.Duration) *FailoverOpts {
	f.CheckTimeout = timeout
	return f
}

// SetFailureThreshold sets the number of consecutive failures that must occur before FailoverClient
// will failover to the secondary RedisClient.
//
// Default is 3.
func (f *FailoverOpts) SetFailureThreshold(threshold int) *FailoverOpts {
	f.FailureThreshold = threshold
	return f
}

// SetSuccessThreshold sets the number of consecutive successes that must occur before FailoverClient
// will return to the primary RedisClient after failing over to the secondary RedisClient.
//
// Default is 2.
func (f *FailoverOpts) SetSuccessThreshold(threshold int) *FailoverOpts {
	f.SuccessThreshold = threshold
	return f
}

// SetOnFailover sets the callback invoked when FailoverClient detects the primary RedisClient is not
// available and is falling back to the secondary RedisClient.
//
// Default is a no-op.
func (f *FailoverOpts) SetOnFailover(fn func()) *FailoverOpts {
	f.OnFailover = fn
	return f
}

// SetOnRecovered sets the callback invoked when FailoverClient detects the primary RedisClient is
// available again and flips back to the primary RedisClient.
//
// Default is a no-op.
func (f *FailoverOpts) SetOnRecovered(fn func()) *FailoverOpts {
	f.OnRecovered = fn
	return f
}

// SetOnCheckFailure sets the callback invoked when FailoverClient detects the health check for either
// the primary or secondary RedisClient has failed.
//
// Default is a no-op.
func (f *FailoverOpts) SetOnCheckFailure(fn func(client string, err error)) *FailoverOpts {
	f.OnCheckFailure = fn
	return f
}

// FailoverClient is an implementation of RedisClient that will automatically failover to a secondary
// RedisClient if the primary RedisClient is not available.
//
// FailoverClient is safe for concurrent use by multiple goroutines.
//
// The zero-value of FailoverClient is not usable. Use NewFailoverClient to create a new instance.
//
// FailoverOpts can be provided to NewFailoverClient to customize the behavior of FailoverClient.
// By default, FailoverClient will ping Redis every 5 seconds with a 1-second timeout. If the
// primary RedisClient fails 3 consecutive health checks, it will be considered unhealthy and
// FailoverClient will failover to the secondary RedisClient. Once the primary RedisClient passes
// two consecutive health checks, it will be considered healthy and FailoverClient will return to
// the primary RedisClient.
//
// Note: The active RedisClient is intentionally not protected by a mutex to avoid adding a lock
// in the hot path. That means it is possible for goroutines to get different Redis clients during
// transitions between primary and secondary clients.
type FailoverClient struct {
	active     RedisClient
	primary    RedisClient
	secondary  RedisClient
	config     *FailoverOpts
	closeChan  chan struct{}
	failedOver bool
	closeOnce  sync.Once
}

// NewFailoverClient returns a new FailoverClient that will automatically failover to a secondary
// RedisClient if the primary RedisClient is not available.
//
// FailoverOpts can be provided to customize the default behavior of FailoverClient.
//
// Providing a nil RedisClient for either primary or secondary arguments will result in a panic.
func NewFailoverClient(primary, secondary RedisClient, opt ...*FailoverOpts) *FailoverClient {

	if primary == nil {
		panic("primary RedisClient cannot be nil")
	}
	if secondary == nil {
		panic("secondary RedisClient cannot be nil")
	}

	failoverOpts := FailoverOptions()
	failoverOpts.init()

	for _, o := range opt {
		if o.CheckInterval != 0 {
			failoverOpts.CheckInterval = o.CheckInterval
		}
		if o.CheckTimeout != 0 {
			failoverOpts.CheckTimeout = o.CheckTimeout
		}
		if o.FailureThreshold != 0 {
			failoverOpts.FailureThreshold = o.FailureThreshold
		}
		if o.SuccessThreshold != 0 {
			failoverOpts.SuccessThreshold = o.SuccessThreshold
		}
		if o.OnFailover != nil {
			failoverOpts.OnFailover = o.OnFailover
		}
		if o.OnRecovered != nil {
			failoverOpts.OnRecovered = o.OnRecovered
		}
		if o.OnCheckFailure != nil {
			failoverOpts.OnCheckFailure = o.OnCheckFailure
		}
	}

	failoverClient := &FailoverClient{
		active:    primary,
		primary:   primary,
		secondary: secondary,
		config:    failoverOpts,
		closeChan: make(chan struct{}, 1),
	}

	go failoverClient.monitorClients()

	return failoverClient
}

func (f *FailoverClient) monitorClients() {
	ticker := time.NewTicker(f.config.CheckInterval)
	defer ticker.Stop()

	pingFn := func(client RedisClient, timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		return client.Ping(ctx).Err()
	}

	var (
		failures  int64
		successes int64
	)

	for {
		select {
		case <-ticker.C:
			var (
				wg           sync.WaitGroup
				primaryErr   error
				secondaryErr error
			)
			wg.Add(2)
			go func() {
				primaryErr = pingFn(f.primary, f.config.CheckTimeout)
				wg.Done()
			}()
			go func() {
				secondaryErr = pingFn(f.secondary, f.config.CheckTimeout)
				wg.Done()
			}()
			wg.Wait()

			if primaryErr != nil {
				failures++
				successes = 0
				f.config.OnCheckFailure("primary", primaryErr)
			} else {
				failures = 0
				successes++
			}

			if failures >= int64(f.config.FailureThreshold) && !f.failedOver {
				f.failedOver = true
				f.active = f.secondary
				f.config.OnFailover()
				failures = 0
				successes = 0
			} else if successes >= int64(f.config.SuccessThreshold) && f.failedOver {
				f.failedOver = false
				f.active = f.primary
				f.config.OnRecovered()
				failures = 0
				successes = 0
			}

			if secondaryErr != nil {
				f.config.OnCheckFailure("secondary", secondaryErr)
			}

		case <-f.closeChan:
			return
		}
	}
}

func (f *FailoverClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return f.active.Get(ctx, key)
}

func (f *FailoverClient) GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd {
	return f.active.GetEx(ctx, key, expiration)
}

func (f *FailoverClient) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return f.active.GetDel(ctx, key)
}

func (f *FailoverClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return f.active.MGet(ctx, keys...)
}

func (f *FailoverClient) Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd {
	return f.active.Set(ctx, key, val, ttl)
}

func (f *FailoverClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return f.active.SetNX(ctx, key, value, expiration)
}

func (f *FailoverClient) SetXX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return f.active.SetXX(ctx, key, value, expiration)
}

func (f *FailoverClient) MSet(ctx context.Context, values ...any) *redis.StatusCmd {
	return f.active.MSet(ctx, values...)
}

func (f *FailoverClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return f.active.Del(ctx, keys...)
}

func (f *FailoverClient) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return f.active.Watch(ctx, fn, keys...)
}

func (f *FailoverClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return f.active.Scan(ctx, cursor, match, count)
}

func (f *FailoverClient) FlushDB(ctx context.Context) *redis.StatusCmd {
	return f.active.FlushDB(ctx)
}

func (f *FailoverClient) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	return f.active.FlushDBAsync(ctx)
}

func (f *FailoverClient) Ping(ctx context.Context) *redis.StatusCmd {
	return f.active.Ping(ctx)
}

func (f *FailoverClient) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return f.active.TTL(ctx, key)
}

func (f *FailoverClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return f.active.Expire(ctx, key, expiration)
}

func (f *FailoverClient) Pipeline() redis.Pipeliner {
	return f.active.Pipeline()
}

// Close stops the failover client from monitoring the primary and secondary Redis clients
// and closes the underlying Redis clients.
//
// Close is a no-op if Close has already been called.
func (f *FailoverClient) Close() error {
	var err error
	f.closeOnce.Do(func() {
		f.closeChan <- struct{}{}
		close(f.closeChan)
		err = errors.Join(f.primary.Close(), f.secondary.Close())
	})
	return err
}

// IsPrimary returns true if the failover client is currently using the primary Redis client.
func (f *FailoverClient) IsPrimary() bool {
	return !f.failedOver
}

// IsSecondary returns true if the failover client is currently using the secondary Redis client.
func (f *FailoverClient) IsSecondary() bool {
	return f.failedOver
}

// Active returns the active RedisClient.
func (f *FailoverClient) Active() RedisClient {
	return f.active
}
