package cache

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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
	// up to time.Second. NewFailoverClient panics if CheckInterval is negative.
	//
	// Default is 5 seconds.
	CheckInterval time.Duration

	// CheckTimeout is the timeout for the health check.
	//
	// CheckTimeout must be less than CheckInterval. If CheckTimeout is greater than or equal to CheckInterval,
	// it will be adjusted to half of CheckInterval. NewFailoverClient panics if CheckTimeout is negative.
	//
	// Default is 1 second.
	CheckTimeout time.Duration

	// FailureThreshold is the number of consecutive failures that must occur before FailoverClient
	// will failover to the secondary RedisClient. FailoverClient only fails over if the secondary
	// RedisClient passed its last health check. NewFailoverClient panics if FailureThreshold is negative.
	//
	// Default is 3.
	FailureThreshold int

	// SuccessThreshold is the number of consecutive successes that must occur before FailoverClient
	// will return to the primary RedisClient after failing over to the secondary RedisClient.
	// NewFailoverClient panics if SuccessThreshold is negative.
	//
	// Default is 2.
	SuccessThreshold int

	// OnFailover is a callback invoked when FailoverClient detects the primary RedisClient is not
	// available and is falling back to the secondary RedisClient.
	//
	// Callbacks are invoked from the monitoring goroutine and must not block. A panic in a callback
	// is recovered and ignored. No callbacks are invoked after Close returns.
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

// minFailoverCheckInterval is the smallest CheckInterval allowed. Values below are
// rounded up to it.
const minFailoverCheckInterval = time.Second

// validate panics if any of the durations or thresholds are negative.
func (f *FailoverOpts) validate() {
	if f.CheckInterval < 0 {
		panic(fmt.Errorf("invalid FailoverOpts: negative CheckInterval %s", f.CheckInterval))
	}
	if f.CheckTimeout < 0 {
		panic(fmt.Errorf("invalid FailoverOpts: negative CheckTimeout %s", f.CheckTimeout))
	}
	if f.FailureThreshold < 0 {
		panic(fmt.Errorf("invalid FailoverOpts: negative FailureThreshold %d", f.FailureThreshold))
	}
	if f.SuccessThreshold < 0 {
		panic(fmt.Errorf("invalid FailoverOpts: negative SuccessThreshold %d", f.SuccessThreshold))
	}
}

func (f *FailoverOpts) init() {
	f.normalize(minFailoverCheckInterval)
}

// normalize applies defaults to zero values and clamps CheckInterval to minInterval
// and CheckTimeout to less than CheckInterval.
func (f *FailoverOpts) normalize(minInterval time.Duration) {
	if f.CheckInterval <= 0 {
		f.CheckInterval = 5 * time.Second
	}
	if f.CheckInterval < minInterval {
		f.CheckInterval = minInterval
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
// The active RedisClient is swapped atomically, so no lock is taken in the hot path. Operations
// that are in flight while FailoverClient switches between the primary and secondary RedisClient
// complete on the client they started on.
type FailoverClient struct {
	active       atomic.Pointer[clientRef]
	primary      RedisClient
	secondary    RedisClient
	primaryRef   *clientRef
	secondaryRef *clientRef
	config       *FailoverOpts
	closeChan    chan struct{}
	done         chan struct{}
	failedOver   atomic.Bool

	// mu guards closed and inCallback, which ensure no callback starts after Close
	// and that Close doesn't wait for a callback that is running, which may be the
	// caller of Close.
	mu         sync.Mutex
	closed     bool
	inCallback bool
}

// clientRef wraps a RedisClient so it can be stored in an atomic.Pointer.
type clientRef struct {
	RedisClient
}

// NewFailoverClient returns a new FailoverClient that will automatically failover to a secondary
// RedisClient if the primary RedisClient is not available.
//
// FailoverOpts can be provided to customize the default behavior of FailoverClient.
//
// Providing a nil RedisClient for either primary or secondary arguments will result in a panic.
// NewFailoverClient also panics if any duration or threshold in FailoverOpts is negative.
func NewFailoverClient(primary, secondary RedisClient, opt ...*FailoverOpts) *FailoverClient {
	return newFailoverClient(primary, secondary, minFailoverCheckInterval, opt...)
}

// newFailoverClient is NewFailoverClient with a configurable minimum CheckInterval,
// which allows tests to use short intervals.
func newFailoverClient(primary, secondary RedisClient, minInterval time.Duration, opt ...*FailoverOpts) *FailoverClient {

	if primary == nil {
		panic("primary RedisClient cannot be nil")
	}
	if secondary == nil {
		panic("secondary RedisClient cannot be nil")
	}

	failoverOpts := FailoverOptions()

	for _, o := range opt {
		if o == nil {
			continue
		}
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

	failoverOpts.validate()
	failoverOpts.normalize(minInterval)

	failoverClient := &FailoverClient{
		primary:      primary,
		secondary:    secondary,
		primaryRef:   &clientRef{RedisClient: primary},
		secondaryRef: &clientRef{RedisClient: secondary},
		config:       failoverOpts,
		closeChan:    make(chan struct{}),
		done:         make(chan struct{}),
	}
	failoverClient.active.Store(failoverClient.primaryRef)

	go failoverClient.monitorClients()

	return failoverClient
}

// current returns the active RedisClient.
func (f *FailoverClient) current() RedisClient {
	return f.active.Load().RedisClient
}

// safeCall invokes fn and recovers any panic so a misbehaving callback can't crash
// the monitor goroutine and with it the process.
func safeCall(fn func()) {
	defer func() {
		_ = recover()
	}()
	fn()
}

// callback invokes the callback fn unless Close has been called.
func (f *FailoverClient) callback(fn func()) {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return
	}
	f.inCallback = true
	f.mu.Unlock()

	defer func() {
		f.mu.Lock()
		f.inCallback = false
		f.mu.Unlock()
	}()
	safeCall(fn)
}

func (f *FailoverClient) monitorClients() {
	defer close(f.done)

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

			// Don't act on the results or invoke callbacks once Close has been called.
			select {
			case <-f.closeChan:
				return
			default:
			}

			if primaryErr != nil {
				failures++
				successes = 0
				f.callback(func() { f.config.OnCheckFailure("primary", primaryErr) })
			} else {
				failures = 0
				successes++
			}

			failedOver := f.failedOver.Load()
			// Only failover if the secondary is healthy, otherwise stay on the primary and
			// keep counting failures so failover happens once the secondary is healthy.
			if failures >= int64(f.config.FailureThreshold) && !failedOver && secondaryErr == nil {
				f.active.Store(f.secondaryRef)
				f.failedOver.Store(true)
				f.callback(f.config.OnFailover)
				failures = 0
				successes = 0
			} else if successes >= int64(f.config.SuccessThreshold) && failedOver {
				f.active.Store(f.primaryRef)
				f.failedOver.Store(false)
				f.callback(f.config.OnRecovered)
				failures = 0
				successes = 0
			}

			if secondaryErr != nil {
				f.callback(func() { f.config.OnCheckFailure("secondary", secondaryErr) })
			}

		case <-f.closeChan:
			return
		}
	}
}

func (f *FailoverClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return f.current().Get(ctx, key)
}

func (f *FailoverClient) GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd {
	return f.current().GetEx(ctx, key, expiration)
}

func (f *FailoverClient) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return f.current().GetDel(ctx, key)
}

func (f *FailoverClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return f.current().MGet(ctx, keys...)
}

func (f *FailoverClient) Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd {
	return f.current().Set(ctx, key, val, ttl)
}

func (f *FailoverClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return f.current().SetNX(ctx, key, value, expiration)
}

func (f *FailoverClient) SetXX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return f.current().SetXX(ctx, key, value, expiration)
}

func (f *FailoverClient) MSet(ctx context.Context, values ...any) *redis.StatusCmd {
	return f.current().MSet(ctx, values...)
}

func (f *FailoverClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return f.current().Del(ctx, keys...)
}

func (f *FailoverClient) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return f.current().Watch(ctx, fn, keys...)
}

func (f *FailoverClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return f.current().Scan(ctx, cursor, match, count)
}

func (f *FailoverClient) FlushDB(ctx context.Context) *redis.StatusCmd {
	return f.current().FlushDB(ctx)
}

func (f *FailoverClient) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	return f.current().FlushDBAsync(ctx)
}

func (f *FailoverClient) Ping(ctx context.Context) *redis.StatusCmd {
	return f.current().Ping(ctx)
}

func (f *FailoverClient) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return f.current().TTL(ctx, key)
}

func (f *FailoverClient) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return f.current().PTTL(ctx, key)
}

func (f *FailoverClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return f.current().Expire(ctx, key, expiration)
}

func (f *FailoverClient) PExpire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return f.current().PExpire(ctx, key, expiration)
}

func (f *FailoverClient) Persist(ctx context.Context, key string) *redis.BoolCmd {
	return f.current().Persist(ctx, key)
}

func (f *FailoverClient) Pipeline() redis.Pipeliner {
	return f.current().Pipeline()
}

// Close stops the failover client from monitoring the primary and secondary Redis clients
// and closes the underlying Redis clients. No callbacks are invoked after Close is called.
// Close waits for the monitoring goroutine to stop, unless a callback is running, so a
// callback can call Close. In that case the running callback may still be running when
// Close returns.
//
// Close is a no-op if Close has already been called.
func (f *FailoverClient) Close() error {
	f.mu.Lock()
	if f.closed {
		f.mu.Unlock()
		return nil
	}
	f.closed = true
	wait := !f.inCallback
	f.mu.Unlock()

	close(f.closeChan)
	if wait {
		<-f.done
	}
	return errors.Join(f.primary.Close(), f.secondary.Close())
}

// IsPrimary returns true if the failover client is currently using the primary Redis client.
func (f *FailoverClient) IsPrimary() bool {
	return !f.failedOver.Load()
}

// IsSecondary returns true if the failover client is currently using the secondary Redis client.
func (f *FailoverClient) IsSecondary() bool {
	return f.failedOver.Load()
}

// Active returns the active RedisClient.
func (f *FailoverClient) Active() RedisClient {
	return f.current()
}
