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

	// HealthCheck checks the health of the primary or secondary RedisClient. A non-nil error counts
	// as a failed health check. ctx is canceled once CheckTimeout has elapsed.
	//
	// The default sends PING, which only verifies Redis is reachable. A Redis that answers PING but
	// rejects writes, for example because it reached its memory limit or became a read-only replica,
	// passes it. A HealthCheck that writes a key detects these cases as well:
	//
	//	func(ctx context.Context, client cache.RedisClient) error {
	//		return client.Set(ctx, "health:canary", "ok", time.Minute).Err()
	//	}
	//
	// HealthCheck is invoked from the monitoring goroutine for both clients concurrently. A panic in
	// HealthCheck is recovered and counts as a failed health check.
	//
	// Default sends PING.
	HealthCheck func(ctx context.Context, client RedisClient) error
}

// pingHealthCheck is the default HealthCheck.
func pingHealthCheck(ctx context.Context, client RedisClient) error {
	return client.Ping(ctx).Err()
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
		HealthCheck:      pingHealthCheck,
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
	if f.HealthCheck == nil {
		f.HealthCheck = pingHealthCheck
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

// SetHealthCheck sets the function checking the health of the primary and secondary RedisClient.
// See FailoverOpts.HealthCheck.
//
// Default sends PING.
func (f *FailoverOpts) SetHealthCheck(fn func(ctx context.Context, client RedisClient) error) *FailoverOpts {
	f.HealthCheck = fn
	return f
}

// FailoverClient is an implementation of RedisClient that will automatically failover to a secondary
// RedisClient if the primary RedisClient is not available.
//
// FailoverClient expects the primary and secondary Redis to replicate each other's writes in both
// directions (active-active replication), such as Redis Enterprise Active-Active databases. Every
// command is only sent to the active RedisClient, FailoverClient doesn't copy writes, deletes or
// expirations to the other one. If the primary and secondary aren't replicated, writes made while
// failed over never reach the primary, so after recovering FailoverClient serves the values the
// primary held before the outage, which can be stale and may never expire. The same applies to the
// secondary on the next failover.
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
//
// FailoverClient takes ownership of the primary and secondary RedisClient: Close closes both of them.
type FailoverClient struct {
	// active is either primaryRef or secondaryRef. It is the only state describing
	// which client is active, so IsPrimary, IsSecondary and Active always agree.
	active       atomic.Pointer[clientRef]
	primary      RedisClient
	secondary    RedisClient
	primaryRef   *clientRef
	secondaryRef *clientRef
	config       *FailoverOpts
	closeChan    chan struct{}
	done         chan struct{}

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
// The primary and secondary Redis are expected to replicate each other's writes, see FailoverClient.
// The FailoverClient takes ownership of primary and secondary, and closes them when it is closed.
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
		if o.HealthCheck != nil {
			failoverOpts.HealthCheck = o.HealthCheck
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
// the goroutine it runs in, such as the monitor goroutine, and with it the process.
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

	checkFn := func(client RedisClient) (err error) {
		ctx, cancel := context.WithTimeout(context.Background(), f.config.CheckTimeout)
		defer cancel()

		// The check runs in a goroutine started by the monitor, so a panic in a
		// custom HealthCheck would crash the process.
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("health check panic: %v", r)
			}
		}()
		return f.config.HealthCheck(ctx, client)
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
				primaryErr = checkFn(f.primary)
				wg.Done()
			}()
			go func() {
				secondaryErr = checkFn(f.secondary)
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

			failedOver := f.IsSecondary()
			// Only failover if the secondary is healthy, otherwise stay on the primary and
			// keep counting failures so failover happens once the secondary is healthy.
			if failures >= int64(f.config.FailureThreshold) && !failedOver && secondaryErr == nil {
				f.active.Store(f.secondaryRef)
				f.callback(f.config.OnFailover)
				failures = 0
				successes = 0
			} else if successes >= int64(f.config.SuccessThreshold) && failedOver {
				f.active.Store(f.primaryRef)
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

// Get calls Get on the active RedisClient.
func (f *FailoverClient) Get(ctx context.Context, key string) *redis.StringCmd {
	return f.current().Get(ctx, key)
}

// GetEx calls GetEx on the active RedisClient.
func (f *FailoverClient) GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd {
	return f.current().GetEx(ctx, key, expiration)
}

// GetDel calls GetDel on the active RedisClient.
func (f *FailoverClient) GetDel(ctx context.Context, key string) *redis.StringCmd {
	return f.current().GetDel(ctx, key)
}

// MGet calls MGet on the active RedisClient.
func (f *FailoverClient) MGet(ctx context.Context, keys ...string) *redis.SliceCmd {
	return f.current().MGet(ctx, keys...)
}

// Set calls Set on the active RedisClient.
func (f *FailoverClient) Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd {
	return f.current().Set(ctx, key, val, ttl)
}

// SetNX calls SetNX on the active RedisClient.
func (f *FailoverClient) SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return f.current().SetNX(ctx, key, value, expiration)
}

// SetXX calls SetXX on the active RedisClient.
func (f *FailoverClient) SetXX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd {
	return f.current().SetXX(ctx, key, value, expiration)
}

// MSet calls MSet on the active RedisClient.
func (f *FailoverClient) MSet(ctx context.Context, values ...any) *redis.StatusCmd {
	return f.current().MSet(ctx, values...)
}

// Del calls Del on the active RedisClient.
func (f *FailoverClient) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	return f.current().Del(ctx, keys...)
}

// Watch calls Watch on the active RedisClient.
func (f *FailoverClient) Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error {
	return f.current().Watch(ctx, fn, keys...)
}

// Scan calls Scan on the active RedisClient.
func (f *FailoverClient) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	return f.current().Scan(ctx, cursor, match, count)
}

// FlushDB calls FlushDB on the active RedisClient.
func (f *FailoverClient) FlushDB(ctx context.Context) *redis.StatusCmd {
	return f.current().FlushDB(ctx)
}

// FlushDBAsync calls FlushDBAsync on the active RedisClient.
func (f *FailoverClient) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	return f.current().FlushDBAsync(ctx)
}

// Ping calls Ping on the active RedisClient.
func (f *FailoverClient) Ping(ctx context.Context) *redis.StatusCmd {
	return f.current().Ping(ctx)
}

// TTL calls TTL on the active RedisClient.
func (f *FailoverClient) TTL(ctx context.Context, key string) *redis.DurationCmd {
	return f.current().TTL(ctx, key)
}

// PTTL calls PTTL on the active RedisClient.
func (f *FailoverClient) PTTL(ctx context.Context, key string) *redis.DurationCmd {
	return f.current().PTTL(ctx, key)
}

// Expire calls Expire on the active RedisClient.
func (f *FailoverClient) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return f.current().Expire(ctx, key, expiration)
}

// PExpire calls PExpire on the active RedisClient.
func (f *FailoverClient) PExpire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	return f.current().PExpire(ctx, key, expiration)
}

// Persist calls Persist on the active RedisClient.
func (f *FailoverClient) Persist(ctx context.Context, key string) *redis.BoolCmd {
	return f.current().Persist(ctx, key)
}

// Pipeline calls Pipeline on the active RedisClient.
func (f *FailoverClient) Pipeline() redis.Pipeliner {
	return f.current().Pipeline()
}

// Close stops the failover client from monitoring the primary and secondary Redis clients
// and closes the underlying Redis clients, which FailoverClient owns. Don't use the primary or
// secondary RedisClient once Close has been called. No callbacks are invoked after Close is called.
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
	return f.active.Load() == f.primaryRef
}

// IsSecondary returns true if the failover client is currently using the secondary Redis client.
func (f *FailoverClient) IsSecondary() bool {
	return f.active.Load() == f.secondaryRef
}

// Active returns the active RedisClient.
func (f *FailoverClient) Active() RedisClient {
	return f.current()
}
