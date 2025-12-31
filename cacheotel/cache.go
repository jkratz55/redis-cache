package cacheotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

// todo: Add func to support custom histogram boundaries

type Cache interface {
	Get(ctx context.Context, key string, v any) error
	GetAndExpire(ctx context.Context, key string, v any, ttl time.Duration) error
	Keys(ctx context.Context) ([]string, error)
	Set(ctx context.Context, key string, v any, ttl time.Duration) error
	SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error)
	SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error)
	MSet(ctx context.Context, keyvalues map[string]any) error
	Delete(ctx context.Context, keys ...string) error
	Flush(ctx context.Context) error
	FlushAsync(ctx context.Context) error
	Ping(ctx context.Context) bool
	TTL(ctx context.Context, key string) (time.Duration, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
	Scan(ctx context.Context, pattern string, batch int64) ([]string, error)
}

type InstrumentedCache struct {
	next              Cache
	executionDuration metric.Float64Histogram
}

func InstrumentCache(c Cache) *InstrumentedCache {
	return &InstrumentedCache{
		next:              c,
		executionDuration: executionDuration,
	}
}

func (i *InstrumentedCache) Get(ctx context.Context, key string, v any) error {
	start := time.Now()
	err := i.next.Get(ctx, key, v)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Get")))
	return err
}

func (i *InstrumentedCache) GetAndExpire(ctx context.Context, key string, v any, ttl time.Duration) error {
	start := time.Now()
	err := i.next.GetAndExpire(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "GetAndExpire")))
	return err
}

func (i *InstrumentedCache) Keys(ctx context.Context) ([]string, error) {
	start := time.Now()
	res, err := i.next.Keys(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Keys")))
	return res, err
}

func (i *InstrumentedCache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	start := time.Now()
	err := i.next.Set(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Set")))
	return err
}

func (i *InstrumentedCache) SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	start := time.Now()
	res, err := i.next.SetIfAbsent(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfAbsent")))
	return res, err
}

func (i *InstrumentedCache) SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	start := time.Now()
	res, err := i.next.SetIfPresent(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfPresent")))
	return res, err
}

func (i *InstrumentedCache) MSet(ctx context.Context, keyvalues map[string]any) error {
	start := time.Now()
	err := i.next.MSet(ctx, keyvalues)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MSet")))
	return err
}

func (i *InstrumentedCache) Delete(ctx context.Context, keys ...string) error {
	start := time.Now()
	err := i.next.Delete(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Delete")))
	return err
}

func (i *InstrumentedCache) Flush(ctx context.Context) error {
	start := time.Now()
	err := i.next.Flush(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Flush")))
	return err
}

func (i *InstrumentedCache) FlushAsync(ctx context.Context) error {
	start := time.Now()
	err := i.next.FlushAsync(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "FlushAsync")))
	return err
}

func (i *InstrumentedCache) Ping(ctx context.Context) bool {
	start := time.Now()
	res := i.next.Ping(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", res),
			attribute.String("operation", "Ping")))
	return res
}

func (i *InstrumentedCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	start := time.Now()
	res, err := i.next.TTL(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "TTL")))
	return res, err
}

func (i *InstrumentedCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	start := time.Now()
	err := i.next.Expire(ctx, key, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Expire")))
	return err
}

func (i *InstrumentedCache) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	start := time.Now()
	res, err := i.next.Scan(ctx, pattern, batch)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Scan")))
	return res, err
}

var (
	executionDuration metric.Float64Histogram
	mgetKeys          metric.Int64Histogram
)

func init() {
	meterProvider := otel.GetMeterProvider()
	meter := meterProvider.Meter(scope)

	var err error
	executionDuration, err = meter.Float64Histogram("cache.execution_duration",
		metric.WithDescription("Duration of cache operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.005, 0.010, 0.025, 0.050, 0.100)) // 5ms, 10ms, 25ms, 50ms, 100ms
	if err != nil {
		panic(err)
	}

	mgetKeys, err = meter.Int64Histogram("cache.mget_keys",
		metric.WithDescription("Number of keys used in MGetMap operation"),
		metric.WithExplicitBucketBoundaries(100, 250, 500, 1000, 2500, 5000, 10000, 20000, 40000, 80000))
	if err != nil {
		panic(err)
	}
}

// MGet retrieves multiple keys in a single operation and returns the result as a slice.
//
// If a key doesn't exist in Redis it will not be present in the result slice. Due to missing keys
// being omitted it isn't possible to correlate the keys with their values. If you need to fetch
// multiple keys and their relationship between each other is important, use MGetMap instead.
func MGet[T any](ctx context.Context, rdb *cache.Cache, keys ...string) ([]T, error) {
	mgetKeys.Record(ctx, int64(len(keys)))

	start := time.Now()
	res, err := cache.MGet[T](ctx, rdb, keys...)
	executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "MGetMap"),
			attribute.Bool("success", err == nil)))
	return res, err
}

// MGetMap retrieves multiple keys in a single operation and returns the result as a map key -> value.
//
// If a key doesn't exist in Redis it will not be present in the result map.
//
// MGetMap is useful when you want to fetch multiple keys and their relationship between each other
// is important. For example, if you want to fetch a user's profile and their friends, you can use
// MGetMap to fetch both the profile and the friends in a single operation. If you just want the values
// and don't care about the relationship between the keys, use MGet instead.
func MGetMap[T any](ctx context.Context, rdb *cache.Cache, keys ...string) (map[string]T, error) {
	mgetKeys.Record(ctx, int64(len(keys)))

	start := time.Now()
	res, err := cache.MGetMap[T](ctx, rdb, keys...)
	executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "MGetMap"),
			attribute.Bool("success", err == nil)))
	return res, err
}

// Upsert retrieves the existing value for a given key and invokes the UpsertCallback.
// The UpsertCallback function is responsible for determining the value to be stored.
// The value returned from the UpsertCallback is what is set in Redis.
//
// Upsert allows for atomic updates of existing records, or simply inserting new
// entries when the key doesn't exist.
//
// Redis uses an optimistic locking model. If the key changes during the transaction
// Redis will fail the transaction and return an error. However, these errors are
// retryable. To determine if the error is retryable use the IsRetryable function
// with the returned error.
//
//	cb := rcache.UpsertCallback[Person](func(found bool, oldValue Person, newValue Person) Person {
//		fmt.Println(found)
//		fmt.Println(oldValue)
//		fmt.Println(newValue)
//		return newValue
//	})
//	retries := 3
//	for i := 0; i < retries; i++ {
//		err := rcache.UpsertTTL[Person](context.Background(), c, "BillyBob", p, cb, time.Minute * 1)
//		if rcache.IsRetryable(err) {
//			continue
//		}
//		// do something useful ...
//		break
//	}
func Upsert[T any](ctx context.Context, c *cache.Cache, key string, val T, cb cache.UpsertCallback[T], ttl time.Duration) error {
	start := time.Now()
	err := cache.Upsert[T](ctx, c, key, val, cb, ttl)
	executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "Upsert"),
			attribute.Bool("success", err == nil)))
	return err
}

type TypedCache[T any] interface {
	Get(ctx context.Context, key string) (T, error)
	GetAndExpire(ctx context.Context, key string, ttl time.Duration) (T, error)
	GetAndDelete(ctx context.Context, key string) (T, error)
	MGet(ctx context.Context, keys ...string) ([]T, error)
	MGetMap(ctx context.Context, keys ...string) (cache.MultiResult[T], error)
	Set(ctx context.Context, key string, val T, ttl time.Duration) error
	SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error)
	SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error)
	MSet(ctx context.Context, values map[string]T) error
	Delete(ctx context.Context, keys ...string) error
	FlushDB(ctx context.Context) error
	FlushDBAsync(ctx context.Context) error
	TTL(ctx context.Context, key string) (time.Duration, error)
	Expire(ctx context.Context, key string, ttl time.Duration) error
	Scan(ctx context.Context, pattern string, batch int64) ([]string, error)
}

type InstrumentedTypedCache[T any] struct {
	next              TypedCache[T]
	executionDuration metric.Float64Histogram
	mgetKeys          metric.Int64Histogram
}

func InstrumentTypedCache[T any](c TypedCache[T]) *InstrumentedTypedCache[T] {
	return &InstrumentedTypedCache[T]{
		next:              c,
		executionDuration: executionDuration,
		mgetKeys:          mgetKeys,
	}
}

func (i *InstrumentedTypedCache[T]) Get(ctx context.Context, key string) (T, error) {
	start := time.Now()
	res, err := i.next.Get(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Get")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) GetAndExpire(ctx context.Context, key string, ttl time.Duration) (T, error) {
	start := time.Now()
	res, err := i.next.GetAndExpire(ctx, key, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "GetAndExpire")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) GetAndDelete(ctx context.Context, key string) (T, error) {
	start := time.Now()
	res, err := i.next.GetAndDelete(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "GetAndDelete")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) MGet(ctx context.Context, keys ...string) ([]T, error) {
	start := time.Now()
	res, err := i.next.MGet(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MGet")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) MGetMap(ctx context.Context, keys ...string) (cache.MultiResult[T], error) {
	start := time.Now()
	res, err := i.next.MGetMap(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MGetMap")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	start := time.Now()
	err := i.next.Set(ctx, key, val, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Set")))
	return err
}

func (i *InstrumentedTypedCache[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	start := time.Now()
	res, err := i.next.SetIfAbsent(ctx, key, val, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfAbsent")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	start := time.Now()
	res, err := i.next.SetIfPresent(ctx, key, val, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfPresent")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) MSet(ctx context.Context, values map[string]T) error {
	start := time.Now()
	err := i.next.MSet(ctx, values)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MSet")))
	return err
}

func (i *InstrumentedTypedCache[T]) Delete(ctx context.Context, keys ...string) error {
	start := time.Now()
	err := i.next.Delete(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Delete")))
	return err
}

func (i *InstrumentedTypedCache[T]) FlushDB(ctx context.Context) error {
	start := time.Now()
	err := i.next.FlushDB(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "FlushDB")))
	return err
}

func (i *InstrumentedTypedCache[T]) FlushDBAsync(ctx context.Context) error {
	start := time.Now()
	err := i.next.FlushDBAsync(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "FlushDBAsync")))
	return err
}

func (i *InstrumentedTypedCache[T]) TTL(ctx context.Context, key string) (time.Duration, error) {
	start := time.Now()
	res, err := i.next.TTL(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "TTL")))
	return res, err
}

func (i *InstrumentedTypedCache[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	start := time.Now()
	err := i.next.Expire(ctx, key, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Expire")))
	return err
}

func (i *InstrumentedTypedCache[T]) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	start := time.Now()
	res, err := i.next.Scan(ctx, pattern, batch)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Scan")))
	return res, err
}
