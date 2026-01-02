package cacheotel

import (
	"context"
	"errors"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	cache "github.com/jkratz55/redis-cache/v2"
)

// Init configures the OpenTelemetry metrics for the cacheotel package.
//
// Note: You only need to call Init if you want to override the default MeterProvider and configuration.
// Init should only be called once. Calling Init multiple times can lead to non-determinitic results and
// behavior.
func Init(opts ...Option) {
	conf := newConfig()
	for _, opt := range opts {
		opt(conf)
	}

	meter := conf.meterProvider.Meter(scope)
	tracer = conf.traceProvider.Tracer(scope)

	var err error
	executionDuration, err = meter.Float64Histogram("cache.execution_duration",
		metric.WithDescription("Duration of cache operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(conf.durationBoundaries...))
	if err != nil {
		panic(err)
	}

	mgetKeys, err = meter.Int64Histogram("cache.mget_keys",
		metric.WithDescription("Number of keys used in MGetMap operation"),
		metric.WithExplicitBucketBoundaries(conf.mgetKeyBoundaries...))
	if err != nil {
		panic(err)
	}
}

type InstrumentedCache struct {
	next              *cache.Cache
	executionDuration metric.Float64Histogram
}

func InstrumentCache(c *cache.Cache) *InstrumentedCache {
	return &InstrumentedCache{
		next:              c,
		executionDuration: executionDuration,
	}
}

func (i *InstrumentedCache) Get(ctx context.Context, key string, v any) error {
	ctx, span := tracer.Start(ctx, "cache.get", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Get(ctx, key, v)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", isSuccess(err)),
			attribute.String("operation", "Get")))

	hit := err == nil

	span.SetAttributes(
		attribute.Bool("cache.hit", hit),
	)

	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) GetAndExpire(ctx context.Context, key string, v any, ttl time.Duration) error {
	ctx, span := tracer.Start(ctx, "cache.get_and_expire", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.GetAndExpire(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", isSuccess(err)),
			attribute.String("operation", "GetAndExpire")))

	hit := err == nil

	span.SetAttributes(
		attribute.Bool("cache.hit", hit),
	)

	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) GetAndDelete(ctx context.Context, key string, v any) error {
	ctx, span := tracer.Start(ctx, "cache.get_and_delete", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.GetAndDelete(ctx, key, v)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", isSuccess(err)),
			attribute.String("operation", "GetAndDelete")))

	hit := err == nil

	span.SetAttributes(
		attribute.Bool("cache.hit", hit),
	)

	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) Keys(ctx context.Context) ([]string, error) {
	ctx, span := tracer.Start(ctx, "cache.keys", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.Keys(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Keys")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedCache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	ctx, span := tracer.Start(ctx, "cache.set", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Set(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Set")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	ctx, span := tracer.Start(ctx, "cache.set_if_absent", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.SetIfAbsent(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfAbsent")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedCache) SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	ctx, span := tracer.Start(ctx, "cache.set_if_present", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.SetIfPresent(ctx, key, v, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfPresent")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedCache) MSet(ctx context.Context, keyvalues map[string]any) error {
	ctx, span := tracer.Start(ctx, "cache.mset", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.MSet(ctx, keyvalues)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MSet")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) Delete(ctx context.Context, keys ...string) error {
	ctx, span := tracer.Start(ctx, "cache.delete", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Delete(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Delete")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) Flush(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "cache.flush", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Flush(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Flush")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) FlushAsync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "cache.flush_async", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.FlushAsync(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "FlushAsync")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) TTL(ctx context.Context, key string) (time.Duration, error) {
	ctx, span := tracer.Start(ctx, "cache.ttl", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.TTL(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "TTL")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedCache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ctx, span := tracer.Start(ctx, "cache.expire", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Expire(ctx, key, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Expire")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedCache) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	ctx, span := tracer.Start(ctx, "cache.scan", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.Scan(ctx, pattern, batch)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Scan")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

var (
	executionDuration metric.Float64Histogram
	mgetKeys          metric.Int64Histogram
	tracer            trace.Tracer
)

func init() {
	// Initializes OpenTelemetry meter and metrics with defaults
	Init()
}

// MGet retrieves multiple keys in a single operation and returns the result as a slice.
//
// If a key doesn't exist in Redis it will not be present in the result slice. Due to missing keys
// being omitted it isn't possible to correlate the keys with their values. If you need to fetch
// multiple keys and their relationship between each other is important, use MGetMap instead.
func MGet[T any](ctx context.Context, rdb *InstrumentedCache, keys ...string) ([]T, error) {
	ctx, span := tracer.Start(ctx, "cache.mget", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	mgetKeys.Record(ctx, int64(len(keys)))
	span.SetAttributes(attribute.Int("mget.keys.count", len(keys)))

	start := time.Now()
	res, err := cache.MGet[T](ctx, rdb.next, keys...)
	executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "MGet"),
			attribute.Bool("success", err == nil)))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

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
func MGetMap[T any](ctx context.Context, rdb *InstrumentedCache, keys ...string) (map[string]T, error) {
	ctx, span := tracer.Start(ctx, "cache.mget_map", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	mgetKeys.Record(ctx, int64(len(keys)))
	span.SetAttributes(attribute.Int("mget.keys.count", len(keys)))

	start := time.Now()
	res, err := cache.MGetMap[T](ctx, rdb.next, keys...)
	executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "MGetMap"),
			attribute.Bool("success", err == nil)))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

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
func Upsert[T any](ctx context.Context, c *InstrumentedCache, key string, val T, cb cache.UpsertCallback[T], ttl time.Duration) error {
	ctx, span := tracer.Start(ctx, "cache.upsert", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := cache.Upsert[T](ctx, c.next, key, val, cb, ttl)
	executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "Upsert"),
			attribute.Bool("success", err == nil)))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

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
	ctx, span := tracer.Start(ctx, "typedcache.get", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.Get(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", isSuccess(err)),
			attribute.String("operation", "Get")))

	hit := err == nil

	span.SetAttributes(
		attribute.Bool("cache.hit", hit),
	)

	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) GetAndExpire(ctx context.Context, key string, ttl time.Duration) (T, error) {
	ctx, span := tracer.Start(ctx, "typedcache.get_and_expire", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.GetAndExpire(ctx, key, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", isSuccess(err)),
			attribute.String("operation", "GetAndExpire")))

	hit := err == nil

	span.SetAttributes(
		attribute.Bool("cache.hit", hit),
	)

	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) GetAndDelete(ctx context.Context, key string) (T, error) {
	ctx, span := tracer.Start(ctx, "typedcache.get_and_delete", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.GetAndDelete(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", isSuccess(err)),
			attribute.String("operation", "GetAndDelete")))

	hit := err == nil

	span.SetAttributes(
		attribute.Bool("cache.hit", hit),
	)

	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) MGet(ctx context.Context, keys ...string) ([]T, error) {
	ctx, span := tracer.Start(ctx, "typedcache.mget", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.MGet(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MGet")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) MGetMap(ctx context.Context, keys ...string) (cache.MultiResult[T], error) {
	ctx, span := tracer.Start(ctx, "typedcache.mget_map", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.MGetMap(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MGetMap")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	ctx, span := tracer.Start(ctx, "typedcache.set", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Set(ctx, key, val, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Set")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedTypedCache[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	ctx, span := tracer.Start(ctx, "typedcache.set_if_absent", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.SetIfAbsent(ctx, key, val, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfAbsent")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	ctx, span := tracer.Start(ctx, "typedcache.set_if_present", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.SetIfPresent(ctx, key, val, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "SetIfPresent")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) MSet(ctx context.Context, values map[string]T) error {
	ctx, span := tracer.Start(ctx, "typedcache.mset", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.MSet(ctx, values)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "MSet")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedTypedCache[T]) Delete(ctx context.Context, keys ...string) error {
	ctx, span := tracer.Start(ctx, "typedcache.delete", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Delete(ctx, keys...)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Delete")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedTypedCache[T]) FlushDB(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "typedcache.flushdb", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.FlushDB(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "FlushDB")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedTypedCache[T]) FlushDBAsync(ctx context.Context) error {
	ctx, span := tracer.Start(ctx, "typedcache.flushdb_async", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.FlushDBAsync(ctx)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "FlushDBAsync")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedTypedCache[T]) TTL(ctx context.Context, key string) (time.Duration, error) {
	ctx, span := tracer.Start(ctx, "typedcache.ttl", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.TTL(ctx, key)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "TTL")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func (i *InstrumentedTypedCache[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ctx, span := tracer.Start(ctx, "typedcache.expire", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	err := i.next.Expire(ctx, key, ttl)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Expire")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return err
}

func (i *InstrumentedTypedCache[T]) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	ctx, span := tracer.Start(ctx, "typedcache.scan", trace.WithSpanKind(trace.SpanKindClient))
	defer span.End()

	start := time.Now()
	res, err := i.next.Scan(ctx, pattern, batch)
	i.executionDuration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.Bool("success", err == nil),
			attribute.String("operation", "Scan")))

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}

	return res, err
}

func isSuccess(err error) bool {
	if err == nil {
		return true
	}

	// A cache miss is not considered an error.
	if errors.Is(err, cache.ErrKeyNotFound) {
		return true
	}

	return false
}
