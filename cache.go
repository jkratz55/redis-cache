package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/rueidis"
)

const (
	// InfiniteTTL indicates a key will never expire.
	//
	// Depending on Redis configuration keys may still be evicted if Redis is
	// under memory pressure in accordance to the eviction policy configured.
	InfiniteTTL time.Duration = -3

	// KeepTTL indicates to keep the existing TTL on the key on SET commands.
	KeepTTL = -1
)

var (
	// ErrKeyNotFound is an error value that signals the key requested does not
	// exist in the cache.
	ErrKeyNotFound = errors.New("key not found")
)

// Cache is a simple type that provides basic caching functionality: store, retrieve,
// and delete. It is backed by Redis and supports storing entries with a TTL.
//
// The zero-value is not usable, and this type should be instantiated using the
// New function.
type Cache struct {
	redis            rueidis.Client
	marshaller       Marshaller
	unmarshaller     Unmarshaller
	codec            Codec
	mgetBatch        int // zero-value indicates no batching
	nearCacheEnabled bool
	nearCacheTTL     time.Duration
	hooksMixin
}

// New creates and initializes a new Cache instance.
//
// By default, msgpack is used for marshalling and unmarshalling the entry values.
// The behavior of Cache can be configured by passing Options.
func New(client rueidis.Client, opts ...Option) *Cache {
	if client == nil {
		panic(fmt.Errorf("a valid redis client is required, illegal use of api"))
	}
	cache := &Cache{
		redis:        client,
		marshaller:   DefaultMarshaller(),
		unmarshaller: DefaultUnmarshaller(),
		codec:        nopCodec{},
	}
	for _, opt := range opts {
		opt(cache)
	}

	cache.hooksMixin = hooksMixin{
		initial: hooks{
			marshal:    cache.marshaller,
			unmarshall: cache.unmarshaller,
			compress:   cache.codec.Flate,
			decompress: cache.codec.Deflate,
		},
	}
	cache.chain()

	return cache
}

// Get retrieves an entry from the Cache for the given key, and if found will
// unmarshall the value into v.
//
// If the key does not exist ErrKeyNotFound will be returned as the error value.
// A non-nil error value will be returned if the operation on the backing Redis
// fails, or if the value cannot be unmarshalled into the target type.
func (c *Cache) Get(ctx context.Context, key string, v any) error {
	var (
		data []byte
		err  error
	)

	cmd := c.redis.B().Get().Key(key)
	if c.nearCacheEnabled {
		data, err = c.redis.DoCache(ctx, cmd.Cache(), c.nearCacheTTL).AsBytes()
	} else {
		data, err = c.redis.Do(ctx, cmd.Build()).AsBytes()
	}
	if err != nil {
		if errors.Is(err, rueidis.Nil) {
			return ErrKeyNotFound
		}
		return fmt.Errorf("redis: %w", err)
	}
	data, err = c.hooksMixin.current.decompress(data)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}
	if err := c.hooksMixin.current.unmarshall(data, v); err != nil {
		return fmt.Errorf("unmarshall value: %w", err)
	}
	return nil
}

// GetAndExpire retrieves a value from the Cache for the given key, decompresses it if
// applicable, unmarshalls the value to v, and sets the TTL for the key.
//
// If the key does not exist ErrKeyNotFound will be returned as the error value.
// A non-nil error value will be returned if the operation on the backing Redis
// fails, or if the value cannot be unmarshalled into the target type.
//
// Passing a negative ttl value has no effect on the existing TTL for the key.
// Passing a ttl value of 0 or InfiniteTTL removes the TTL and persist the key
// until it is either explicitly deleted or evicted according to Redis eviction
// policy.
func (c *Cache) GetAndExpire(ctx context.Context, key string, v any, ttl time.Duration) error {
	// This is a bit wonky since tll values are used different in different places
	// in go-redis and Redis. So here we map InfiniteTTL to 0, so it keeps the same
	// semantic meaning through the package.
	if ttl == InfiniteTTL {
		ttl = 0
	}

	cmd := c.redis.B().Getex().Key(key).Ex(ttl).Build()
	val, err := c.redis.Do(ctx, cmd).AsBytes()

	if err != nil {
		if errors.Is(err, rueidis.Nil) {
			return ErrKeyNotFound
		}
		return fmt.Errorf("redis: %w", err)
	}

	data, err := c.hooksMixin.current.decompress(val)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}
	if err := c.hooksMixin.current.unmarshall(data, v); err != nil {
		return fmt.Errorf("unmarshall value: %w", err)
	}
	return nil
}

// Keys retrieves all the keys in Redis/Cache
func (c *Cache) Keys(ctx context.Context) ([]string, error) {
	cursor := uint64(0)
	keys := make([]string, 0)
	for {
		result, err := c.redis.Do(ctx, c.redis.B().Scan().Cursor(cursor).Count(1000).Build()).AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}

		cursor = result.Cursor
		keys = append(keys, result.Elements...)

		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

// ScanKeys allows for scanning keys in Redis using a pattern.
func (c *Cache) ScanKeys(ctx context.Context, pattern string) ([]string, error) {
	cursor := uint64(0)
	keys := make([]string, 0)
	for {
		result, err := c.redis.Do(ctx, c.redis.B().Scan().Cursor(cursor).
			Match(pattern).Count(1000).Build()).AsScanEntry()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}

		cursor = result.Cursor
		keys = append(keys, result.Elements...)

		if cursor == 0 {
			break
		}
	}

	return keys, nil
}

// Set adds an entry into the cache, or overwrites an entry if the key already
// existed.
func (c *Cache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := c.hooksMixin.current.marshal(v)
	if err != nil {
		return fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.hooksMixin.current.compress(data)
	if err != nil {
		return fmt.Errorf("compress value: %w", err)
	}

	cmd := c.redis.B().Set().Key(key).Value(string(data))
	if ttl > 0 {
		cmd.Ex(ttl)
	}

	err = c.redis.Do(ctx, cmd.Build()).Error()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return err
}

// SetIfAbsent adds an entry into the cache only if the key doesn't already exist.
// The entry is set with the provided TTL and automatically removed from the cache
// once the TTL is expired.
func (c *Cache) SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	data, err := c.hooksMixin.current.marshal(v)
	if err != nil {
		return false, fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.hooksMixin.current.compress(data)
	if err != nil {
		return false, fmt.Errorf("compress value: %w", err)
	}

	cmd := c.redis.B().Set().Key(key).Value(string(data)).Nx()
	if ttl > 0 {
		cmd.Ex(ttl)
	}

	ok, err := c.redis.Do(ctx, cmd.Build()).AsBool()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return ok, err
}

// SetIfPresent updates an entry into the cache if they key already exists in the
// cache. The entry is set with the provided TTL and automatically removed from the
// cache once the TTL is expired.
func (c *Cache) SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	data, err := c.hooksMixin.current.marshal(v)
	if err != nil {
		return false, fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.hooksMixin.current.compress(data)
	if err != nil {
		return false, fmt.Errorf("compress value: %w", err)
	}

	cmd := c.redis.B().Set().Key(key).Value(string(data)).Xx()
	if ttl > 0 {
		cmd.Ex(ttl)
	}

	ok, err := c.redis.Do(ctx, cmd.Build()).AsBool()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return ok, err
}

// MSet performs multiple SET operations. Entries are added to the cache or
// overridden if they already exists.
//
// MSet is atomic, either all keyvalues are set or none are set. Since MSet
// operates using a single atomic command it is the fastest way to bulk write
// entries to the Cache. It greatly reduces network overhead and latency when
// compared to calling SET sequentially.
func (c *Cache) MSet(ctx context.Context, keyvalues map[string]any) error {
	// The key and values needs to be processed prior to calling MSet by marshalling
	// and compressing the values.
	cmd := c.redis.B().Mset().KeyValue()
	for k, v := range keyvalues {
		val, err := c.hooksMixin.current.marshal(v)
		if err != nil {
			return fmt.Errorf("marshal value: %w", err)
		}
		val, err = c.hooksMixin.current.compress(val)
		if err != nil {
			return fmt.Errorf("compress value: %w", err)
		}
		cmd.KeyValue(k, string(val))
	}

	if err := c.redis.Do(ctx, cmd.Build()).Error(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

// Delete removes entries from the cache for a given set of keys.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.redis.Do(ctx, c.redis.B().Del().Key(keys...).Build()).Error()
}

// Flush flushes the cache deleting all keys/entries.
func (c *Cache) Flush(ctx context.Context) error {
	return c.redis.Do(ctx, c.redis.B().Flushdb().Sync().Build()).Error()
}

// FlushAsync flushes the cache deleting all keys/entries asynchronously. Only keys
// that were present when FLUSH ASYNC command was received by Redis will be deleted.
// Any keys created during asynchronous flush will be unaffected.
func (c *Cache) FlushAsync(ctx context.Context) error {
	return c.redis.Do(ctx, c.redis.B().Flushdb().Async().Build()).Error()
}

// Healthy pings Redis to ensure it is reachable and responding. Healthy returns
// true if Redis successfully responds to the ping, otherwise false.
func (c *Cache) Healthy(ctx context.Context) bool {
	return c.redis.Do(ctx, c.redis.B().Ping().Build()).Error() == nil
}

// TTL returns the time to live for a particular key.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value.
// If the key doesn't have a TTL InfiniteTTL will be returned.
func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	dur, err := c.redis.Do(ctx, c.redis.B().Ttl().Key(key).Build()).AsInt64() // c.redis.TTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: %w", err)
	}

	// Redis returns -1 for TTL command to indicate there is no TTL on the key
	if dur == -1 {
		return InfiniteTTL, nil
	}
	// Redis returns -2 for TTL command if the key doesn't exist
	if dur == -2 {
		return 0, ErrKeyNotFound
	}
	return time.Duration(dur) * time.Second, nil
}

// Expire sets a TTL on the given key.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value.
// If the key already has a TTL it will be overridden with ttl value provided.
//
// Calling Expire with a non-positive ttl will result in the key being deleted.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ok, err := c.redis.Do(ctx, c.redis.B().Expire().Key(key).
		Seconds(int64(ttl.Seconds())).Build()).AsBool()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	if !ok {
		return ErrKeyNotFound
	}
	return nil
}

// ExtendTTL extends the TTL for the key by the given duration.
//
// ExtendTTL retrieves the TTL remaining for the key, adds the duration, and then
// executes the EXPIRE command to set a new TTL.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value.
func (c *Cache) ExtendTTL(ctx context.Context, key string, dur time.Duration) error {
	ttl, err := c.redis.Do(ctx, c.redis.B().Ttl().Key(key).Build()).AsInt64()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	// Redis returns -2 for TTL command if the key doesn't exist
	if ttl == -2 {
		return ErrKeyNotFound
	}

	return c.Expire(ctx, key, time.Duration(ttl)*time.Second+dur)
}

// Client returns the underlying Redis client the Cache is wrapping/using.
//
// This can be useful when there are operations that are not supported by Cache
// that are required, but you don't want to have to pass the Redis client around
// your application as well. This allows for the Redis client to be accessed from
// the Cache. However, it is important to understand that when using the Redis
// client directly it will not have the same behavior as the Cache. You will have
// to handle marshalling, unmarshalling, and compression yourself. You will also
// not have the same hooks behavior as the Cache and some metrics will not be
// tracked.
func (c *Cache) Client() rueidis.Client {
	return c.redis
}

// MultiResult is a type representing returning multiple entries from the Cache.
type MultiResult[T any] map[string]T

// Keys returns all the keys found.
func (mr MultiResult[T]) Keys() []string {
	keys := make([]string, 0, len(mr))
	for key := range mr {
		keys = append(keys, key)
	}
	return keys
}

// Values returns all the values found.
func (mr MultiResult[T]) Values() []T {
	values := make([]T, 0, len(mr))
	for _, val := range mr {
		values = append(values, val)
	}
	return values
}

// Get returns the value and a boolean indicating if the key exists. If the key
// doesn't exist the value will be the default zero value.
func (mr MultiResult[T]) Get(key string) (T, bool) {
	val, ok := mr[key]
	return val, ok
}

// IsEmpty returns a boolean indicating if the results are empty.
func (mr MultiResult[T]) IsEmpty() bool {
	return len(mr) == 0
}

// MGet uses the provided Cache to retrieve multiple keys from Redis and returns
// a MultiResult.
//
// If a key doesn't exist in Redis it will not be included in the MultiResult
// returned. If all keys are not found the MultiResult will be empty.
func MGet[R any](ctx context.Context, c *Cache, keys ...string) (MultiResult[R], error) {

	// If batching is enabled and the number of keys exceeds the batch size use
	// multiple MGET commands in a pipeline.
	if c.mgetBatch > 0 && len(keys) > c.mgetBatch {
		return mGetBatch[R](ctx, c, keys...)
	}

	var (
		results []string
		err     error
	)

	cmd := c.redis.B().Mget().Key(keys...)
	if c.nearCacheEnabled {
		results, err = c.redis.DoCache(ctx, cmd.Cache(), c.nearCacheTTL).AsStrSlice()
	} else {
		results, err = c.redis.Do(ctx, cmd.Build()).AsStrSlice()
	}

	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	resultMap := make(map[string]R)
	for i, res := range results {
		if res == "" {
			// Some or all of the requested keys may not exist. Skip iterations
			// where the key wasn't found
			continue
		}
		data, err := c.hooksMixin.current.decompress([]byte(res))
		if err != nil {
			return nil, fmt.Errorf("decompress value: %w", err)
		}
		var val R
		if err := c.hooksMixin.current.unmarshall(data, &val); err != nil {
			return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
		}
		resultMap[keys[i]] = val
	}
	return resultMap, nil
}

// mGetBatch is a helper function that uses pipelining to fetch multiple keys
// from Redis in batches. Under certain conditions, this can be faster than
// fetching all keys in a single MGET command.
func mGetBatch[R any](ctx context.Context, c *Cache, keys ...string) (MultiResult[R], error) {

	chunks := chunk(keys, c.mgetBatch)
	resultMap := make(map[string]R)
	var redisResults []rueidis.RedisResult

	// Using near caching/local caching requires handling the calls to rueidis
	// differently.
	if c.nearCacheEnabled {
		cmds := make([]rueidis.CacheableTTL, 0, len(chunks))
		for i := 0; i < len(chunks); i++ {
			cmds = append(cmds, rueidis.CacheableTTL{
				Cmd: c.redis.B().Mget().Key(chunks[i]...).Cache(),
				TTL: c.nearCacheTTL,
			})
		}
		redisResults = c.redis.DoMultiCache(ctx, cmds...)
	} else {
		cmds := make([]rueidis.Completed, 0, len(chunks))
		for i := 0; i < len(chunks); i++ {
			cmds = append(cmds, c.redis.B().Mget().Key(chunks[i]...).Build())
		}
		redisResults = c.redis.DoMulti(ctx, cmds...)
	}

	for i := 0; i < len(redisResults); i++ {
		results, err := redisResults[i].AsStrSlice()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for j := 0; j < len(results); j++ {
			res := results[j]
			if res == "" {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			data, err := c.hooksMixin.current.decompress([]byte(res))
			if err != nil {
				return nil, fmt.Errorf("decompress value: %w", err)
			}
			var val R
			if err := c.hooksMixin.current.unmarshall(data, &val); err != nil {
				return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
			}
			key := chunks[i][j]
			resultMap[key] = val
		}
	}

	return resultMap, nil
}

// MGetValues fetches multiple keys from Redis and returns only the values. If
// the relationship between key -> value is required use MGet instead.
//
// MGetValues is useful when you only want to values and want to avoid the
// overhead of allocating a slice from a MultiResult.
func MGetValues[T any](ctx context.Context, c *Cache, keys ...string) ([]T, error) {

	// If batching is enabled and the number of keys exceeds the batch size use
	// multiple MGET commands in a pipeline.
	if c.mgetBatch > 0 && len(keys) > c.mgetBatch {
		return mGetValuesBatch[T](ctx, c, keys...)
	}

	results, err := c.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	values := make([]T, 0, len(keys))
	for i := 0; i < len(results); i++ {
		res := results[i]
		if res == nil || res == rueidis.Nil {
			// Some or all of the requested keys may not exist. Skip iterations
			// where the key wasn't found
			continue
		}
		str, ok := res.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
		}
		data, err := c.hooksMixin.current.decompress([]byte(str))
		if err != nil {
			return nil, fmt.Errorf("decompress value: %w", err)
		}
		var val T
		if err := c.hooksMixin.current.unmarshall(data, &val); err != nil {
			return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
		}
		values = append(values, val)
	}
	return values, nil
}

func mGetValuesBatch[T any](ctx context.Context, c *Cache, keys ...string) ([]T, error) {
	chunks := chunk(keys, c.mgetBatch)
	pipe := c.redis.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, len(chunks))

	for i := 0; i < len(chunks); i++ {
		cmds = append(cmds, pipe.MGet(ctx, chunks[i]...))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	values := make([]T, 0, len(keys))
	for i := 0; i < len(cmds); i++ {
		results, err := cmds[i].Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for j := 0; j < len(results); j++ {
			res := results[j]
			if res == nil || res == rueidis.Nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			data, err := c.hooksMixin.current.decompress([]byte(str))
			if err != nil {
				return nil, fmt.Errorf("decompress value: %w", err)
			}
			var val T
			if err := c.hooksMixin.current.unmarshall(data, &val); err != nil {
				return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
			}
			values = append(values, val)
		}
	}

	return values, nil
}

// UpsertCallback is a callback function that is invoked by Upsert. An UpsertCallback
// is passed if a key was found, the old value (or zero-value if the key wasn't found)
// and the new value. An UpsertCallback is responsible for determining what value should
// be set for a given key in the cache. The value returned from UpsertCallback is the
// value set.
type UpsertCallback[T any] func(found bool, oldValue T, newValue T) T

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
//		err := rcache.Upsert[Person](context.Background(), c, "BillyBob", p, cb)
//		if rcache.IsRetryable(err) {
//			continue
//		}
//		// do something useful ...
//		break
//	}
func Upsert[T any](ctx context.Context, c *Cache, key string, val T, cb UpsertCallback[T]) error {
	return UpsertTTL(ctx, c, key, val, cb, InfiniteTTL)
}

// UpsertTTL retrieves the existing value for a given key and invokes the UpsertCallback.
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
func UpsertTTL[T any](ctx context.Context, c *Cache, key string, val T, cb UpsertCallback[T], ttl time.Duration) error {

	// Builds Redis transaction where the value is fetched from Redis, unmarshalled,
	// and then passed to the UpsertCallback. The returned value from the UpsertCallback
	// is the value set for the given key.
	tx := func(tx *redis.Tx) error {
		found := false
		var oldVal T
		err := c.Get(ctx, key, &oldVal)
		if err != nil && !errors.Is(err, ErrKeyNotFound) {
			return fmt.Errorf("redis: %w", err)
		}
		if err == nil {
			found = true
		}
		newVal := cb(found, oldVal, val)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			binaryValue, err := c.hooksMixin.current.marshal(newVal)
			if err != nil {
				return fmt.Errorf("marshal value: %w", err)
			}
			data, err := c.hooksMixin.current.compress(binaryValue)
			if err != nil {
				return fmt.Errorf("compress value: %w", err)
			}
			return pipe.Set(ctx, key, data, ttl).Err()
		})
		return err
	}

	err := c.redis.Watch(ctx, tx, key)

	// Since Redis uses optimistic locking the key might have been modified during
	// the transaction. In such cases the operation will fail from Redis but the
	// operation can be retried, so we return a RetryableError.
	if errors.Is(err, redis.TxFailedErr) {
		return RetryableError{
			retryable: true,
			cause:     err,
		}
	}
	return err
}

// Scan retrieves all the keys and values from Redis matching the given pattern.
//
// Scan works similar to MGet, but allows a pattern to be specified rather than
// providing keys.
func Scan[T any](ctx context.Context, c *Cache, pattern string) (MultiResult[T], error) {
	keys, err := c.ScanKeys(ctx, pattern)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return MGet[T](ctx, c, keys...)
}
