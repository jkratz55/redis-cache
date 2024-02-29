package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	// InfiniteTTL indicates a key will never expire.
	//
	// Depending on Redis configuration keys may still be evicted if Redis is
	// under memory pressure in accordance to the eviction policy configured.
	InfiniteTTL time.Duration = -3

	// KeepTTL indicates to keep the existing TTL on the key on SET commands.
	KeepTTL = redis.KeepTTL
)

var (
	// ErrKeyNotFound is an error value that signals the key requested does not
	// exist in the cache.
	ErrKeyNotFound = errors.New("key not found")
)

// RedisClient is an interface type that defines the Redis functionality this
// package requires to use Redis as a cache.
type RedisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	GetEx(ctx context.Context, key string, expiration time.Duration) *redis.StringCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	SetXX(ctx context.Context, key string, value any, expiration time.Duration) *redis.BoolCmd
	MSet(ctx context.Context, values ...any) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	FlushDB(ctx context.Context) *redis.StatusCmd
	FlushDBAsync(ctx context.Context) *redis.StatusCmd
	Ping(ctx context.Context) *redis.StatusCmd
	TTL(ctx context.Context, key string) *redis.DurationCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Pipeline() redis.Pipeliner
}

// Marshaller is a function type that marshals the value of a cache entry for
// storage.
type Marshaller func(v any) ([]byte, error)

// Unmarshaller is a function type that unmarshalls the value retrieved from the
// cache into the target type.
type Unmarshaller func(b []byte, v any) error

// Cache is a simple type that provides basic caching functionality: store, retrieve,
// and delete. It is backed by Redis and supports storing entries with a TTL.
//
// The zero-value is not usable, and this type should be instantiated using the
// New function.
type Cache struct {
	redis        RedisClient
	marshaller   Marshaller
	unmarshaller Unmarshaller
	codec        Codec
	hooksMixin
}

// New creates and initializes a new Cache instance.
//
// By default, msgpack is used for marshalling and unmarshalling the entry values.
// The behavior of Cache can be configured by passing Options.
func New(client RedisClient, opts ...Option) *Cache {
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

// NewCache creates and initializes a new Cache instance.
//
// By default, msgpack is used for marshalling and unmarshalling the entry values.
// The behavior of Cache can be configured by passing Options.
//
// DEPRECATED: NewCache has been deprecated in favor of New and subject to being
// removed in future versions.
func NewCache(client RedisClient, opts ...Option) *Cache {
	return New(client, opts...)
}

// Get retrieves an entry from the Cache for the given key, and if found will
// unmarshall the value into v.
//
// If the key does not exist ErrKeyNotFound will be returned as the error value.
// A non-nil error value will be returned if the operation on the backing Redis
// fails, or if the value cannot be unmarshalled into the target type.
func (c *Cache) Get(ctx context.Context, key string, v any) error {
	data, err := c.redis.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
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

	val, err := c.redis.GetEx(ctx, key, ttl).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
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
	keys := make([]string, 0)
	iter := c.redis.Scan(ctx, 0, "*", 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return keys, nil
}

// ScanKeys allows for scanning keys in Redis using a pattern.
func (c *Cache) ScanKeys(ctx context.Context, pattern string) ([]string, error) {
	keys := make([]string, 0)
	iter := c.redis.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return keys, nil
}

// Set adds an entry into the cache, or overwrites an entry if the key already
// existed. The entry is set without an expiration.
func (c *Cache) Set(ctx context.Context, key string, v any) error {
	return c.SetWithTTL(ctx, key, v, InfiniteTTL)
}

// SetWithTTL adds an entry into the cache, or overwrites an entry if the key
// already existed. The entry is set with the provided TTL and automatically
// removed from the cache once the TTL is expired.
func (c *Cache) SetWithTTL(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := c.hooksMixin.current.marshal(v)
	if err != nil {
		return fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.hooksMixin.current.compress(data)
	if err != nil {
		return fmt.Errorf("compress value: %w", err)
	}
	err = c.redis.Set(ctx, key, data, ttl).Err()
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
	ok, err := c.redis.SetNX(ctx, key, data, ttl).Result()
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
	ok, err := c.redis.SetXX(ctx, key, data, ttl).Result()
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
	rawData := make(map[string]any)
	for k, v := range keyvalues {
		val, err := c.hooksMixin.current.marshal(v)
		if err != nil {
			return fmt.Errorf("marshal value: %w", err)
		}
		val, err = c.hooksMixin.current.compress(val)
		if err != nil {
			return fmt.Errorf("compress value: %w", err)
		}
		rawData[k] = val
	}

	if err := c.redis.MSet(ctx, rawData).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

// MSetWithTTL adds or overwrites multiple entries to the cache with a TTL value.
//
// MSetWithTTL performs multiple SET operations using a Pipeline. Unlike MSet,
// MSetWithTTL is not atomic. It is possible for some entries to succeed while
// others fail. When the pipeline itself executes successfully but commands fail
// MSetWithTTL returns CommandErrors which can be inspected to understand which
// keys failed in the event they need to be retried or logged. If the pipeline
// execution fails, or marshal and compression fails, a standard error value is
// returned.
//
// The command errors can be inspected following this example:
//
//	err := cache.MSetWithTTL(context.Background(), data, time.Minute*30)
//	if err != nil {
//		var cmdErrs CommandErrors
//		if errors.As(err, &cmdErrs) {
//			for _, e := range cmdErrs {
//				fmt.Println(e)
//			}
//			// todo: do something actually useful here
//		} else {
//			fmt.Println(err) // just a normal error value
//		}
//	}
func (c *Cache) MSetWithTTL(ctx context.Context, keyvalues map[string]any, ttl time.Duration) error {
	pipe := c.redis.Pipeline()
	for k, v := range keyvalues {
		val, err := c.hooksMixin.current.marshal(v)
		if err != nil {
			return fmt.Errorf("marshal value: %w", err)
		}
		val, err = c.hooksMixin.current.compress(val)
		if err != nil {
			return fmt.Errorf("compress value: %w", err)
		}
		pipe.Set(ctx, k, val, ttl)
	}

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		cmdErrors := make(CommandErrors, 0)
		for _, cmd := range cmds {
			if cmd.Err() != nil {
				// This shouldn't panic but let's not take chances ...
				var key string
				if k, ok := cmd.Args()[1].(string); ok {
					key = k
				}

				cmdErrors = append(cmdErrors, CommandError{
					Command: cmd.Name(),
					Key:     key,
					Err:     cmd.Err(),
				})
			}
		}

		// If len redis errors is zero than the pipeline itself failed and none
		// of the commands executed on Redis. In this case the original error
		// from the Pipeliner is returned. If there were errors on at least one
		// command than the command errors are return.
		if len(cmdErrors) == 0 {
			return err
		}
		return cmdErrors
	}

	return nil
}

// Delete removes entries from the cache for a given set of keys.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.redis.Del(ctx, keys...).Err()
}

// Flush flushes the cache deleting all keys/entries.
func (c *Cache) Flush(ctx context.Context) error {
	return c.redis.FlushDB(ctx).Err()
}

// FlushAsync flushes the cache deleting all keys/entries asynchronously. Only keys
// that were present when FLUSH ASYNC command was received by Redis will be deleted.
// Any keys created during asynchronous flush will be unaffected.
func (c *Cache) FlushAsync(ctx context.Context) error {
	return c.redis.FlushDBAsync(ctx).Err()
}

// Healthy pings Redis to ensure it is reachable and responding. Healthy returns
// true if Redis successfully responds to the ping, otherwise false.
func (c *Cache) Healthy(ctx context.Context) bool {
	return c.redis.Ping(ctx).Err() == nil
}

// TTL returns the time to live for a particular key.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value.
// If the key doesn't have a TTL InfiniteTTL will be returned.
func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	dur, err := c.redis.TTL(ctx, key).Result()
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
	return dur, nil
}

// Expire sets a TTL on the given key.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value.
// If the key already has a TTL it will be overridden with ttl value provided.
//
// Calling Expire with a non-positive ttl will result in the key being deleted.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ok, err := c.redis.Expire(ctx, key, ttl).Result()
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
	ttl, err := c.redis.TTL(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	// Redis returns -2 for TTL command if the key doesn't exist
	if ttl == -2 {
		return ErrKeyNotFound
	}

	return c.Expire(ctx, key, ttl+dur)
}

// DefaultMarshaller returns a Marshaller using msgpack to marshall
// values.
func DefaultMarshaller() Marshaller {
	return func(v any) ([]byte, error) {
		return msgpack.Marshal(v)
	}
}

// DefaultUnmarshaller returns an Unmarshaller using msgpack to unmarshall
// values.
func DefaultUnmarshaller() Unmarshaller {
	return func(b []byte, v any) error {
		return msgpack.Unmarshal(b, v)
	}
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
func MGet[R any](ctx context.Context, c *Cache, keys ...string) (MultiResult[R], error) {
	results, err := c.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	resultMap := make(map[string]R)
	for i, res := range results {
		if res == nil || res == redis.Nil {
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
		var val R
		if err := c.hooksMixin.current.unmarshall(data, &val); err != nil {
			return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
		}
		resultMap[keys[i]] = val
	}
	return resultMap, nil
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
