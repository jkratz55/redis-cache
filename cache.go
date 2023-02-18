package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
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
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
	Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
	Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
	FlushDB(ctx context.Context) *redis.StatusCmd
	FlushDBAsync(ctx context.Context) *redis.StatusCmd
}

// Marshaller is a function type that marshals the value of a cache entry for
// storage.
type Marshaller func(v any) ([]byte, error)

// Unmarshaller is a function type that unmarshalls the value retrieved from the
// cache into the target type.
type Unmarshaller func(b []byte, v any) error

// Option allows for the Cache behavior/configuration to be customized.
type Option func(c *Cache)

// Serialization allows for the marshalling and unmarshalling behavior to be
// customized for the Cache.
//
// A valid Marshaller and Unmarshaller must be provided. Providing nil for either
// will immediately panic.
func Serialization(mar Marshaller, unmar Unmarshaller) Option {
	if mar == nil || unmar == nil {
		panic(fmt.Errorf("nil Marshaller and/or Unmarshaller not permitted, illegal use of api"))
	}
	return func(c *Cache) {
		c.marshaller = mar
		c.unmarshaller = unmar
	}
}

// Cache is a simple type that provides basic caching functionality: store, retrieve,
// and delete. It is backed by Redis and supports storing entries with a TTL.
//
// The zero-value is not usable, and this type should be instantiated using the
// NewCache function.
type Cache struct {
	redis        RedisClient
	marshaller   Marshaller
	unmarshaller Unmarshaller
}

// NewCache creates and initializes a new Cache instance.
//
// By default, msgpack is used for marshalling and unmarshalling the entry values.
// The behavior of Cache can be configured by passing Options.
func NewCache(redis RedisClient, opts ...Option) *Cache {
	if redis == nil {
		panic(fmt.Errorf("a valid redis client is required, illegal use of api"))
	}
	cache := &Cache{
		redis:        redis,
		marshaller:   DefaultMarshaller(),
		unmarshaller: DefaultUnmarshaller(),
	}
	for _, opt := range opts {
		opt(cache)
	}
	return cache
}

// Get retrieves an entry from the Cache for the given key, and if found will
// unmarshall the value into the provided type.
//
// If the key does not exist ErrKeyNotFound will be returned as the error value.
// A non-nil error value will be returned if the operation on the backing Redis
// fails, or if the value cannot be unmarshalled into the target type.
func (c *Cache) Get(ctx context.Context, key string, v any) error {
	data, err := c.redis.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrKeyNotFound
		}
		return fmt.Errorf("error fetching key %s from Redis: %w", key, err)
	}
	if err := c.unmarshaller(data, v); err != nil {
		return fmt.Errorf("error unmarshalling value for key %s: %w", key, err)
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
		return nil, err
	}
	return keys, nil
}

// Set adds an entry into the cache, or overwrites an entry if the key already
// existed. The entry is set without an expiration.
func (c *Cache) Set(ctx context.Context, key string, v any) error {
	return c.SetTTL(ctx, key, v, 0)
}

// SetTTL adds an entry into the cache, or overwrites an entry if the key
// already existed. The entry is set with the provided TTL and automatically
// removed from the cache once the TTL is expired.
func (c *Cache) SetTTL(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := c.marshaller(v)
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	return c.redis.Set(ctx, key, data, ttl).Err()
}

func (c *Cache) SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	data, err := c.marshaller(v)
	if err != nil {
		return false, fmt.Errorf("error marshalling value: %w", err)
	}
	return c.redis.SetNX(ctx, key, data, ttl).Result()
}

func (c *Cache) SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	data, err := c.marshaller(v)
	if err != nil {
		return false, fmt.Errorf("error marshalling value: %w", err)
	}
	return c.redis.SetXX(ctx, key, data, ttl).Result()
}

// Delete removes entries from the cache for a given set of keys.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.redis.Del(ctx, keys...).Err()
}

// Flush flushes the cache deleting all keys/entries.
func (c *Cache) Flush(ctx context.Context) {
	c.redis.FlushDB(ctx)
}

// FlushAsync flushes the cache deleting all keys/entries asynchronously. Only keys
// that were present when FLUSH ASYNC command was received by Redis will be deleted.
// Any keys created during asynchronous flush will be unaffected.
func (c *Cache) FlushAsync(ctx context.Context) {
	c.redis.FlushDBAsync(ctx)
}

// DefaultMarshaller returns a Marshaller using msgpack to marshall
// values.
func DefaultMarshaller() Marshaller {
	return func(v any) ([]byte, error) {
		return msgpack.Marshal(v)
	}
}

// DefaultUnmarshaller returns a Unmarshaller using msgpack to unmarshall
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
	for key, _ := range mr {
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
		return nil, fmt.Errorf("error retriving entries from Redis: %w", err)
	}
	resultMap := make(map[string]R, 0)
	for i, res := range results {
		if res == nil || res == redis.Nil {
			// Some or all of the requested keys may not exist. Skip iterations
			// where the key wasn't found
			continue
		}
		str, ok := res.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value from Redis: %v", res)
		}
		var val R
		if err := c.unmarshaller([]byte(str), &val); err != nil {
			return nil, fmt.Errorf("error unmarshalling result to type %T: %w", val, err)
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
	return UpsertTTL(ctx, c, key, val, cb, 0)
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
			return fmt.Errorf("error fetching key %s: %w", key, err)
		}
		if err == nil {
			found = true
		}
		newVal := cb(found, oldVal, val)
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			binaryValue, err := c.marshaller(newVal)
			if err != nil {
				return fmt.Errorf("failed to marshal value: %w", err)
			}
			return pipe.Set(ctx, key, binaryValue, ttl).Err()
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
