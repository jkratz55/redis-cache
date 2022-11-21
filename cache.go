package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

var (
	// ErrKeyNotFound is an error value that signals the key requested does not
	// exist in the cache.
	ErrKeyNotFound = errors.New("key not found")
)

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	MGet(ctx context.Context, keys ...string) *redis.SliceCmd
	Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
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
	redis        redisClient
	marshaller   Marshaller
	unmarshaller Unmarshaller
}

// NewCache creates and initializes a new Cache instance.
//
// By default, msgpack is used for marshalling and unmarshalling the entry values.
// The behavior of Cache can be configured by passing Options.
func NewCache(redis redisClient, opts ...Option) *Cache {
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

// MGet retrieves multiple entries from the cache for the given keys.
//
// Because Go doesn't support type parameters on methods there is no way to handle
// unmarshalling into the destination type. For this reason each value returned from
// Redis will be passed to a callback as []byte along with they key and Unmarshaller
// func. The caller can unmarshall the result from Redis and process the results as
// they wish.
//
// It is recommended to use the MGet function instead unless there is a specific need
// to handle building the results on your own.
func (c *Cache) MGet(ctx context.Context, keys []string, callback func(key string, val []byte, um Unmarshaller)) error {
	results, err := c.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return fmt.Errorf("error fetching multiple keys from Redis: %w", err)
	}
	for i, val := range results {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("invalid Redis response")
		}
		callback(keys[i], []byte(str), c.unmarshaller)
	}
	return nil
}

// Set adds an entry into the cache, or overwrites an entry if the key already
// existed. The entry is set without an expiration.
func (c *Cache) Set(ctx context.Context, key string, v any) error {
	return c.SetWithTTL(ctx, key, v, 0)
}

// SetWithTTL adds an entry into the cache, or overwrites an entry if the key
// already existed. The entry is set with the provided TTL and automatically
// removed from the cache once the TTL is expired.
func (c *Cache) SetWithTTL(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := c.marshaller(v)
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	return c.redis.Set(ctx, key, data, ttl).Err()
}

// Delete removes entries from the cache for a given set of keys.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.redis.Del(ctx, keys...).Err()
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
//
// The Cache type has a MGet method but because Go doesn't support type parameters
// on methods it relies on callbacks for the caller to unmarshall into the destination
// type. For that reason it is highly recommended to use this variant of MGet over the
// method on the Cache type.
func MGet[R any](ctx context.Context, c *Cache, keys ...string) (MultiResult[R], error) {
	results, err := c.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("error retriving entries from Redis: %w", err)
	}
	resultMap := make(map[string]R, 0)
	for i, res := range results {
		str, ok := res.(string)
		if !ok {
			return nil, fmt.Errorf("invalid Redis response")
		}
		var val R
		if err := c.unmarshaller([]byte(str), &val); err != nil {
			return nil, fmt.Errorf("error unmarshalling result to type %T: %w", val, err)
		}
		resultMap[keys[i]] = val
	}
	return resultMap, nil
}
