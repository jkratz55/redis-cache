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
