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
	ErrKeyNotFound = errors.New("key not found")
)

type redisClient interface {
	Get(ctx context.Context, key string) *redis.StringCmd
	Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd
	Del(ctx context.Context, keys ...string) *redis.IntCmd
}

type Marshaller func(v any) ([]byte, error)

type Unmarshaller func(b []byte, v any) error

type Cache struct {
	Redis        redisClient
	Marshaller   Marshaller
	Unmarshaller Unmarshaller
}

func (c *Cache) Get(ctx context.Context, key string, v any) error {
	data, err := c.Redis.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrKeyNotFound
		}
		return fmt.Errorf("error fetching key %s from Redis: %w", key, err)
	}
	if err := c.Unmarshaller(data, v); err != nil {
		return fmt.Errorf("error unmarshalling value for key %s: %w", key, err)
	}
	return nil
}

func (c *Cache) Set(ctx context.Context, key string, v any) error {
	return c.SetWithTTL(ctx, key, v, 0)
}

func (c *Cache) SetWithTTL(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := c.Marshaller(v)
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	return c.Redis.Set(ctx, key, data, ttl).Err()
}

func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.Redis.Del(ctx, keys...).Err()
}

func DefaultMarshaller() Marshaller {
	return func(v any) ([]byte, error) {
		return msgpack.Marshal(v)
	}
}

func DefaultUnmarshaller() Unmarshaller {
	return func(b []byte, v any) error {
		return msgpack.Unmarshal(b, v)
	}
}
