package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v9"
)

// Cacheable is a type that can Marshall itself into binary representation
// (aka []byte) and Unmarshall itself from []byte.
//
// Implementations of Cacheable should use pointer receiver, otherwise Unmarshall
// will not work as expected as the contents of type have to be modified and
// reflect outside the function.
type Cacheable interface {
	Marshall() ([]byte, error)
	Unmarshall(data []byte) error
}

type TypesafeCache[T Cacheable] struct {
	redis RedisClient
}

func (tc *TypesafeCache[T]) Get(ctx context.Context, key string, val T) error {
	data, err := tc.redis.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound
		}
		return fmt.Errorf("error fetching key %s from Redis: %w", key, err)
	}
	err = val.Unmarshall(data)
	if err != nil {
		return fmt.Errorf("error unmarshalling value: %w", err)
	}
	return err
}

func (tc *TypesafeCache[T]) MGet(ctx context.Context, keys []string, cb func(key string, data []byte)) error {
	results, err := tc.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return fmt.Errorf("error fetching multiple keys from Redis: %w", err)
	}
	for i, val := range results {
		str, ok := val.(string)
		if !ok {
			return fmt.Errorf("invalid Redis response")
		}
		cb(keys[i], []byte(str))
	}
	return nil
}

func (tc *TypesafeCache[T]) Set(ctx context.Context, key string, val T) error {
	data, err := val.Marshall()
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	return tc.redis.Set(ctx, key, data, 0).Err()
}

func (tc *TypesafeCache[T]) SetTTL(ctx context.Context, key string, val T, ttl time.Duration) error {
	data, err := val.Marshall()
	if err != nil {
		return fmt.Errorf("error marshalling value: %w", err)
	}
	return tc.redis.Set(ctx, key, data, ttl).Err()
}

func (tc *TypesafeCache[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	data, err := val.Marshall()
	if err != nil {
		return false, fmt.Errorf("error marshalling value: %w", err)
	}
	return tc.redis.SetNX(ctx, key, data, ttl).Result()
}

func (tc *TypesafeCache[T]) SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	data, err := val.Marshall()
	if err != nil {
		return false, fmt.Errorf("error marshalling value: %w", err)
	}
	return tc.redis.SetXX(ctx, key, data, ttl).Result()
}

func (tc *TypesafeCache[T]) Delete(ctx context.Context, keys ...string) error {
	return tc.redis.Del(ctx, keys...).Err()
}
