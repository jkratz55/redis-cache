package cache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/redis/go-redis/v9"
)

// A TypedCache is a thin abstraction of the go-redis client that provides serialization, compression
// support, and batching MGET operations.
//
// TypedCache is similar to the Cache type, but provides a simpler API since it leverages generics.
// However, as a tradeoff, each cache instance can only support a single type. If you need to use multiple
// types, you will need to create multiple cache instances.
//
// The zero-value of TypedCache is not usable. Use NewTyped to create a new instance.
type TypedCache[T any] struct {
	redis        RedisClient
	marshaller   Marshaller
	unmarshaller Unmarshaller
	codec        Codec
	mgetBatch    int // zero-value indicates no batching
	hooksMixin
}

func NewTyped[T any](client RedisClient, opts ...Option) *TypedCache[T] {
	if client == nil {
		panic(fmt.Errorf("a valid redis client is required, illegal use of api"))
	}
	cache := &TypedCache[T]{
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

func (c *TypedCache[T]) Get(ctx context.Context, key string) (T, error) {
	var res T

	data, err := c.redis.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, ErrKeyNotFound
		}
		return res, fmt.Errorf("redis: %w", err)
	}
	data, err = c.hooksMixin.current.decompress(data)
	if err != nil {
		return res, fmt.Errorf("decompress value: %w", err)
	}
	if err := c.hooksMixin.current.unmarshall(data, &res); err != nil {
		return res, fmt.Errorf("unmarshall value to type %T: %w", res, err)
	}
	return res, nil
}

func (c *TypedCache[T]) GetAndExpire(ctx context.Context, key string, ttl time.Duration) (T, error) {
	var res T

	data, err := c.redis.GetEx(ctx, key, ttl).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, ErrKeyNotFound
		}
		return res, fmt.Errorf("redis: %w", err)
	}
	data, err = c.hooksMixin.current.decompress(data)
	if err != nil {
		return res, fmt.Errorf("decompress value: %w", err)
	}
	if err := c.hooksMixin.current.unmarshall(data, &res); err != nil {
		return res, fmt.Errorf("unmarshall value to type %T: %w", res, err)
	}
	return res, nil
}

func (c *TypedCache[T]) GetAndDelete(ctx context.Context, key string) (T, error) {
	var res T

	data, err := c.redis.GetDel(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, ErrKeyNotFound
		}
		return res, fmt.Errorf("redis: %w", err)
	}
	data, err = c.hooksMixin.current.decompress(data)
	if err != nil {
		return res, fmt.Errorf("decompress value: %w", err)
	}
	if err := c.hooksMixin.current.unmarshall(data, &res); err != nil {
		return res, fmt.Errorf("unmarshall value to type %T: %w", res, err)
	}
	return res, nil
}

func (c *TypedCache[T]) MGet(ctx context.Context, keys ...string) (MultiResult[T], error) {
	batch := c.mgetBatch
	if batch == 0 {
		batch = math.MaxInt
	}
	chunks := chunk(keys, batch)

	pipe := c.redis.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, len(chunks))
	for i := 0; i < len(chunks); i++ {
		cmds = append(cmds, pipe.MGet(ctx, chunks[i]...))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	resultMap := make(map[string]T, len(keys))
	for i := 0; i < len(cmds); i++ {
		results, err := cmds[i].Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for j, res := range results {
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
			var val T
			if err := c.hooksMixin.current.unmarshall(data, &val); err != nil {
				return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
			}
			key := chunks[i][j]
			resultMap[key] = val
		}
	}

	return resultMap, nil
}

func (c *TypedCache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	data, err := c.hooksMixin.current.marshal(val)
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

func (c *TypedCache[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	data, err := c.hooksMixin.current.marshal(val)
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

func (c *TypedCache[T]) SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	data, err := c.hooksMixin.current.marshal(val)
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

func (c *TypedCache[T]) MSet(ctx context.Context, values map[string]T) error {
	// The key and values needs to be processed prior to calling MSet by marshalling
	// and compressing the values.
	rawData := make(map[string]any)
	for k, v := range values {
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

func (c *TypedCache[T]) Delete(ctx context.Context, keys ...string) error {
	return c.redis.Del(ctx, keys...).Err()
}

func (c *TypedCache[T]) FlushDB(ctx context.Context) error {
	return c.redis.FlushDB(ctx).Err()
}

func (c *TypedCache[T]) FlushDBAsync(ctx context.Context) error {
	return c.redis.FlushDBAsync(ctx).Err()
}

func (c *TypedCache[T]) TTL(ctx context.Context, key string) (time.Duration, error) {
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

func (c *TypedCache[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	ok, err := c.redis.Expire(ctx, key, ttl).Result()
	if err != nil {
		return fmt.Errorf("redis: %w", err)
	}
	if !ok {
		return ErrKeyNotFound
	}
	return nil
}

func (c *TypedCache[T]) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	keys := make([]string, 0, batch)
	switch rdb := c.redis.(type) {
	case *redis.ClusterClient:
		err := rdb.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
			iter := client.Scan(ctx, 0, pattern, batch).Iterator()
			for iter.Next(ctx) {
				keys = append(keys, iter.Val())
			}
			if err := iter.Err(); err != nil {
				return fmt.Errorf("redis: %w", err)
			}

			return nil
		})
		return keys, err
	default:
		iter := rdb.Scan(ctx, 0, pattern, batch).Iterator()
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if err := iter.Err(); err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		return keys, nil
	}
}

func (c *TypedCache[T]) Client() redis.UniversalClient {
	return c.redis.(redis.UniversalClient)
}

func (c *TypedCache[T]) setMarshaller(marshaller Marshaller) {
	c.marshaller = marshaller
}

func (c *TypedCache[T]) setUnmarshaller(unmarshaller Unmarshaller) {
	c.unmarshaller = unmarshaller
}

func (c *TypedCache[T]) setCodec(codec Codec) {
	c.codec = codec
}

func (c *TypedCache[T]) setMGetBatch(i int) {
	c.mgetBatch = i
}
