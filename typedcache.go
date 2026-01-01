package cache

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
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
	redis      RedisClient
	serializer Serializer
	codec      CompressionCodec
	mgetBatch  int // zero-value indicates no batching
	isPtr      bool
}

// NewTyped creates and initializes a new TypedCache instance.
func NewTyped[T any](client RedisClient, opts ...Option) *TypedCache[T] {
	if client == nil {
		panic(errors.New("client cannot be nil"))
	}

	cache := &TypedCache[T]{
		redis:      client,
		serializer: MessagePackSerializer{},
		codec:      nopCodec{},
	}

	for _, opt := range opts {
		opt(cache)
	}

	t := reflect.TypeOf((*T)(nil)).Elem()
	cache.isPtr = t.Kind() == reflect.Ptr

	return cache
}

// Get retrieves the value for the given key from the cache.
//
// If the key does not exist, ErrKeyNotFound will be returned. If the operation/command to Redis failed,
// the value could not be unmarshalled, or the value was compressed and could not be decompressed, the
// error will be returned.
func (c *TypedCache[T]) Get(ctx context.Context, key string) (T, error) {
	var res T

	data, err := c.redis.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, ErrKeyNotFound
		}
		return res, fmt.Errorf("redis: %w", err)
	}
	return c.decompressAndUnmarshal(data)
}

// GetAndExpire retrieves the value for the given key from the cache and sets the expiration time for the key.
//
// If the key does not exist, ErrKeyNotFound will be returned. If the operation/command to Redis failed,
// the value could not be unmarshalled, or the value was compressed and could not be decompressed, the
// error will be returned.
//
// A ttl value of 0 will remove the TTL on the key if there is one.
func (c *TypedCache[T]) GetAndExpire(ctx context.Context, key string, ttl time.Duration) (T, error) {
	var res T

	data, err := c.redis.GetEx(ctx, key, ttl).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, ErrKeyNotFound
		}
		return res, fmt.Errorf("redis: %w", err)
	}
	return c.decompressAndUnmarshal(data)
}

// GetAndDelete retrieves the value for the given key from the cache and deletes the key from the cache.
//
// If the key does not exist, ErrKeyNotFound will be returned. If the operation/command to Redis failed,
// the value could not be unmarshalled, or the value was compressed and could not be decompressed, the
// error will be returned.
func (c *TypedCache[T]) GetAndDelete(ctx context.Context, key string) (T, error) {
	var res T

	data, err := c.redis.GetDel(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return res, ErrKeyNotFound
		}
		return res, fmt.Errorf("redis: %w", err)
	}
	return c.decompressAndUnmarshal(data)
}

// MGet retrieves multiple values from Redis for the given keys.
func (c *TypedCache[T]) MGet(ctx context.Context, keys ...string) ([]T, error) {
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

	resultValues := make([]T, 0, len(keys))
	for i := 0; i < len(cmds); i++ {
		results, err := cmds[i].Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for _, res := range results {
			if res == nil || res == redis.Nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			val, err := c.decompressAndUnmarshal([]byte(str))
			if err != nil {
				return nil, err
			}
			resultValues = append(resultValues, val)
		}
	}

	return resultValues, nil
}

func (c *TypedCache[T]) MGetMap(ctx context.Context, keys ...string) (MultiResult[T], error) {
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
			val, err := c.decompressAndUnmarshal([]byte(str))
			if err != nil {
				return nil, err
			}
			key := chunks[i][j]
			resultMap[key] = val
		}
	}

	return resultMap, nil
}

func (c *TypedCache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	data, err := c.serializer.Marshal(val)
	if err != nil {
		return fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.codec.Compress(data)
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
	data, err := c.serializer.Marshal(val)
	if err != nil {
		return false, fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.codec.Compress(data)
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
	data, err := c.serializer.Marshal(val)
	if err != nil {
		return false, fmt.Errorf("marshall value: %w", err)
	}
	data, err = c.codec.Compress(data)
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
		val, err := c.serializer.Marshal(v)
		if err != nil {
			return fmt.Errorf("marshal value: %w", err)
		}
		val, err = c.codec.Compress(val)
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

func (c *TypedCache[T]) setSerializer(serializer Serializer) {
	c.serializer = serializer
}

func (c *TypedCache[T]) setCodec(codec CompressionCodec) {
	c.codec = codec
}

func (c *TypedCache[T]) setMGetBatch(i int) {
	c.mgetBatch = i
}

func (c *TypedCache[T]) decompressAndUnmarshal(data []byte) (T, error) {
	var res T

	data, err := c.codec.Decompress(data)
	if err != nil {
		return res, fmt.Errorf("decompress value: %w", err)
	}
	if c.isPtr {
		t := reflect.TypeOf((*T)(nil)).Elem()
		ptr := reflect.New(t.Elem()).Interface()
		if err := c.serializer.Unmarshal(data, ptr); err != nil {
			return res, fmt.Errorf("unmarshal value: %w", err)
		}
		res = ptr.(T)
		return res, nil
	}

	if err := c.serializer.Unmarshal(data, &res); err != nil {
		return res, fmt.Errorf("unmarshal value: %w", err)
	}
	return res, nil
}
