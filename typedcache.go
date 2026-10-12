package cache

import (
	"context"
	"errors"
	"fmt"
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
//
// When T is a pointer type, such as TypedCache[*Person], values are always decoded into a newly
// allocated value. A nil pointer that was stored decodes to a non-nil pointer to the zero value of
// the element type, not to nil.
type TypedCache[T any] struct {
	redis     RedisClient
	formats   *formats
	mgetBatch int // zero-value indicates no batching
	isPtr     bool

	// formatOptions collects the serialization and compression configuration
	// while options are applied. It is not used after construction.
	formatOptions
}

// NewTyped creates and initializes a new TypedCache instance.
//
// Every value is stored with a small header identifying the Serializer and
// CompressionCodec used to write it. This allows values written with a different
// configuration to be read as long as their Serializer and CompressionCodec are
// registered, see DecodeSerializers and DecodeCompression.
//
// NewTyped panics if the client is nil or if a Serializer or CompressionCodec
// doesn't have a valid format ID.
func NewTyped[T any](client RedisClient, opts ...Option) *TypedCache[T] {
	if client == nil {
		panic(errors.New("client cannot be nil"))
	}

	cache := &TypedCache[T]{
		redis:         client,
		formatOptions: defaultFormatOptions(),
	}

	for _, opt := range opts {
		opt(cache)
	}
	cache.formats = cache.formatOptions.build()
	cache.formatOptions = formatOptions{}

	t := reflect.TypeFor[T]()
	cache.isPtr = t.Kind() == reflect.Pointer

	return cache
}

// Get retrieves the value for the given key from the cache.
//
// If the key does not exist, ErrKeyNotFound will be returned. If the operation/command to Redis failed,
// the value could not be unmarshalled, or the value was compressed and could not be decompressed, the
// error will be returned.
//
// When T is a pointer type, a stored nil decodes to a non-nil pointer to the zero value of the
// element type.
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
// A ttl value of 0 or InfiniteTTL will remove the TTL on the key if there is one, and KeepTTL leaves
// it unchanged. Any other negative ttl returns ErrInvalidTTL.
func (c *TypedCache[T]) GetAndExpire(ctx context.Context, key string, ttl time.Duration) (T, error) {
	var res T

	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return res, err
	}
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

// MGet retrieves multiple values from Redis for the given keys. Keys that don't exist are skipped.
//
// When T is a pointer type, a stored nil decodes to a non-nil pointer to the zero value of the
// element type.
func (c *TypedCache[T]) MGet(ctx context.Context, keys ...string) ([]T, error) {
	numKeys := len(keys)
	if numKeys == 0 {
		return nil, nil
	}

	if c.mgetBatch <= 0 || numKeys <= c.mgetBatch {
		results, err := c.redis.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		resultValues := make([]T, 0, numKeys)
		for i, res := range results {
			if res == nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			val, err := c.decodeKey(keys[i], []byte(str))
			if err != nil {
				return nil, err
			}
			resultValues = append(resultValues, val)
		}
		return resultValues, nil
	}

	numBatches := (numKeys + c.mgetBatch - 1) / c.mgetBatch
	pipe := c.redis.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, numBatches)
	for i := 0; i < numKeys; i += c.mgetBatch {
		end := min(i+c.mgetBatch, numKeys)
		cmds = append(cmds, pipe.MGet(ctx, keys[i:end]...))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	resultValues := make([]T, 0, len(keys))
	for i, cmd := range cmds {
		results, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for j, res := range results {
			if res == nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			val, err := c.decodeKey(keys[i*c.mgetBatch+j], []byte(str))
			if err != nil {
				return nil, err
			}
			resultValues = append(resultValues, val)
		}
	}

	return resultValues, nil
}

func (c *TypedCache[T]) MGetMap(ctx context.Context, keys ...string) (MultiResult[T], error) {
	numKeys := len(keys)
	if numKeys == 0 {
		return nil, nil
	}

	if c.mgetBatch <= 0 || numKeys <= c.mgetBatch {
		results, err := c.redis.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		resultMap := make(map[string]T, numKeys)
		for i, res := range results {
			if res == nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			val, err := c.decodeKey(keys[i], []byte(str))
			if err != nil {
				return nil, err
			}
			resultMap[keys[i]] = val
		}
		return resultMap, nil
	}

	numBatches := (numKeys + c.mgetBatch - 1) / c.mgetBatch
	pipe := c.redis.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, numBatches)
	for i := 0; i < numKeys; i += c.mgetBatch {
		end := min(i+c.mgetBatch, numKeys)
		cmds = append(cmds, pipe.MGet(ctx, keys[i:end]...))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	resultMap := make(map[string]T, len(keys))
	for i, cmd := range cmds {
		results, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for j, res := range results {
			if res == nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			key := keys[i*c.mgetBatch+j]
			val, err := c.decodeKey(key, []byte(str))
			if err != nil {
				return nil, err
			}
			resultMap[key] = val
		}
	}

	return resultMap, nil
}

func (c *TypedCache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return err
	}
	data, err := c.formats.encode(val)
	if err != nil {
		return err
	}
	err = c.redis.Set(ctx, key, data, ttl).Err()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return err
}

func (c *TypedCache[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.formats.encode(val)
	if err != nil {
		return false, err
	}
	ok, err := c.redis.SetNX(ctx, key, data, ttl).Result()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return ok, err
}

func (c *TypedCache[T]) SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.formats.encode(val)
	if err != nil {
		return false, err
	}
	ok, err := c.redis.SetXX(ctx, key, data, ttl).Result()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return ok, err
}

// MSet sets multiple entries in Redis for the given key/value pairs, overriding any existing
// entries.
//
// MSet uses a single MSET command, which is only atomic for keys in the same hash slot. On Redis
// Cluster all keys must share a hash tag (e.g. "{user}:1" and "{user}:2"), otherwise Redis returns
// a CROSSSLOT error. MSet doesn't set a TTL, so the entries never expire and any existing TTL on
// the keys is removed. Redis rejects an MSET without keys, so calling MSet with an empty map
// returns an error.
func (c *TypedCache[T]) MSet(ctx context.Context, values map[string]T) error {
	// The key and values needs to be processed prior to calling MSet by marshalling
	// and compressing the values.
	rawData := make(map[string]any)
	for k, v := range values {
		val, err := c.formats.encode(v)
		if err != nil {
			return err
		}
		rawData[k] = val
	}

	if err := c.redis.MSet(ctx, rawData).Err(); err != nil {
		return fmt.Errorf("redis: %w", err)
	}

	return nil
}

func (c *TypedCache[T]) Delete(ctx context.Context, keys ...string) error {
	return wrapRedisErr(c.redis.Del(ctx, keys...).Err())
}

func (c *TypedCache[T]) FlushDB(ctx context.Context) error {
	return wrapRedisErr(c.redis.FlushDB(ctx).Err())
}

func (c *TypedCache[T]) FlushDBAsync(ctx context.Context) error {
	return wrapRedisErr(c.redis.FlushDBAsync(ctx).Err())
}

// TTL returns the remaining time to live for the given key with millisecond precision.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value. If the key exists
// but has no TTL, InfiniteTTL will be returned.
func (c *TypedCache[T]) TTL(ctx context.Context, key string) (time.Duration, error) {
	return keyTTL(ctx, c.redis, key)
}

// Expire sets a TTL on the given key with millisecond precision.
//
// A ttl of InfiniteTTL or 0 removes the TTL from the key so it never expires; the key is never
// deleted. Any other negative ttl returns ErrInvalidTTL. If the key doesn't exist ErrKeyNotFound
// will be returned for the error value.
func (c *TypedCache[T]) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return expireKey(ctx, c.redis, key, ttl)
}

// Scan performs a Redis SCAN command to retrieve all keys matching the given pattern.
//
// The batch parameter controls the number of keys returned in each SCAN command. Values less
// than or equal to zero default to 1000.
//
// If the underlying client is a redis.ClusterClient, Scan will iterate over all masters in the
// cluster.
func (c *TypedCache[T]) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	return scanKeys(ctx, c.redis, pattern, batch)
}

func (c *TypedCache[T]) Client() redis.UniversalClient {
	return c.redis.(redis.UniversalClient)
}

func (c *TypedCache[T]) setMGetBatch(i int) {
	c.mgetBatch = i
}

// decodeKey decodes the value of a key returned by a multi-key operation and
// adds the key and type to the error if decoding fails.
func (c *TypedCache[T]) decodeKey(key string, data []byte) (T, error) {
	val, err := c.decompressAndUnmarshal(data)
	if err != nil {
		return val, fmt.Errorf("decode key %q to type %s: %w", key, reflect.TypeFor[T](), err)
	}
	return val, nil
}

func (c *TypedCache[T]) decompressAndUnmarshal(data []byte) (T, error) {
	var res T

	if c.isPtr {
		t := reflect.TypeFor[T]()
		ptr := reflect.New(t.Elem()).Interface()
		if err := c.formats.decode(data, ptr); err != nil {
			return res, err
		}
		res = ptr.(T)
		return res, nil
	}

	if err := c.formats.decode(data, &res); err != nil {
		return res, err
	}
	return res, nil
}
