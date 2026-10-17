package cache

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"
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
	data, err := readValue(ctx, c.formats, c.redis.Get(ctx, key))
	if err != nil {
		var zero T
		return zero, err
	}
	return c.decompressAndUnmarshal(ctx, data)
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
	var zero T

	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return zero, err
	}
	data, err := readValue(ctx, c.formats, c.redis.GetEx(ctx, key, ttl))
	if err != nil {
		return zero, err
	}
	return c.decompressAndUnmarshal(ctx, data)
}

// GetAndDelete retrieves the value for the given key from the cache and deletes the key from the cache.
//
// If the key does not exist, ErrKeyNotFound will be returned. If the operation/command to Redis failed,
// the value could not be unmarshalled, or the value was compressed and could not be decompressed, the
// error will be returned.
func (c *TypedCache[T]) GetAndDelete(ctx context.Context, key string) (T, error) {
	data, err := readValue(ctx, c.formats, c.redis.GetDel(ctx, key))
	if err != nil {
		var zero T
		return zero, err
	}
	return c.decompressAndUnmarshal(ctx, data)
}

// MGet retrieves the values of multiple keys in a single operation.
//
// Like the reply to the Redis MGET command, the MultiResult has an entry for every key in the order of
// keys. Use Values for the values that were found, At to read them by position, All or Map to read them
// by key, and Missing for the keys that don't exist, for example to load them from the source of truth.
//
// If the values of some keys can't be decoded, for example because they are corrupt or were written in
// a format that isn't registered, MGet returns the values of the other keys along with a *MultiKeyError
// holding the error of every key that couldn't be decoded. Those keys are neither found nor missing. If
// the Redis command fails, MGet returns an empty MultiResult and the error. If no keys are provided, MGet
// returns an empty MultiResult without sending a command to Redis.
//
// When T is a pointer type, a stored nil decodes to a non-nil pointer to the zero value of the
// element type.
//
// MGet uses the MGET command. On Redis Cluster, a single MGET command can only read keys of the same
// hash slot, otherwise Redis returns a CROSSSLOT error. Use hash tags, such as "{user:1}:profile" and
// "{user:1}:friends", to store related keys in the same slot. BatchMultiGets splits keys by count, not
// by slot, so it doesn't lift this restriction.
func (c *TypedCache[T]) MGet(ctx context.Context, keys ...string) (MultiResult[T], error) {
	return multiGet(ctx, c.redis, c.formats, c.mgetBatch, keys, func(data []byte) (T, error) {
		return c.decompressAndUnmarshal(ctx, data)
	})
}

// Set adds an entry to Redis with the given key and value. The entry is set with the provided TTL.
// If the TTL is 0 or InfiniteTTL the entry will not expire, and KeepTTL keeps the TTL of an existing
// entry. Any other negative TTL returns ErrInvalidTTL.
//
// Set will overwrite an existing value for the key if it already exists.
func (c *TypedCache[T]) Set(ctx context.Context, key string, val T, ttl time.Duration) error {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return err
	}
	data, err := c.formats.encode(ctx, val)
	if err != nil {
		return err
	}
	err = c.redis.Set(ctx, key, data, ttl).Err()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return err
}

// SetIfAbsent adds an entry to Redis with the given key and value if the key does not already exist.
// If the key already exists, SetIfAbsent will not update Redis and will return false. Otherwise,
// SetIfAbsent will add the entry to Redis and return true.
//
// If the TTL is 0 or InfiniteTTL the entry will not expire. Negative TTLs other than InfiniteTTL and
// KeepTTL return ErrInvalidTTL.
func (c *TypedCache[T]) SetIfAbsent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.formats.encode(ctx, val)
	if err != nil {
		return false, err
	}
	ok, err := c.redis.SetNX(ctx, key, data, ttl).Result()
	if err != nil {
		err = fmt.Errorf("redis: %w", err)
	}
	return ok, err
}

// SetIfPresent updates an existing entry in Redis with the given key and value if the key already
// exists. If the key does not exist, SetIfPresent will not update Redis and will return false.
// Otherwise, SetIfPresent will update the entry in Redis and return true.
//
// If the TTL is 0 or InfiniteTTL the entry will not expire, and KeepTTL keeps the TTL of the existing
// entry. Any other negative TTL returns ErrInvalidTTL.
func (c *TypedCache[T]) SetIfPresent(ctx context.Context, key string, val T, ttl time.Duration) (bool, error) {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.formats.encode(ctx, val)
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
		val, err := c.formats.encode(ctx, v)
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

// Delete removes entries from the cache for a given set of keys.
//
// Delete uses a single DEL command, so on Redis Cluster all keys must be in the same hash slot,
// otherwise Redis returns a CROSSSLOT error.
func (c *TypedCache[T]) Delete(ctx context.Context, keys ...string) error {
	return wrapRedisErr(c.redis.Del(ctx, keys...).Err())
}

// FlushDB flushes the cache deleting all keys/entries with the FLUSHDB command.
//
// FLUSHDB deletes every key of the Redis database, including keys that weren't written by this
// TypedCache. On Redis Cluster, a *redis.ClusterClient sends FLUSHDB to a single master, so FlushDB
// only deletes the keys stored on that node. Use ForEachMaster on the *redis.ClusterClient to flush
// every node.
func (c *TypedCache[T]) FlushDB(ctx context.Context) error {
	return wrapRedisErr(c.redis.FlushDB(ctx).Err())
}

// FlushDBAsync flushes the cache deleting all keys/entries asynchronously. Only keys that were
// present when FLUSHDB ASYNC command was received by Redis will be deleted. Any keys created during
// an asynchronous flush will be unaffected.
//
// Like FlushDB, FlushDBAsync deletes every key of the Redis database and only flushes a single node
// of a Redis Cluster.
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
// If the underlying client is a *redis.ClusterClient, including one wrapped by a *FailoverClient,
// Scan will iterate over all masters in the cluster.
func (c *TypedCache[T]) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	return scanKeys(ctx, c.redis, pattern, batch)
}

// Client returns the underlying Redis client.
//
// Note: When using the client directly, you are responsible for handling serialization and compression
// yourself. TypedCache doesn't care about the underlying implementation and doesn't know what kind of
// client was provided to NewTyped, such as a *redis.Client, a *redis.ClusterClient or a
// *FailoverClient. For that reason it returns a RedisClient. If you need functionality beyond what the
// RedisClient interface offers, you need to handle type casting the returned RedisClient yourself.
func (c *TypedCache[T]) Client() RedisClient {
	return c.redis
}

func (c *TypedCache[T]) setMGetBatch(i int) {
	c.mgetBatch = i
}

func (c *TypedCache[T]) decompressAndUnmarshal(ctx context.Context, data []byte) (T, error) {
	var res T

	if c.isPtr {
		t := reflect.TypeFor[T]()
		ptr := reflect.New(t.Elem()).Interface()
		if err := c.formats.decode(ctx, data, ptr); err != nil {
			return res, err
		}
		res = ptr.(T)
		return res, nil
	}

	if err := c.formats.decode(ctx, data, &res); err != nil {
		return res, err
	}
	return res, nil
}
