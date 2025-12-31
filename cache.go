package cache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// InfiniteTTL indicates a key will never expire.
	//
	// Depending on Redis configuration keys may still be evicted if Redis is
	// under memory pressure in accordance to the eviction policy configured.
	InfiniteTTL time.Duration = -3

	// KeepTTL indicates to keep the existing TTL on the key on RedisOperationSET commands.
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
	GetDel(ctx context.Context, key string) *redis.StringCmd
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

// Cache is a thin abstraction over the go-redis client that provides serialization, compression
// and batching of MGET operations.
//
// By default, msgpack is used for marshalling and unmarshalling the entry values. There is no
// compression by default, but it can be enabled by using an Option in New.
//
// The zero-value of Cache is not usable. Use New to create a new instance.
type Cache struct {
	redis      RedisClient
	serializer Serializer
	codec      CompressionCodec
	mgetBatch  int // zero-value indicates no batching
}

// New creates and initializes a new Cache instance.
//
// By default msgpack is used for marshalling and unmarshalling the entry values. There is no
// compression by default, but it can be enabled by using an Option.
func New(client RedisClient, opts ...Option) *Cache {
	if client == nil {
		panic(fmt.Errorf("client cannot be nil"))
	}
	cache := &Cache{
		redis:      client,
		serializer: MessagePackSerializer{},
		codec:      nopCodec{},
	}
	for _, opt := range opts {
		opt(cache)
	}

	return cache
}

// Get retrieves a value from Redis for the given key.
//
// If the key does not exist, ErrKeyNotFound will be returned as the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed, the value could not
// be unmarshalled, or the value was compressed and could not be decompressed.
//
// The destination type v should be a pointer to the desired type.
//
// Example:
//
//	var val string
//	err := cache.Get(ctx, "key", &val)
//	if err != nil {
//	  // handle error
//	}
func (c *Cache) Get(ctx context.Context, key string, v any) error {
	data, err := c.redis.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound
		}
		return fmt.Errorf("redis: %w", err)
	}
	data, err = c.codec.Decompress(data)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}
	if err := c.serializer.Unmarshal(data, v); err != nil {
		return fmt.Errorf("unmarshall value: %w", err)
	}
	return nil
}

// GetAndExpire retrieves a value from Redis for the given key and sets the TTL on the key.
//
// If the key does not exist, ErrKeyNotFound will be returned as the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed, the value could not
// be unmarshalled, or the value was compressed and could not be decompressed.
//
// The destination type v should be a pointer to the desired type.
//
// GetAndExpire is useful for operations such as fetching and extending a session stored in Redis.
//
// A ttl value of 0 will remove the TTL on the key if there is one.
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
	data, err := c.codec.Decompress(val)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}
	if err := c.serializer.Unmarshal(data, v); err != nil {
		return fmt.Errorf("unmarshall value: %w", err)
	}
	return nil
}

// GetAndDelete retrieves a value from Redis for the given key and deletes the key from Redis.
//
// If the key does not exist, ErrKeyNotFound will be returned as the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed, the value could not
// be unmarshalled, or the value was compressed and could not be decompressed.
//
// The destination type v should be a pointer to the desired type.
func (c *Cache) GetAndDelete(ctx context.Context, key string, v any) error {
	val, err := c.redis.GetDel(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound
		}
		return fmt.Errorf("redis: %w", err)
	}
	data, err := c.codec.Decompress(val)
	if err != nil {
		return fmt.Errorf("decompress value: %w", err)
	}
	if err := c.serializer.Unmarshal(data, v); err != nil {
		return fmt.Errorf("unmarshall value: %w", err)
	}
	return nil
}

// Keys retrieves all the keys in Redis/Cache
//
// Note: Keys under the hood is using the Redis SCAN command instead of KEYS, which is more efficient,
// but this is still an expensive operation.
func (c *Cache) Keys(ctx context.Context) ([]string, error) {
	return c.Scan(ctx, "*", 1000)
}

// Set adds an entry to Redis with the given key and value. The entry is set with the provided TTL.
// If the TTL is 0 the entry will not expire.
//
// Set will overwrite an existing value for the key if it already exists.
func (c *Cache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	data, err := c.serializer.Marshal(v)
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

// SetIfAbsent adds an entry to Redis with the given key and value if the key does not already exist.
// If the key already exists, SetIfAbsent will not update Redis and will return false. Otherwise,
// SetIfAbsent will add the entry to Redis and return true.
//
// If the TTL is 0 the entry will not expire.
func (c *Cache) SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	data, err := c.serializer.Marshal(v)
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

// SetIfPresent updates an existing entry in Redis with the given key and value if the key already
// exists. If the key does not exist, SetIfPresent will not update Redis and will return false.
// Otherwise, SetIfPresent will update the entry in Redis and return true.
//
// If the TTL is 0 the entry will not expire.
func (c *Cache) SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	data, err := c.serializer.Marshal(v)
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

// MSet sets multiple entries in Redis for the given key/value pairs.
//
// MSet is atomic and will override any existing entries with the provided values.
func (c *Cache) MSet(ctx context.Context, keyvalues map[string]any) error {
	// The key and values needs to be processed prior to calling MSet by marshalling
	// and compressing the values.
	rawData := make(map[string]any)
	for k, v := range keyvalues {
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

// Delete removes entries from the cache for a given set of keys.
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return c.redis.Del(ctx, keys...).Err()
}

// Flush flushes the cache deleting all keys/entries.
func (c *Cache) Flush(ctx context.Context) error {
	return c.redis.FlushDB(ctx).Err()
}

// FlushAsync flushes the cache deleting all keys/entries asynchronously. Only keys that were present
// when FLUSH ASYNC command was received by Redis will be deleted. Any keys created during an asynchronous
// flush will be unaffected.
func (c *Cache) FlushAsync(ctx context.Context) error {
	return c.redis.FlushDBAsync(ctx).Err()
}

// TTL returns the remaining time to live for the given key.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed. If the key exists but
// has no TTL, InfiniteTTL will be returned.
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
// If the key doesn't exist ErrKeyNotFound will be returned for the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed.
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

// Scan performs a Redis SCAN command to retrieve all keys matching the given pattern.
//
// The batch parameter controls the number of keys returned in each SCAN command. The larger the
// batch, the more efficient the operation is, but the more memory is used.
//
// If the underlying client is a redis.ClusterClient, Scan will iterate over all masters in the
// cluster.
func (c *Cache) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	if batch <= 0 {
		batch = 1000
	}

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

// Client returns the underlying Redis client.
//
// Note: When using the client directly, you are responsible for handling serialization and compression
// yourself. Keep in mind Cache doesn't care about the underlying implementation and doesn't know what
// kind of client was provided to New. For that reason it returns a RedisClient. If you need functionality
// beyond what the RedisClient interface offers, you need to handle type casting the returned RedisClient
// yourself.
func (c *Cache) Client() RedisClient {
	return c.redis
}

func (c *Cache) setSerializer(serializer Serializer) {
	c.serializer = serializer
}

func (c *Cache) setCodec(codec CompressionCodec) {
	c.codec = codec
}

func (c *Cache) setMGetBatch(i int) {
	c.mgetBatch = i
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
//
// Note: This operation can be expensive as it allocates a new slice and copies all the values from
// the result.
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

// MGetMap retrieves multiple keys in a single operation and returns the result as a map key -> value.
//
// If a key doesn't exist in Redis it will not be present in the result map.
//
// MGetMap is useful when you want to fetch multiple keys and their relationship between each other
// is important. For example, if you want to fetch a user's profile and their friends, you can use
// MGetMap to fetch both the profile and the friends in a single operation. If you just want the values
// and don't care about the relationship between the keys, use MGet instead.
func MGetMap[R any](ctx context.Context, c *Cache, keys ...string) (MultiResult[R], error) {

	// If batching is enabled and the number of keys exceeds the batch size use
	// multiple MGET commands in a pipeline.
	if c.mgetBatch > 0 && len(keys) > c.mgetBatch {
		return mGetMapBatch[R](ctx, c, keys...)
	}

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
		data, err := c.codec.Decompress([]byte(str))
		if err != nil {
			return nil, fmt.Errorf("decompress value: %w", err)
		}
		var val R
		if err := c.serializer.Unmarshal(data, &val); err != nil {
			return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
		}
		resultMap[keys[i]] = val
	}
	return resultMap, nil
}

// mGetMapBatch is a helper function that uses pipelining to fetch multiple keys
// from Redis in batches. Under certain conditions, this can be faster than
// fetching all keys in a single MGET command.
func mGetMapBatch[R any](ctx context.Context, c *Cache, keys ...string) (MultiResult[R], error) {

	chunks := chunk(keys, c.mgetBatch)

	pipe := c.redis.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, len(chunks))
	for i := 0; i < len(chunks); i++ {
		cmds = append(cmds, pipe.MGet(ctx, chunks[i]...))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	resultMap := make(map[string]R)
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
			data, err := c.codec.Decompress([]byte(str))
			if err != nil {
				return nil, fmt.Errorf("decompress value: %w", err)
			}
			var val R
			if err := c.serializer.Unmarshal(data, &val); err != nil {
				return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
			}
			key := chunks[i][j]
			resultMap[key] = val
		}
	}

	return resultMap, nil
}

// MGet retrieves multiple keys in a single operation and returns the result as a slice.
//
// If a key doesn't exist in Redis it will not be present in the result slice. Due to missing keys
// being omitted it isn't possible to correlate the keys with their values. If you need to fetch
// multiple keys and their relationship between each other is important, use MGetMap instead.
func MGet[T any](ctx context.Context, c *Cache, keys ...string) ([]T, error) {

	// If batching is enabled and the number of keys exceeds the batch size use
	// multiple MGET commands in a pipeline.
	if c.mgetBatch > 0 && len(keys) > c.mgetBatch {
		return mGetBatch[T](ctx, c, keys...)
	}

	results, err := c.redis.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	values := make([]T, 0, len(keys))
	for i := 0; i < len(results); i++ {
		res := results[i]
		if res == nil || res == redis.Nil {
			// Some or all of the requested keys may not exist. Skip iterations
			// where the key wasn't found
			continue
		}
		str, ok := res.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
		}
		data, err := c.codec.Decompress([]byte(str))
		if err != nil {
			return nil, fmt.Errorf("decompress value: %w", err)
		}
		var val T
		if err := c.serializer.Unmarshal(data, &val); err != nil {
			return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
		}
		values = append(values, val)
	}
	return values, nil
}

func mGetBatch[T any](ctx context.Context, c *Cache, keys ...string) ([]T, error) {
	chunks := chunk(keys, c.mgetBatch)
	pipe := c.redis.Pipeline()
	cmds := make([]*redis.SliceCmd, 0, len(chunks))

	for i := 0; i < len(chunks); i++ {
		cmds = append(cmds, pipe.MGet(ctx, chunks[i]...))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}

	values := make([]T, 0, len(keys))
	for i := 0; i < len(cmds); i++ {
		results, err := cmds[i].Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
		for j := 0; j < len(results); j++ {
			res := results[j]
			if res == nil || res == redis.Nil {
				// Some or all of the requested keys may not exist. Skip iterations
				// where the key wasn't found
				continue
			}
			str, ok := res.(string)
			if !ok {
				return nil, fmt.Errorf("unexpected value type from Redis: expected %T but got %T", str, res)
			}
			data, err := c.codec.Decompress([]byte(str))
			if err != nil {
				return nil, fmt.Errorf("decompress value: %w", err)
			}
			var val T
			if err := c.serializer.Unmarshal(data, &val); err != nil {
				return nil, fmt.Errorf("unmarshall value to type %T: %w", val, err)
			}
			values = append(values, val)
		}
	}

	return values, nil
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
//		err := rcache.UpsertTTL[Person](context.Background(), c, "BillyBob", p, cb, time.Minute * 1)
//		if rcache.IsRetryable(err) {
//			continue
//		}
//		// do something useful ...
//		break
//	}
func Upsert[T any](ctx context.Context, c *Cache, key string, val T, cb UpsertCallback[T], ttl time.Duration) error {

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
			binaryValue, err := c.serializer.Marshal(newVal)
			if err != nil {
				return fmt.Errorf("marshal value: %w", err)
			}
			data, err := c.codec.Compress(binaryValue)
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
