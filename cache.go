package cache

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"reflect"
	"sync"
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

	// ErrInvalidTTL is an error value that signals the provided TTL is not valid
	// for the operation. Negative TTLs other than InfiniteTTL and KeepTTL are never
	// valid, and Expire doesn't accept KeepTTL either.
	ErrInvalidTTL = errors.New("invalid ttl")
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
	PTTL(ctx context.Context, key string) *redis.DurationCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	PExpire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	Persist(ctx context.Context, key string) *redis.BoolCmd
	Pipeline() redis.Pipeliner
	Close() error
}

// Cache is a thin abstraction over the go-redis client that provides serialization, compression,
// and batching of MGET operations.
//
// By default, msgpack is used for marshaling and unmarshalling the entry values. There is no
// compression by default, but it can be enabled by using an Option in New.
//
// The zero-value of Cache is not usable. Use New to create a new instance.
type Cache struct {
	redis     RedisClient
	formats   *formats
	mgetBatch int // zero-value indicates no batching

	// flights deduplicates the loads of ReadThroughSingleFlight.
	flights flightGroup

	// formatOptions collects the serialization and compression configuration
	// while options are applied. It is not used after construction.
	formatOptions
}

// New creates and initializes a new Cache instance.
//
// By default msgpack is used for marshalling and unmarshalling the entry values. There is no
// compression by default, but it can be enabled by using an Option.
//
// Every value is stored with a small header identifying the Serializer and
// CompressionCodec used to write it. This allows values written with a different
// configuration to be read as long as their Serializer and CompressionCodec are
// registered, see DecodeSerializers and DecodeCompression.
//
// New panics if the client is nil or if a Serializer or CompressionCodec doesn't
// have a valid format ID.
func New(client RedisClient, opts ...Option) *Cache {
	if client == nil {
		panic(fmt.Errorf("client cannot be nil"))
	}
	cache := &Cache{
		redis:         client,
		formatOptions: defaultFormatOptions(),
	}
	for _, opt := range opts {
		opt(cache)
	}
	cache.formats = cache.formatOptions.build()
	cache.formatOptions = formatOptions{}

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
	data, err := readValue(ctx, c.formats, c.redis.Get(ctx, key))
	if err != nil {
		return err
	}
	return c.formats.decode(ctx, data, v)
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
// A ttl value of 0 or InfiniteTTL will remove the TTL on the key if there is one, and KeepTTL leaves
// it unchanged. Any other negative ttl returns ErrInvalidTTL.
func (c *Cache) GetAndExpire(ctx context.Context, key string, v any, ttl time.Duration) error {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return err
	}
	data, err := readValue(ctx, c.formats, c.redis.GetEx(ctx, key, ttl))
	if err != nil {
		return err
	}
	return c.formats.decode(ctx, data, v)
}

// GetAndDelete retrieves a value from Redis for the given key and deletes the key from Redis.
//
// If the key does not exist, ErrKeyNotFound will be returned as the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed, the value could not
// be unmarshalled, or the value was compressed and could not be decompressed.
//
// The destination type v should be a pointer to the desired type.
func (c *Cache) GetAndDelete(ctx context.Context, key string, v any) error {
	data, err := readValue(ctx, c.formats, c.redis.GetDel(ctx, key))
	if err != nil {
		return err
	}
	return c.formats.decode(ctx, data, v)
}

// readValue returns the value read by cmd, a command reading a single key, and
// reports the lookup to the OnLookup hook. ErrKeyNotFound is returned if the key
// doesn't exist.
func readValue(ctx context.Context, f *formats, cmd *redis.StringCmd) ([]byte, error) {
	data, err := cmd.Bytes()
	switch {
	case errors.Is(err, redis.Nil):
		f.hooks.lookup(ctx, 0, 1)
		return nil, ErrKeyNotFound
	case err != nil:
		return nil, fmt.Errorf("redis: %w", err)
	}
	f.hooks.lookup(ctx, 1, 0)
	return data, nil
}

// Keys retrieves all the keys in Redis/Cache.
//
// Note: Keys under the hood is using the Redis SCAN command instead of KEYS, which is more efficient,
// but this is still an expensive operation. Every key is held in memory at once.
func (c *Cache) Keys(ctx context.Context) ([]string, error) {
	return c.Scan(ctx, "*", 1000)
}

// Set adds an entry to Redis with the given key and value. The entry is set with the provided TTL.
// If the TTL is 0 or InfiniteTTL the entry will not expire, and KeepTTL keeps the TTL of an existing
// entry. Any other negative TTL returns ErrInvalidTTL.
//
// Set will overwrite an existing value for the key if it already exists.
func (c *Cache) Set(ctx context.Context, key string, v any, ttl time.Duration) error {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return err
	}
	data, err := c.formats.encode(ctx, v)
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
func (c *Cache) SetIfAbsent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.formats.encode(ctx, v)
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
func (c *Cache) SetIfPresent(ctx context.Context, key string, v any, ttl time.Duration) (bool, error) {
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return false, err
	}
	data, err := c.formats.encode(ctx, v)
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
func (c *Cache) MSet(ctx context.Context, keyvalues map[string]any) error {
	// The key and values needs to be processed prior to calling MSet by marshalling
	// and compressing the values.
	rawData := make(map[string]any)
	for k, v := range keyvalues {
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
func (c *Cache) Delete(ctx context.Context, keys ...string) error {
	return wrapRedisErr(c.redis.Del(ctx, keys...).Err())
}

// Flush flushes the cache deleting all keys/entries with the FLUSHDB command.
//
// FLUSHDB deletes every key of the Redis database, including keys that weren't written by this Cache.
// On Redis Cluster, a *redis.ClusterClient sends FLUSHDB to a single master, so Flush only deletes the
// keys stored on that node. Use ForEachMaster on the *redis.ClusterClient to flush every node.
func (c *Cache) Flush(ctx context.Context) error {
	return wrapRedisErr(c.redis.FlushDB(ctx).Err())
}

// FlushAsync flushes the cache deleting all keys/entries asynchronously. Only keys that were present
// when FLUSH ASYNC command was received by Redis will be deleted. Any keys created during an asynchronous
// flush will be unaffected.
//
// Like Flush, FlushAsync deletes every key of the Redis database and only flushes a single node of a
// Redis Cluster.
func (c *Cache) FlushAsync(ctx context.Context) error {
	return wrapRedisErr(c.redis.FlushDBAsync(ctx).Err())
}

// wrapRedisErr prefixes a non-nil error returned by Redis with "redis: ".
func wrapRedisErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("redis: %w", err)
}

// TTL returns the remaining time to live for the given key with millisecond precision.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value. If the error is
// non-nil, but not ErrKeyNotFound, the operation on the backing Redis failed. If the key exists but
// has no TTL, InfiniteTTL will be returned.
func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	return keyTTL(ctx, c.redis, key)
}

// Expire sets a TTL on the given key with millisecond precision.
//
// A ttl of InfiniteTTL or 0 removes the TTL from the key so it never expires; the key is never
// deleted. Any other negative ttl returns ErrInvalidTTL.
//
// If the key doesn't exist ErrKeyNotFound will be returned for the error value. If the error is
// non-nil, but not ErrKeyNotFound or ErrInvalidTTL, the operation on the backing Redis failed.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return expireKey(ctx, c.redis, key, ttl)
}

// normalizeTTL validates ttl and maps the package TTL semantics to the values
// go-redis expects. InfiniteTTL is mapped to 0, which go-redis treats as no
// expiration, and KeepTTL is returned unchanged. Any other negative ttl returns
// ErrInvalidTTL, since go-redis would store the value without an expiration, fail
// with a Redis error, or leave the TTL unchanged depending on the command.
func normalizeTTL(ttl time.Duration) (time.Duration, error) {
	switch {
	case ttl == InfiniteTTL:
		return 0, nil
	case ttl < 0 && ttl != KeepTTL:
		return 0, fmt.Errorf("%w: %s", ErrInvalidTTL, ttl)
	}
	return ttl, nil
}

// keyTTL returns the remaining TTL of a key with millisecond precision. InfiniteTTL
// is returned if the key has no TTL and ErrKeyNotFound if the key doesn't exist.
func keyTTL(ctx context.Context, rdb RedisClient, key string) (time.Duration, error) {
	dur, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: %w", err)
	}

	// Redis returns -1 for PTTL command to indicate there is no TTL on the key
	if dur == -1 {
		return InfiniteTTL, nil
	}
	// Redis returns -2 for PTTL command if the key doesn't exist
	if dur == -2 {
		return 0, ErrKeyNotFound
	}
	return dur, nil
}

// expireKey sets the TTL of a key with millisecond precision.
//
// InfiniteTTL and 0 remove the TTL of the key (PERSIST). Any other negative TTL
// returns ErrInvalidTTL. ErrKeyNotFound is returned if the key doesn't exist.
func expireKey(ctx context.Context, rdb RedisClient, key string, ttl time.Duration) error {
	switch {
	case ttl == InfiniteTTL || ttl == 0:
		ok, err := rdb.Persist(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		if ok {
			return nil
		}
		// PERSIST returns 0 if the key doesn't exist or if it has no TTL. In the
		// latter case there is nothing to do.
		dur, err := rdb.PTTL(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		if dur == -2 {
			return ErrKeyNotFound
		}
		return nil
	case ttl < 0:
		return fmt.Errorf("%w: %s", ErrInvalidTTL, ttl)
	default:
		ok, err := rdb.PExpire(ctx, key, ttl).Result()
		if err != nil {
			return fmt.Errorf("redis: %w", err)
		}
		if !ok {
			return ErrKeyNotFound
		}
		return nil
	}
}

// Scan performs a Redis SCAN command to retrieve all keys matching the given pattern.
//
// The batch parameter controls the number of keys returned in each SCAN command. The larger the
// batch, the more efficient the operation is, but the more memory is used. Values less than or
// equal to zero default to 1000.
//
// If the underlying client is a *redis.ClusterClient, including one wrapped by a *FailoverClient,
// Scan will iterate over all masters in the cluster. Any other client, including a type that embeds
// one of these, is scanned with its own Scan method.
func (c *Cache) Scan(ctx context.Context, pattern string, batch int64) ([]string, error) {
	return scanKeys(ctx, c.redis, pattern, batch)
}

// masterIterator is implemented by clients that are backed by multiple master nodes,
// such as redis.ClusterClient. ForEachMaster may invoke fn concurrently.
type masterIterator interface {
	ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error
}

// scanKeys performs SCAN with the given pattern and returns all matching keys. If the
// client is a *redis.ClusterClient, including one wrapped by a *FailoverClient, every
// master is scanned. A batch less than or equal to zero defaults to 1000.
//
// Only the concrete types are unwrapped, so the Scan method of a type that embeds
// them and overrides Scan, such as a decorator scoping keys to a tenant, is used.
func scanKeys(ctx context.Context, rdb RedisClient, pattern string, batch int64) ([]string, error) {
	if batch <= 0 {
		batch = 1000
	}

	for {
		fc, ok := rdb.(*FailoverClient)
		if !ok {
			break
		}
		rdb = fc.current()
	}

	if cluster, ok := rdb.(*redis.ClusterClient); ok {
		return scanMasters(ctx, cluster, pattern, batch)
	}
	return scanClient(ctx, rdb, pattern, batch)
}

// scanClient performs SCAN on a single client.
func scanClient(ctx context.Context, client RedisClient, pattern string, batch int64) ([]string, error) {
	keys := make([]string, 0, batch)
	iter := client.Scan(ctx, 0, pattern, batch).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}
	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("redis: %w", err)
	}
	return keys, nil
}

// scanMasters performs SCAN on every master of cluster and merges the results.
func scanMasters(ctx context.Context, cluster masterIterator, pattern string, batch int64) ([]string, error) {
	// ForEachMaster invokes the callback concurrently for each master, so the results
	// of each node are merged under a lock.
	var (
		mu   sync.Mutex
		keys []string
	)
	err := cluster.ForEachMaster(ctx, func(ctx context.Context, client *redis.Client) error {
		nodeKeys, err := scanClient(ctx, client, pattern, batch)
		if err != nil {
			return err
		}
		mu.Lock()
		keys = append(keys, nodeKeys...)
		mu.Unlock()
		return nil
	})
	if err != nil {
		return nil, err
	}
	if keys == nil {
		keys = []string{}
	}
	return keys, nil
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

func (c *Cache) setMGetBatch(i int) {
	c.mgetBatch = i
}

// MultiResult holds the values read by MGet. Like the reply to the Redis MGET command, it has an
// entry for every requested key in the order the keys were requested, including keys that don't
// exist and keys requested more than once.
//
// A key is found if its value was decoded and missing if it doesn't exist in Redis. A key whose value
// couldn't be decoded is neither, its error is in the *MultiKeyError returned with the MultiResult.
//
// The zero value is an empty MultiResult.
type MultiResult[T any] struct {
	// entries holds an entry for every requested key, in the order of the keys.
	entries []resultEntry
	// values holds the values of the keys that were found, in the order of the keys.
	values []T
	// missing is the number of keys that don't exist.
	missing int
}

// resultEntry is the entry of a key in a MultiResult.
type resultEntry struct {
	key string
	// value is the index of the key's value in MultiResult.values, or entryMissing
	// or entryFailed if the key has no value.
	value int32
}

const (
	// entryMissing is the value index of a key that doesn't exist.
	entryMissing int32 = -1
	// entryFailed is the value index of a key whose value couldn't be decoded.
	entryFailed int32 = -2
)

// Len returns the number of keys requested.
func (r MultiResult[T]) Len() int {
	return len(r.entries)
}

// At returns the value of the i-th requested key and whether it was found, like the i-th element of the
// reply to MGET. If the key doesn't exist or its value couldn't be decoded, At returns the zero value
// and false. At panics if i isn't in the range [0, Len()).
func (r MultiResult[T]) At(i int) (T, bool) {
	if idx := r.entries[i].value; idx >= 0 {
		return r.values[idx], true
	}
	var zero T
	return zero, false
}

// All returns an iterator over the keys that were found and their values, in the order the keys were
// requested. A key requested more than once is yielded every time.
func (r MultiResult[T]) All() iter.Seq2[string, T] {
	return func(yield func(string, T) bool) {
		for _, e := range r.entries {
			if e.value >= 0 && !yield(e.key, r.values[e.value]) {
				return
			}
		}
	}
}

// Values returns the values of the keys that were found, in the order the keys were requested. Values
// doesn't copy the values, so the returned slice shares its memory with r.
func (r MultiResult[T]) Values() []T {
	return r.values
}

// Missing returns the keys that don't exist in Redis, in the order they were requested, for example to
// load them from the source of truth. A key requested more than once is returned every time. Keys whose
// value couldn't be decoded aren't missing, so loading the missing keys and caching them doesn't
// overwrite a value written in a format this instance can't read.
func (r MultiResult[T]) Missing() []string {
	if r.missing == 0 {
		return nil
	}
	keys := make([]string, 0, r.missing)
	for _, e := range r.entries {
		if e.value == entryMissing {
			keys = append(keys, e.key)
		}
	}
	return keys
}

// Map returns the values of the keys that were found, by key.
func (r MultiResult[T]) Map() map[string]T {
	m := make(map[string]T, len(r.values))
	for key, val := range r.All() {
		m[key] = val
	}
	return m
}

// MGet retrieves the values of multiple keys in a single operation and decodes them into T.
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
// MGet uses the MGET command. On Redis Cluster, a single MGET command can only read keys of the same
// hash slot, otherwise Redis returns a CROSSSLOT error. Use hash tags, such as "{user:1}:profile" and
// "{user:1}:friends", to store related keys in the same slot. BatchMultiGets splits keys by count, not
// by slot, so it doesn't lift this restriction.
func MGet[T any](ctx context.Context, c *Cache, keys ...string) (MultiResult[T], error) {
	return multiGet(ctx, c.redis, c.formats, c.mgetBatch, keys, func(data []byte) (T, error) {
		var val T
		err := c.formats.decode(ctx, data, &val)
		return val, err
	})
}

// multiGet reads keys with mget and decodes their values with decode.
func multiGet[T any](
	ctx context.Context,
	rdb RedisClient,
	f *formats,
	batch int,
	keys []string,
	decode func(data []byte) (T, error)) (MultiResult[T], error) {

	if len(keys) == 0 {
		return MultiResult[T]{}, nil
	}
	results, err := mget(ctx, rdb, batch, keys)
	if err != nil {
		return MultiResult[T]{}, err
	}
	return decodeMulti(ctx, f, keys, results, decode)
}

// mget returns the values of keys in the same order as keys, with nil for every key
// that doesn't exist. If batch is positive and there are more keys than batch, the
// keys are read with a pipeline of MGET commands of at most batch keys each.
func mget(ctx context.Context, rdb RedisClient, batch int, keys []string) ([]any, error) {
	var results []any
	if batch <= 0 || len(keys) <= batch {
		var err error
		results, err = rdb.MGet(ctx, keys...).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}
	} else {
		pipe := rdb.Pipeline()
		cmds := make([]*redis.SliceCmd, 0, (len(keys)+batch-1)/batch)
		for i := 0; i < len(keys); i += batch {
			cmds = append(cmds, pipe.MGet(ctx, keys[i:min(i+batch, len(keys))]...))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("redis: %w", err)
		}

		// Exec returns the error of the first command that failed, so every
		// command succeeded.
		results = make([]any, 0, len(keys))
		for _, cmd := range cmds {
			results = append(results, cmd.Val()...)
		}
	}

	if len(results) != len(keys) {
		return nil, fmt.Errorf("redis: MGET returned %d values for %d keys", len(results), len(keys))
	}
	return results, nil
}

// decodeMulti decodes the values returned by mget for keys. The errors of keys whose
// value can't be decoded are returned as a *MultiKeyError along with the result. The
// lookup is reported to the OnLookup hook.
func decodeMulti[T any](
	ctx context.Context,
	f *formats,
	keys []string,
	results []any,
	decode func(data []byte) (T, error)) (MultiResult[T], error) {

	// The entries copy the keys since the caller can reuse its slice.
	r := MultiResult[T]{
		entries: make([]resultEntry, len(keys)),
		values:  make([]T, 0, len(keys)),
	}
	var errs map[string]error
	for i, res := range results {
		r.entries[i].key = keys[i]
		if res == nil {
			// The key doesn't exist.
			r.entries[i].value = entryMissing
			r.missing++
			continue
		}

		val, err := decodeResult(res, decode)
		if err != nil {
			r.entries[i].value = entryFailed
			if errs == nil {
				errs = make(map[string]error)
			}
			errs[keys[i]] = fmt.Errorf("decode key %q to type %s: %w", keys[i], reflect.TypeFor[T](), err)
			continue
		}
		r.entries[i].value = int32(len(r.values))
		r.values = append(r.values, val)
	}

	f.hooks.lookup(ctx, len(results)-r.missing, r.missing)
	if errs != nil {
		return r, &MultiKeyError{Errors: errs}
	}
	return r, nil
}

// decodeResult decodes a single value returned by MGET.
func decodeResult[T any](res any, decode func(data []byte) (T, error)) (T, error) {
	str, ok := res.(string)
	if !ok {
		var zero T
		return zero, fmt.Errorf("unexpected value type from Redis: expected string but got %T", res)
	}
	return decode([]byte(str))
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
	if cb == nil {
		return errors.New("upsert: nil UpsertCallback")
	}
	ttl, err := normalizeTTL(ttl)
	if err != nil {
		return err
	}

	// txErr tracks errors returned by the transaction function, which are already
	// wrapped, so only errors from WATCH itself are wrapped below.
	var txErr error
	tx := func(tx *redis.Tx) error {
		txErr = upsertTx(ctx, c, tx, key, val, cb, ttl)
		return txErr
	}

	err = c.redis.Watch(ctx, tx, key)

	// Since Redis uses optimistic locking the key might have been modified during
	// the transaction. In such cases the operation will fail from Redis but the
	// operation can be retried, so we return a RetryableError.
	if errors.Is(err, redis.TxFailedErr) {
		return RetryableError{
			retryable: true,
			cause:     err,
		}
	}
	if err != nil && err != txErr {
		return fmt.Errorf("redis: %w", err)
	}
	return err
}

// upsertTx is the transaction executed by Upsert on the watched connection. The
// value is fetched, unmarshalled and passed to the UpsertCallback. The value
// returned from the UpsertCallback is the value set for the given key with ttl,
// which must already be normalized.
func upsertTx[T any](ctx context.Context, c *Cache, tx *redis.Tx, key string, val T, cb UpsertCallback[T], ttl time.Duration) error {
	found := false
	var oldVal T
	// The value is read on the watched connection so the read is part of the
	// optimistic transaction.
	data, err := tx.Get(ctx, key).Bytes()
	switch {
	case errors.Is(err, redis.Nil):
	case err != nil:
		return fmt.Errorf("redis: %w", err)
	default:
		if err := c.formats.decode(ctx, data, &oldVal); err != nil {
			return fmt.Errorf("decode key %q to type %T: %w", key, oldVal, err)
		}
		found = true
	}

	newVal := cb(found, oldVal, val)
	newData, err := c.formats.encode(ctx, newVal)
	if err != nil {
		return err
	}
	_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		return pipe.Set(ctx, key, newData, ttl).Err()
	})
	if err != nil && !errors.Is(err, redis.TxFailedErr) {
		return fmt.Errorf("redis: %w", err)
	}
	return err
}
