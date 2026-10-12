package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

var server *miniredis.Miniredis
var client *redis.Client

func setup() {
	server = mockRedis()
	client = redis.NewClient(&redis.Options{
		Addr: server.Addr(),
	})
}

func tearDown() {
	server.Close()
	client.Close()
}

func mockRedis() *miniredis.Miniredis {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	return s
}

type errorRedisClient struct {
	RedisClient
	getErr error
	setErr error
}

func (m *errorRedisClient) Get(ctx context.Context, key string) *redis.StringCmd {
	if m.getErr != nil {
		cmd := redis.NewStringCmd(ctx)
		cmd.SetErr(m.getErr)
		return cmd
	}
	return m.RedisClient.Get(ctx, key)
}

func (m *errorRedisClient) Set(ctx context.Context, key string, value any, expiration time.Duration) *redis.StatusCmd {
	if m.setErr != nil {
		cmd := redis.NewStatusCmd(ctx)
		cmd.SetErr(m.setErr)
		return cmd
	}
	return m.RedisClient.Set(ctx, key, value, expiration)
}

func TestNewCache(t *testing.T) {
	assert.NotPanics(t, func() {
		New(client)
	})
}

func TestCache_Get(t *testing.T) {
	setup()
	defer tearDown()

	val, _ := msgpack.Marshal("value123")
	if err := client.Set(context.Background(), "key123", val, 0).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}
	val, _ = msgpack.Marshal("value456")
	if err := client.Set(context.Background(), "key456", val, 0).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)

	var s string
	err := cache.Get(context.Background(), "key123", &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)

	err = cache.Get(context.Background(), "key456", &s)
	assert.NoError(t, err)
	assert.Equal(t, "value456", s)

	err = cache.Get(context.Background(), "random", &s)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestCache_GetAndExpire(t *testing.T) {
	setup()
	defer tearDown()

	val, _ := msgpack.Marshal("value123")
	if err := client.Set(context.Background(), "key123", val, time.Second*60).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}
	val, _ = msgpack.Marshal("value456")
	if err := client.Set(context.Background(), "key456", val, time.Second*60).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)

	var s string
	err := cache.GetAndExpire(context.Background(), "key123", &s, time.Second*300)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)

	ttl, err := client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Greater(t, ttl, time.Second*60)

	err = cache.GetAndExpire(context.Background(), "random", &s, time.Second*300)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = cache.GetAndExpire(context.Background(), "key123", &s, time.Duration(-1))
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
	ttl, err = client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.LessOrEqual(t, ttl, time.Second*300)

	err = cache.GetAndExpire(context.Background(), "key123", &s, InfiniteTTL)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
	ttl, err = client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Equal(t, ttl, time.Duration(-1))

	err = cache.GetAndExpire(context.Background(), "key123", &s, time.Duration(0))
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
	ttl, err = client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Equal(t, ttl, time.Duration(-1))
}

func TestCache_Set(t *testing.T) {
	setup()
	defer tearDown()

	cache := New(client)

	err := cache.Set(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)

	err = cache.Set(context.Background(), "key456", "value456", 0)
	assert.NoError(t, err)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(assertHeader(t, b, SerializerMsgPack, CodecNone), &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestCache_SetTTL(t *testing.T) {
	setup()
	defer tearDown()

	cache := New(client)

	err := cache.Set(context.Background(), "key123", "value123", time.Second*1)
	assert.NoError(t, err)

	count, err := client.Exists(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	dur, err := client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Second*1, dur)
}

func TestCache_SetIfAbsent(t *testing.T) {
	setup()
	defer tearDown()

	cache := New(client)

	ok, err := cache.SetIfAbsent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = cache.SetIfAbsent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(assertHeader(t, b, SerializerMsgPack, CodecNone), &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestCache_SetIfPresent(t *testing.T) {
	setup()
	defer tearDown()

	cache := New(client)

	ok, err := cache.SetIfPresent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	err = cache.Set(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)

	ok, err = cache.SetIfPresent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.True(t, ok)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(assertHeader(t, b, SerializerMsgPack, CodecNone), &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestCache_MSet(t *testing.T) {
	setup()
	defer tearDown()

	type person struct {
		FirstName string
		LastName  string
		Birthdate time.Time
	}

	data := map[string]any{
		"key123": person{
			FirstName: "Bob",
			LastName:  "Dole",
			Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
		},
		"key 456": person{
			FirstName: "Bill",
			LastName:  "Clinton",
			Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
		},
		"key789": person{
			FirstName: "Jimmy",
			LastName:  "Dean",
			Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
		},
	}

	cache := New(client)

	err := cache.MSet(context.Background(), data)
	assert.NoError(t, err)

	results, err := client.MGet(context.Background(), "key123", "key456", "key789").Result()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))

	res, err := client.Get(context.Background(), "key123").Result()
	assert.NoError(t, err)

	expected := person{
		FirstName: "Bob",
		LastName:  "Dole",
		Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
	}

	var p person
	err = msgpack.Unmarshal(assertHeader(t, []byte(res), SerializerMsgPack, CodecNone), &p)
	assert.NoError(t, err)
	assert.Equal(t, expected, p)

	err = client.Del(context.Background(), "key123", "key456", "key789").Err()
	assert.NoError(t, err)
}

func TestCache_Delete(t *testing.T) {
	setup()
	defer tearDown()

	client.Set(context.Background(), "key123", "value123", 0)
	client.Set(context.Background(), "key456", "value456", 0)

	cache := New(client)
	err := cache.Delete(context.Background(), "key123", "key456")
	assert.NoError(t, err)

	err = cache.Get(context.Background(), "key123", nil)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = cache.Get(context.Background(), "key456", nil)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

type gobSerializer struct{}

func (g gobSerializer) Marshal(v any) ([]byte, error) {
	var buffer bytes.Buffer
	err := gob.NewEncoder(&buffer).Encode(v)
	return buffer.Bytes(), err
}

func (g gobSerializer) Unmarshal(b []byte, v any) error {
	reader := bytes.NewReader(b)
	return gob.NewDecoder(reader).Decode(v)
}

func (g gobSerializer) FormatID() uint8 {
	return 130
}

// assertHeader asserts the value starts with the format header for the given
// serializer and codec IDs and returns the payload following the header.
func assertHeader(t *testing.T, b []byte, serializerID, codecID uint8) []byte {
	t.Helper()
	if !assert.GreaterOrEqual(t, len(b), headerSize) {
		return nil
	}
	assert.Equal(t, []byte{formatMagic, formatVersion, serializerID, codecID}, b[:headerSize])
	return b[headerSize:]
}

func TestNewCache_CustomSerialization(t *testing.T) {
	setup()
	defer tearDown()

	type Person struct {
		FirstName  string
		MiddleName string
		LastName   string
		Age        int
	}

	cache := New(client, Serialization(gobSerializer{}))
	err := cache.Set(context.Background(), "person1", Person{
		FirstName:  "Billy",
		MiddleName: "Joel",
		LastName:   "Bob",
		Age:        99,
	}, 0)
	assert.NoError(t, err)

	var p Person
	err = cache.Get(context.Background(), "person1", &p)
	assert.NoError(t, err)
	assert.Equal(t, Person{
		FirstName:  "Billy",
		MiddleName: "Joel",
		LastName:   "Bob",
		Age:        99,
	}, p)
}

func TestNewCache_SerializerWithoutID(t *testing.T) {
	assert.PanicsWithError(t,
		"Serializer cache.noIDSerializer has no format id: implement FormatID() uint8 returning an id in the range 128-255 or wrap it with cache.SerializerWithID",
		func() {
			New(client, Serialization(noIDSerializer{}))
		})
	assert.Panics(t, func() {
		New(client, DecodeCompression(noIDCodec{}))
	})
	assert.NotPanics(t, func() {
		New(client, Serialization(SerializerWithID(140, noIDSerializer{})))
	})
}

func TestMGet(t *testing.T) {
	setup()
	defer tearDown()

	type name struct {
		First  string
		Middle string
		Last   string
	}

	val, _ := msgpack.Marshal(name{
		First:  "Billy",
		Middle: "Joel",
		Last:   "Bob",
	})
	if err := client.Set(context.Background(), "key123", val, 0).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}
	val, _ = msgpack.Marshal(name{
		First:  "Shelly",
		Middle: "Jane",
		Last:   "Bob",
	})
	if err := client.Set(context.Background(), "key456", val, 0).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)
	results, err := MGetMap[name](context.Background(), cache, "key123", "key456")
	assert.NoError(t, err)
	assert.Equal(t, map[string]name{
		"key123": name{
			First:  "Billy",
			Middle: "Joel",
			Last:   "Bob",
		},
		"key456": name{
			First:  "Shelly",
			Middle: "Jane",
			Last:   "Bob",
		},
	}, map[string]name(results))
}

func TestMGetBatch(t *testing.T) {
	setup()
	defer tearDown()

	type name struct {
		First  string
		Middle string
		Last   string
	}

	testVal := name{
		First:  "Billy",
		Middle: "Joel",
		Last:   "Bob",
	}

	val, _ := msgpack.Marshal(testVal)
	expected := make(map[string]name)

	keys := make([]string, 0, 10000)
	for i := range 10000 {
		key := fmt.Sprintf("key%d", i)
		keys = append(keys, key)
		expected[key] = testVal
		if err := client.Set(context.Background(), key, val, 0).Err(); err != nil {
			t.Errorf("failed to setup data in Redis")
		}
	}

	cache := New(client, BatchMultiGets(1000))
	results, err := MGetMap[name](context.Background(), cache, keys...)
	assert.NoError(t, err)
	assert.Equal(t, 10000, len(results))
	assert.Equal(t, MultiResult[name](expected), results)
}

func TestMGetValues(t *testing.T) {
	setup()
	defer tearDown()

	type name struct {
		First  string
		Middle string
		Last   string
	}

	val, _ := msgpack.Marshal(name{
		First:  "Billy",
		Middle: "Joel",
		Last:   "Bob",
	})
	if err := client.Set(context.Background(), "key123", val, 0).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}
	val, _ = msgpack.Marshal(name{
		First:  "Shelly",
		Middle: "Jane",
		Last:   "Bob",
	})
	if err := client.Set(context.Background(), "key456", val, 0).Err(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)
	results, err := MGet[name](context.Background(), cache, "key123", "key456")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []name{
		{
			First:  "Billy",
			Middle: "Joel",
			Last:   "Bob",
		},
		{
			First:  "Shelly",
			Middle: "Jane",
			Last:   "Bob",
		},
	}, results)
}

func TestMGetValuesBatch(t *testing.T) {
	setup()
	defer tearDown()

	type name struct {
		First  string
		Middle string
		Last   string
	}

	testVal := name{
		First:  "Billy",
		Middle: "Joel",
		Last:   "Bob",
	}

	val, _ := msgpack.Marshal(testVal)
	expected := make([]name, 0, 10000)
	keys := make([]string, 0, 10000)

	for i := range 10000 {
		key := fmt.Sprintf("key%d", i)
		keys = append(keys, key)
		expected = append(expected, testVal)
		if err := client.Set(context.Background(), key, val, 0).Err(); err != nil {
			t.Errorf("failed to setup data in Redis")
		}
	}

	cache := New(client, BatchMultiGets(1000))
	results, err := MGet[name](context.Background(), cache, keys...)
	assert.NoError(t, err)
	assert.Equal(t, 10000, len(results))
	assert.Equal(t, expected, results)
}

func TestUpsertTTL(t *testing.T) {
	setup()
	defer tearDown()

	type name struct {
		First  string
		Middle string
		Last   string
	}

	arg := name{
		First:  "Billy",
		Middle: "Joel",
		Last:   "Bob",
	}

	called := 0
	cb := UpsertCallback[name](func(found bool, oldValue, newValue name) name {
		called++
		assert.False(t, found)
		assert.Equal(t, name{}, oldValue)
		assert.Equal(t, arg, newValue)
		return newValue
	})

	cache := New(client)
	err := Upsert[name](context.Background(), cache, "BillyBob", arg, cb, 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, called)

	raw, err := client.Get(context.Background(), "BillyBob").Result()
	assert.NoError(t, err)
	var actual name
	err = msgpack.Unmarshal(assertHeader(t, []byte(raw), SerializerMsgPack, CodecNone), &actual)
	assert.NoError(t, err)

	assert.Equal(t, arg, actual)
}

func TestCache_Keys(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)

	for i := range 10 {
		key := fmt.Sprintf("%d", i)
		err := rdb.Set(context.Background(), key, key, 0)
		assert.NoError(t, err)
	}

	var x string
	if err := rdb.Get(context.Background(), "0", &x); err != nil {
		panic(err)
	}
	fmt.Println(x)

	expected := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	actual, err := rdb.Keys(context.Background())
	assert.NoError(t, err)
	assert.ElementsMatch(t, expected, actual)
}

func TestCache_TTL(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)

	err := client.Set(context.Background(), "test-nottl", "test", InfiniteTTL).Err()
	assert.NoError(t, err)

	err = client.Set(context.Background(), "test-ttl", "test", time.Second*300).Err()
	assert.NoError(t, err)

	ttl, err := rdb.TTL(context.Background(), "test-nottl")
	assert.NoError(t, err)
	assert.Equal(t, InfiniteTTL, ttl)

	ttl, err = rdb.TTL(context.Background(), "test-ttl")
	assert.NoError(t, err)
	assert.Equal(t, time.Second*300, ttl)

	_, err = rdb.TTL(context.Background(), "random")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestCache_Expire(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)

	err := client.Set(context.Background(), "test-nottl", "test", InfiniteTTL).Err()
	assert.NoError(t, err)

	err = client.Set(context.Background(), "test-ttl", "test", time.Second*300).Err()
	assert.NoError(t, err)

	err = rdb.Expire(context.Background(), "test-nottl", time.Second*300)
	assert.NoError(t, err)
	ttl := client.TTL(context.Background(), "test-nottl").Val()
	assert.Equal(t, time.Second*300, ttl)

	err = rdb.Expire(context.Background(), "test-ttl", InfiniteTTL)
	assert.NoError(t, err)
	ttl = client.TTL(context.Background(), "test-ttl").Val()
	assert.Equal(t, time.Duration(-1), ttl)

	err = rdb.Expire(context.Background(), "random", time.Second*300)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestCache_ExpireSemantics(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	rdb := New(client)

	for _, ttl := range []time.Duration{InfiniteTTL, 0} {
		t.Run(fmt.Sprintf("ttl=%d removes TTL and keeps the key", ttl), func(t *testing.T) {
			assert.NoError(t, rdb.Set(ctx, "key", "value", time.Minute))
			assert.NoError(t, rdb.Expire(ctx, "key", ttl))

			got, err := rdb.TTL(ctx, "key")
			assert.NoError(t, err)
			assert.Equal(t, InfiniteTTL, got)
			var val string
			assert.NoError(t, rdb.Get(ctx, "key", &val))
			assert.Equal(t, "value", val)

			// Persisting a key without a TTL is a no-op.
			assert.NoError(t, rdb.Expire(ctx, "key", ttl))

			assert.ErrorIs(t, rdb.Expire(ctx, "missing", ttl), ErrKeyNotFound)
		})
	}

	t.Run("millisecond precision", func(t *testing.T) {
		assert.NoError(t, rdb.Set(ctx, "key", "value", 0))
		assert.NoError(t, rdb.Expire(ctx, "key", 1500*time.Millisecond))
		got, err := rdb.TTL(ctx, "key")
		assert.NoError(t, err)
		assert.Equal(t, 1500*time.Millisecond, got)
	})

	t.Run("negative TTL is invalid", func(t *testing.T) {
		assert.NoError(t, rdb.Set(ctx, "key", "value", time.Minute))
		assert.ErrorIs(t, rdb.Expire(ctx, "key", -5*time.Second), ErrInvalidTTL)
		got, err := rdb.TTL(ctx, "key")
		assert.NoError(t, err)
		assert.Equal(t, time.Minute, got)
	})

	t.Run("missing key", func(t *testing.T) {
		assert.ErrorIs(t, rdb.Expire(ctx, "missing", time.Minute), ErrKeyNotFound)
	})
}

func TestCache_InfiniteTTL(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	rdb := New(client)

	assertNoTTL := func(t *testing.T, key string) {
		t.Helper()
		got, err := rdb.TTL(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, InfiniteTTL, got)
	}

	assert.NoError(t, rdb.Set(ctx, "set", "value", InfiniteTTL))
	assertNoTTL(t, "set")

	ok, err := rdb.SetIfAbsent(ctx, "absent", "value", InfiniteTTL)
	assert.NoError(t, err)
	assert.True(t, ok)
	assertNoTTL(t, "absent")

	assert.NoError(t, rdb.Set(ctx, "present", "value", time.Minute))
	ok, err = rdb.SetIfPresent(ctx, "present", "value2", InfiniteTTL)
	assert.NoError(t, err)
	assert.True(t, ok)
	assertNoTTL(t, "present")

	assert.NoError(t, rdb.Set(ctx, "getex", "value", time.Minute))
	var val string
	assert.NoError(t, rdb.GetAndExpire(ctx, "getex", &val, InfiniteTTL))
	assert.Equal(t, "value", val)
	assertNoTTL(t, "getex")

	assert.NoError(t, rdb.Set(ctx, "upsert", "value", time.Minute))
	err = Upsert[string](ctx, rdb, "upsert", "value2", func(found bool, oldValue, newValue string) string {
		return newValue
	}, InfiniteTTL)
	assert.NoError(t, err)
	assertNoTTL(t, "upsert")
}

func TestCache_ErrorWrapping(t *testing.T) {
	ctx := context.Background()

	t.Run("redis errors are prefixed once", func(t *testing.T) {
		s := miniredis.RunT(t)
		rdb := redis.NewClient(&redis.Options{Addr: s.Addr()})
		defer rdb.Close()
		c := New(rdb)
		s.SetError("boom")

		for name, err := range map[string]error{
			"Delete":     c.Delete(ctx, "key"),
			"Flush":      c.Flush(ctx),
			"FlushAsync": c.FlushAsync(ctx),
			"Expire":     c.Expire(ctx, "key", time.Minute),
			"Persist":    c.Expire(ctx, "key", InfiniteTTL),
			"TTL": func() error {
				_, err := c.TTL(ctx, "key")
				return err
			}(),
			"Upsert": Upsert[string](ctx, c, "key", "v", func(found bool, oldValue, newValue string) string {
				return newValue
			}, 0),
		} {
			if assert.Error(t, err, name) {
				assert.Equal(t, "redis: boom", err.Error(), name)
			}
		}
	})

	t.Run("successful operations return nil", func(t *testing.T) {
		setup()
		defer tearDown()
		c := New(client)
		assert.NoError(t, c.Delete(ctx, "missing"))
		assert.NoError(t, c.Flush(ctx))
		assert.NoError(t, c.FlushAsync(ctx))
	})

	t.Run("multi get decode errors name the key", func(t *testing.T) {
		setup()
		defer tearDown()
		assert.NoError(t, client.Set(ctx, "good", mustMsgpack(t, "value"), 0).Err())
		assert.NoError(t, client.Set(ctx, "bad", []byte{0xC1, 0x01, SerializerMsgPack, CodecNone, 0xC1}, 0).Err())

		for _, batch := range []int{0, 1} {
			var opts []Option
			if batch > 0 {
				opts = append(opts, BatchMultiGets(batch))
			}
			c := New(client, opts...)

			_, err := MGet[string](ctx, c, "good", "bad")
			assert.ErrorContains(t, err, `decode key "bad"`)
			_, err = MGetMap[string](ctx, c, "good", "bad")
			assert.ErrorContains(t, err, `decode key "bad"`)
		}
	})
}

func mustMsgpack(t *testing.T, v any) []byte {
	t.Helper()
	data, err := msgpack.Marshal(v)
	assert.NoError(t, err)
	return data
}

func TestUpsert_Errors(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	c := New(client)

	t.Run("nil callback", func(t *testing.T) {
		assert.Error(t, Upsert[string](ctx, c, "key", "value", nil, 0))
		assert.False(t, server.Exists("key"))
	})

	t.Run("decode error is not prefixed as redis error", func(t *testing.T) {
		assert.NoError(t, client.Set(ctx, "bad", []byte{0xC1, 0x01, SerializerMsgPack, CodecNone, 0xC1}, 0).Err())
		called := false
		err := Upsert[string](ctx, c, "bad", "value", func(found bool, oldValue, newValue string) string {
			called = true
			return newValue
		}, 0)
		assert.Error(t, err)
		assert.False(t, called)
		assert.True(t, strings.HasPrefix(err.Error(), `decode key "bad"`), err.Error())
		assert.NotContains(t, err.Error(), "redis: ")
	})

	t.Run("found and old value are passed to the callback", func(t *testing.T) {
		assert.NoError(t, c.Set(ctx, "counter", 41, 0))
		err := Upsert[int](ctx, c, "counter", 1, func(found bool, oldValue, newValue int) int {
			assert.True(t, found)
			return oldValue + newValue
		}, 0)
		assert.NoError(t, err)
		var v int
		assert.NoError(t, c.Get(ctx, "counter", &v))
		assert.Equal(t, 42, v)

		err = Upsert[int](ctx, c, "new-counter", 1, func(found bool, oldValue, newValue int) int {
			assert.False(t, found)
			assert.Equal(t, 0, oldValue)
			return newValue
		}, 0)
		assert.NoError(t, err)
	})
}

func TestCache_Scan(t *testing.T) {
	setup()
	defer tearDown()

	assert.NoError(t, client.Set(context.Background(), "user:123", "user123", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "user:456", "user456", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "user:789", "user789", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "system:123", "system123", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "system:456", "system456", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "system:789", "system789", 0).Err())

	rdb := New(client)

	keys, err := rdb.Scan(context.Background(), "user:*", 0)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"user:123", "user:456", "user:789"}, keys)

	keys, err = rdb.Scan(context.Background(), "system:*", 0)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"system:123", "system:456", "system:789"}, keys)
}

// fakeClusterClient simulates a redis.ClusterClient by invoking the ForEachMaster
// callback concurrently for each node, like go-redis does.
type fakeClusterClient struct {
	RedisClient
	nodes []*redis.Client
}

func (f *fakeClusterClient) ForEachMaster(ctx context.Context, fn func(ctx context.Context, client *redis.Client) error) error {
	var wg sync.WaitGroup
	errs := make([]error, len(f.nodes))
	for i, node := range f.nodes {
		wg.Go(func() {
			errs[i] = fn(ctx, node)
		})
	}
	wg.Wait()
	return errors.Join(errs...)
}

// newFakeCluster starts n miniredis servers, each holding perNode keys, and returns
// a fakeClusterClient for them along with all keys written.
func newFakeCluster(t *testing.T, n, perNode int) (*fakeClusterClient, []string) {
	t.Helper()
	cluster := &fakeClusterClient{}
	var keys []string
	for i := range n {
		s := miniredis.RunT(t)
		node := redis.NewClient(&redis.Options{Addr: s.Addr()})
		t.Cleanup(func() { _ = node.Close() })
		for j := range perNode {
			key := fmt.Sprintf("key:%d:%d", i, j)
			assert.NoError(t, node.Set(context.Background(), key, "v", 0).Err())
			keys = append(keys, key)
		}
		cluster.nodes = append(cluster.nodes, node)
	}
	cluster.RedisClient = cluster.nodes[0]
	return cluster, keys
}

func TestCache_ScanCluster(t *testing.T) {
	ctx := context.Background()

	t.Run("merges keys from all masters", func(t *testing.T) {
		cluster, expected := newFakeCluster(t, 8, 200)
		keys, err := New(cluster).Scan(ctx, "key:*", 10)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expected, keys)
	})

	t.Run("unwraps FailoverClient", func(t *testing.T) {
		cluster, expected := newFakeCluster(t, 3, 50)
		secondary := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
		fc := NewFailoverClient(cluster, secondary, FailoverOptions().SetCheckInterval(time.Hour))
		defer func() {
			close(fc.closeChan)
			<-fc.done
			_ = secondary.Close()
		}()

		keys, err := New(fc).Scan(ctx, "key:*", 0)
		assert.NoError(t, err)
		assert.ElementsMatch(t, expected, keys)
	})

	t.Run("returns nil keys on error", func(t *testing.T) {
		cluster, _ := newFakeCluster(t, 3, 10)
		assert.NoError(t, cluster.nodes[1].Close())
		keys, err := New(cluster).Scan(ctx, "key:*", 0)
		assert.Error(t, err)
		assert.Nil(t, keys)
	})
}

func TestCache_ScanFailoverClient(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	assert.NoError(t, client.Set(ctx, "user:1", "v", 0).Err())
	assert.NoError(t, client.Set(ctx, "user:2", "v", 0).Err())

	secondary := redis.NewClient(&redis.Options{Addr: miniredis.RunT(t).Addr()})
	fc := NewFailoverClient(client, secondary, FailoverOptions().SetCheckInterval(time.Hour))
	defer func() {
		close(fc.closeChan)
		<-fc.done
		_ = secondary.Close()
	}()

	keys, err := New(fc).Scan(ctx, "user:*", -1)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"user:1", "user:2"}, keys)
}

// xorTestCodec is a custom codec used to verify custom format IDs.
type xorTestCodec struct{}

func (xorTestCodec) Compress(data []byte) ([]byte, error) { return xor(data), nil }

func (xorTestCodec) Decompress(data []byte) ([]byte, error) { return xor(data), nil }

func (xorTestCodec) FormatID() uint8 { return 201 }
