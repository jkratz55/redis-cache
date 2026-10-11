package cache

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"

	"github.com/jkratz55/redis-cache/v2/compression/gzip"
)

func TestNewTyped(t *testing.T) {
	setup()
	defer tearDown()

	assert.NotPanics(t, func() {
		NewTyped[string](client)
	})
}

func TestTypedCache_Get(t *testing.T) {
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

	cache := NewTyped[string](client)

	res, err := cache.Get(context.Background(), "key123")
	assert.NoError(t, err)
	assert.Equal(t, "value123", res)

	res, err = cache.Get(context.Background(), "key456")
	assert.NoError(t, err)
	assert.Equal(t, "value456", res)

	_, err = cache.Get(context.Background(), "random")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestTypedCache_GetAndExpire(t *testing.T) {
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

	cache := NewTyped[string](client)

	res, err := cache.GetAndExpire(context.Background(), "key123", time.Second*300)
	assert.NoError(t, err)
	assert.Equal(t, "value123", res)

	ttl, err := client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Greater(t, ttl, time.Second*60)

	_, err = cache.GetAndExpire(context.Background(), "random", time.Second*300)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	// Negative ttl should keep existing ttl unchanged per Redis semantics
	res, err = cache.GetAndExpire(context.Background(), "key123", time.Duration(-1))
	assert.NoError(t, err)
	assert.Equal(t, "value123", res)
	ttl2, err := client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.LessOrEqual(t, ttl2, time.Second*300)
}

func TestTypedCache_Set(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	err := rdb.Set(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)

	err = rdb.Set(context.Background(), "key456", "value456", 0)
	assert.NoError(t, err)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(assertHeader(t, b, SerializerMsgPack, CodecNone), &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestTypedCache_SetTTL(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	err := rdb.Set(context.Background(), "key123", "value123", time.Second*1)
	assert.NoError(t, err)

	count, err := client.Exists(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Equal(t, int64(1), count)

	dur, err := client.TTL(context.Background(), "key123").Result()
	assert.NoError(t, err)
	assert.Equal(t, time.Second*1, dur)
}

func TestTypedCache_SetIfAbsent(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	ok, err := rdb.SetIfAbsent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = rdb.SetIfAbsent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(assertHeader(t, b, SerializerMsgPack, CodecNone), &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestTypedCache_SetIfPresent(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	ok, err := rdb.SetIfPresent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	err = rdb.Set(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)

	ok, err = rdb.SetIfPresent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.True(t, ok)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(assertHeader(t, b, SerializerMsgPack, CodecNone), &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestTypedCache_MSet(t *testing.T) {
	setup()
	defer tearDown()

	type person struct {
		FirstName string
		LastName  string
		Birthdate time.Time
	}

	data := map[string]person{
		"key123": {
			FirstName: "Bob",
			LastName:  "Dole",
			Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
		},
		"key 456": {
			FirstName: "Bill",
			LastName:  "Clinton",
			Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
		},
		"key789": {
			FirstName: "Jimmy",
			LastName:  "Dean",
			Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
		},
	}

	rdb := NewTyped[person](client)

	err := rdb.MSet(context.Background(), data)
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

func TestTypedCache_MGet(t *testing.T) {
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

	rdb := NewTyped[name](client)
	results, err := rdb.MGetMap(context.Background(), "key123", "key456")
	assert.NoError(t, err)
	assert.Equal(t, map[string]name{
		"key123": {
			First:  "Billy",
			Middle: "Joel",
			Last:   "Bob",
		},
		"key456": {
			First:  "Shelly",
			Middle: "Jane",
			Last:   "Bob",
		},
	}, map[string]name(results))
}

func TestTypedCache_MGetBatch(t *testing.T) {
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

	rdb := NewTyped[name](client, BatchMultiGets(1000))
	results, err := rdb.MGetMap(context.Background(), keys...)
	assert.NoError(t, err)
	assert.Equal(t, 10000, len(results))
	assert.Equal(t, MultiResult[name](expected), results)
}

func TestTypedCache_Delete(t *testing.T) {
	setup()
	defer tearDown()

	client.Set(context.Background(), "key123", "value123", 0)
	client.Set(context.Background(), "key456", "value456", 0)

	rdb := NewTyped[string](client)
	err := rdb.Delete(context.Background(), "key123", "key456")
	assert.NoError(t, err)

	_, err = rdb.Get(context.Background(), "key123")
	assert.ErrorIs(t, err, ErrKeyNotFound)

	_, err = rdb.Get(context.Background(), "key456")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestTypedCache_CustomSerialization(t *testing.T) {
	setup()
	defer tearDown()

	type Person struct {
		FirstName  string
		MiddleName string
		LastName   string
		Age        int
	}

	rdb := NewTyped[Person](client, Serialization(gobSerializer{}))
	err := rdb.Set(context.Background(), "person1", Person{
		FirstName:  "Billy",
		MiddleName: "Joel",
		LastName:   "Bob",
		Age:        99,
	}, 0)
	assert.NoError(t, err)

	p, err := rdb.Get(context.Background(), "person1")
	assert.NoError(t, err)
	assert.Equal(t, Person{
		FirstName:  "Billy",
		MiddleName: "Joel",
		LastName:   "Bob",
		Age:        99,
	}, p)
}

func TestTypedCache_TTL(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	err := client.Set(context.Background(), "test-nottl", "test", 0).Err()
	assert.NoError(t, err)

	err = client.Set(context.Background(), "test-ttl", "test", time.Second*300).Err()
	assert.NoError(t, err)

	// When Redis reports no TTL (-1), TypedCache.TTL maps it to InfiniteTTL
	ttl, err := rdb.TTL(context.Background(), "test-nottl")
	assert.NoError(t, err)
	assert.Equal(t, InfiniteTTL, ttl)

	ttl, err = rdb.TTL(context.Background(), "test-ttl")
	assert.NoError(t, err)
	assert.Equal(t, time.Second*300, ttl)

	_, err = rdb.TTL(context.Background(), "random")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestTypedCache_Expire(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	err := client.Set(context.Background(), "test-nottl", "test", 0).Err()
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
	assert.Equal(t, time.Duration(-2), ttl)

	err = rdb.Expire(context.Background(), "random", time.Second*300)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestTypedCache_Scan(t *testing.T) {
	setup()
	defer tearDown()

	assert.NoError(t, client.Set(context.Background(), "user:123", "user123", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "user:456", "user456", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "user:789", "user789", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "system:123", "system123", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "system:456", "system456", 0).Err())
	assert.NoError(t, client.Set(context.Background(), "system:789", "system789", 0).Err())

	rdb := NewTyped[string](client)

	keys, err := rdb.Scan(context.Background(), "user:*", 1000)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"user:123", "user:456", "user:789"}, keys)

	keys, err = rdb.Scan(context.Background(), "system:*", 1000)
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"system:123", "system:456", "system:789"}, keys)
}

func TestTypedCache_Client(t *testing.T) {
	setup()
	defer tearDown()

	rdb := NewTyped[string](client)

	cl := rdb.Client()
	_, ok := cl.(*redis.Client)
	assert.True(t, ok)
}

func TestTypedCache_SwitchFormats(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	expected := formatTestPerson{FirstName: "Bob", LastName: "Dole", Age: 45}

	writer := NewTyped[formatTestPerson](client, Serialization(&JsonSerializer{}), Compression(gzip.NewCodec(6)))
	for _, key := range []string{"key1", "key2", "key3"} {
		assert.NoError(t, writer.Set(ctx, key, expected, 0))
	}

	for _, batch := range []int{0, 2} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			opts := []Option{
				Serialization(&MessagePackSerializer{}),
				Compression(xorTestCodec{}),
				DecodeCompression(gzip.NewCodec(6)),
			}
			if batch > 0 {
				opts = append(opts, BatchMultiGets(batch))
			}
			reader := NewTyped[formatTestPerson](client, opts...)

			p, err := reader.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, expected, p)

			p, err = reader.GetAndExpire(ctx, "key1", time.Minute)
			assert.NoError(t, err)
			assert.Equal(t, expected, p)

			values, err := reader.MGet(ctx, "key1", "key2", "key3", "missing")
			assert.NoError(t, err)
			assert.Equal(t, []formatTestPerson{expected, expected, expected}, values)

			m, err := reader.MGetMap(ctx, "key1", "key2", "key3")
			assert.NoError(t, err)
			assert.Equal(t, MultiResult[formatTestPerson]{"key1": expected, "key2": expected, "key3": expected}, m)

			ptrReader := NewTyped[*formatTestPerson](client, opts...)
			ptr, err := ptrReader.Get(ctx, "key2")
			assert.NoError(t, err)
			assert.Equal(t, &expected, ptr)

			ptrs, err := ptrReader.MGet(ctx, "key1", "key2")
			assert.NoError(t, err)
			assert.Equal(t, []*formatTestPerson{&expected, &expected}, ptrs)

			// The new instance writes in its own format, which the old one can't read.
			assert.NoError(t, reader.Set(ctx, "new", expected, 0))
			raw, err := client.Get(ctx, "new").Bytes()
			assert.NoError(t, err)
			assertHeader(t, raw, SerializerMsgPack, xorTestCodec{}.FormatID())
			_, err = writer.Get(ctx, "new")
			assert.ErrorIs(t, err, ErrUnsupportedFormat)
		})
	}

	reader := NewTyped[formatTestPerson](client, Compression(xorTestCodec{}), DecodeCompression(gzip.NewCodec(6)))
	p, err := reader.GetAndDelete(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, expected, p)
	_, err = reader.Get(ctx, "key1")
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestTypedCache_UnsupportedFormat(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	writer := NewTyped[string](client, Compression(gzip.NewCodec(6)))
	assert.NoError(t, writer.Set(ctx, "key1", "value1", 0))
	assert.NoError(t, writer.Set(ctx, "key2", "value2", 0))

	for _, batch := range []int{0, 1} {
		opts := []Option{}
		if batch > 0 {
			opts = append(opts, BatchMultiGets(batch))
		}
		reader := NewTyped[string](client, opts...)

		_, err := reader.Get(ctx, "key1")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
		assert.ErrorContains(t, err, "codec id 2")

		_, err = reader.MGet(ctx, "key1", "key2")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)

		_, err = reader.MGetMap(ctx, "key1", "key2")
		assert.ErrorIs(t, err, ErrUnsupportedFormat)
	}
}

func TestTypedCache_LegacyValues(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	expected := formatTestPerson{FirstName: "Bob", LastName: "Dole", Age: 45}

	// Values written by previous versions of the library have no header.
	raw, err := (&JsonSerializer{}).Marshal(expected)
	assert.NoError(t, err)
	raw, err = gzip.NewCodec(6).Compress(raw)
	assert.NoError(t, err)
	for _, key := range []string{"key1", "key2", "key3"} {
		assert.NoError(t, client.Set(ctx, key, raw, 0).Err())
	}

	for _, batch := range []int{0, 2} {
		opts := []Option{Serialization(&JsonSerializer{}), Compression(gzip.NewCodec(6))}
		if batch > 0 {
			opts = append(opts, BatchMultiGets(batch))
		}
		c := NewTyped[formatTestPerson](client, opts...)

		p, err := c.Get(ctx, "key1")
		assert.NoError(t, err)
		assert.Equal(t, expected, p)

		values, err := c.MGet(ctx, "key1", "key2", "key3")
		assert.NoError(t, err)
		assert.Equal(t, []formatTestPerson{expected, expected, expected}, values)

		m, err := c.MGetMap(ctx, "key1", "key2", "key3")
		assert.NoError(t, err)
		assert.Equal(t, MultiResult[formatTestPerson]{"key1": expected, "key2": expected, "key3": expected}, m)
	}
}

func TestNewTyped_SerializerWithoutID(t *testing.T) {
	assert.Panics(t, func() {
		NewTyped[string](client, Serialization(noIDSerializer{}))
	})
	assert.Panics(t, func() {
		NewTyped[string](client, Compression(noIDCodec{}))
	})
	assert.Panics(t, func() {
		NewTyped[string](client, DecodeSerializers(noIDSerializer{}))
	})
	assert.NotPanics(t, func() {
		NewTyped[string](client, Serialization(SerializerWithID(140, noIDSerializer{})), Compression(CodecWithID(141, noIDCodec{})))
	})
}
