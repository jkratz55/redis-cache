package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	"github.com/go-redis/redis/v9"
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

func TestNewCache(t *testing.T) {
	assert.NotPanics(t, func() {
		NewCache(client)
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

	cache := NewCache(client)

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

func TestCache_MGet(t *testing.T) {
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

	cache := NewCache(client)
	results := make(map[string]name, 0)
	err := cache.MGet(context.Background(), []string{"key123", "key456"}, func(key string, val []byte, un Unmarshaller) {
		var s name
		if err := un(val, &s); err != nil {
			panic(err)
		}
		results[key] = s
	})
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
	}, results)
}

func TestCache_Set(t *testing.T) {
	setup()
	defer tearDown()

	cache := NewCache(client)

	err := cache.Set(context.Background(), "key123", "value123")
	assert.NoError(t, err)

	err = cache.Set(context.Background(), "key456", "value456")
	assert.NoError(t, err)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(b, &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestCache_SetTTL(t *testing.T) {
	setup()
	defer tearDown()

	cache := NewCache(client)

	err := cache.SetTTL(context.Background(), "key123", "value123", time.Second*1)
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

	cache := NewCache(client)

	ok, err := cache.SetIfAbsent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.True(t, ok)

	ok, err = cache.SetIfAbsent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(b, &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestCache_SetIfPresent(t *testing.T) {
	setup()
	defer tearDown()

	cache := NewCache(client)

	ok, err := cache.SetIfPresent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.False(t, ok)

	err = cache.Set(context.Background(), "key123", "value123")
	assert.NoError(t, err)

	ok, err = cache.SetIfPresent(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)
	assert.True(t, ok)

	b, err := client.Get(context.Background(), "key123").Bytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(b, &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
}

func TestCache_Delete(t *testing.T) {
	setup()
	defer tearDown()

	client.Set(context.Background(), "key123", "value123", 0)
	client.Set(context.Background(), "key456", "value456", 0)

	cache := NewCache(client)
	err := cache.Delete(context.Background(), "key123", "key456")
	assert.NoError(t, err)

	err = cache.Get(context.Background(), "key123", nil)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = cache.Get(context.Background(), "key456", nil)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestNewCache_CustomSerialization(t *testing.T) {
	setup()
	defer tearDown()

	marshaller := Marshaller(func(v any) ([]byte, error) {
		var buffer bytes.Buffer
		err := gob.NewEncoder(&buffer).Encode(v)
		return buffer.Bytes(), err
	})

	unmarshaller := Unmarshaller(func(b []byte, v any) error {
		reader := bytes.NewReader(b)
		return gob.NewDecoder(reader).Decode(v)
	})

	type Person struct {
		FirstName  string
		MiddleName string
		LastName   string
		Age        int
	}

	cache := NewCache(client, Serialization(marshaller, unmarshaller))
	err := cache.Set(context.Background(), "person1", Person{
		FirstName:  "Billy",
		MiddleName: "Joel",
		LastName:   "Bob",
		Age:        99,
	})
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

	cache := NewCache(client)
	results, err := MGet[name](context.Background(), cache, "key123", "key456")
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
