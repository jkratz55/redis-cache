package cache

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/assert"
	"github.com/vmihailenco/msgpack/v5"
)

var server *miniredis.Miniredis
var client rueidis.Client

func setup() {
	server = mockRedis()
	var err error
	client, err = rueidis.NewClient(rueidis.ClientOption{
		InitAddress:       []string{server.Addr()},
		DisableCache:      true,
		ForceSingleClient: true, // this is required for unit tests or rueidis tries to operate in cluster mode
	})
	if err != nil {
		panic(err)
	}
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
	setup()
	defer tearDown()

	assert.NotPanics(t, func() {
		New(client)
	})
}

func TestCache_Get(t *testing.T) {
	setup()
	defer tearDown()

	val, _ := msgpack.Marshal("value123")
	if err := client.Do(context.Background(), client.B().Set().Key("key123").
		Value(string(val)).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	val, _ = msgpack.Marshal("value456")
	if err := client.Do(context.Background(), client.B().Set().Key("key456").
		Value(string(val)).Build()).Error(); err != nil {
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

func TestCache_GetAndUpdateTTL(t *testing.T) {
	setup()
	defer tearDown()

	val, _ := msgpack.Marshal("value123")
	if err := client.Do(context.Background(), client.B().Set().Key("key123").
		Value(string(val)).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	val, _ = msgpack.Marshal("value456")
	if err := client.Do(context.Background(), client.B().Set().Key("key456").
		Value(string(val)).Ex(300*time.Second).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)

	var s string
	err := cache.GetAndUpdateTTL(context.Background(), "key123", &s, time.Second*300)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)

	ttl, err := client.Do(context.Background(), client.B().Ttl().Key("key123").Build()).AsInt64()
	assert.NoError(t, err)
	assert.Greater(t, ttl, int64(60))

	err = cache.GetAndUpdateTTL(context.Background(), "random", &s, time.Second*300)
	assert.ErrorIs(t, err, ErrKeyNotFound)

	err = cache.GetAndUpdateTTL(context.Background(), "key123", &s, 0)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
	ttl, err = client.Do(context.Background(), client.B().Ttl().Key("key123").Build()).AsInt64()
	assert.NoError(t, err)
	assert.LessOrEqual(t, ttl, int64(-1))
}

func TestCache_Set(t *testing.T) {
	setup()
	defer tearDown()

	cache := New(client)

	err := cache.Set(context.Background(), "key123", "value123", 0)
	assert.NoError(t, err)

	err = cache.Set(context.Background(), "key456", "value456", 0)
	assert.NoError(t, err)

	b, err := client.Do(context.Background(), client.B().Get().Key("key123").Build()).AsBytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(b, &s)
	assert.NoError(t, err)
	assert.Equal(t, "value123", s)
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

	b, err := client.Do(context.Background(), client.B().Get().Key("key123").Build()).AsBytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(b, &s)
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

	b, err := client.Do(context.Background(), client.B().Get().Key("key123").Build()).AsBytes()
	assert.NoError(t, err)

	var s string
	err = msgpack.Unmarshal(b, &s)
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

	results, err := client.Do(context.Background(), client.B().Mget().Key("key123", "key456", "key789").Build()).AsStrSlice()
	assert.NoError(t, err)
	assert.Equal(t, 3, len(results))

	res, err := client.Do(context.Background(), client.B().Get().Key("key123").Build()).AsBytes()
	assert.NoError(t, err)

	expected := person{
		FirstName: "Bob",
		LastName:  "Dole",
		Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.Local),
	}

	var p person
	err = msgpack.Unmarshal(res, &p)
	assert.NoError(t, err)
	assert.Equal(t, expected, p)

	err = client.Do(context.Background(), client.B().Del().Key("key123", "key456", "key789").Build()).Error()
	assert.NoError(t, err)
}

func TestCache_Delete(t *testing.T) {
	setup()
	defer tearDown()

	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("key123").Value("value123").Build()).Error())
	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("key456").Value("value456").Build()).Error())

	cache := New(client)
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

	cache := New(client, Serialization(marshaller, unmarshaller))
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
	if err := client.Do(context.Background(), client.B().Set().Key("key123").Value(string(val)).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}
	val, _ = msgpack.Marshal(name{
		First:  "Shelly",
		Middle: "Jane",
		Last:   "Bob",
	})
	if err := client.Do(context.Background(), client.B().Set().Key("key456").Value(string(val)).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)
	results, err := MGet[name](context.Background(), cache, "key123", "key456")
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
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		keys = append(keys, key)
		expected[key] = testVal
		if err := client.Do(context.Background(), client.B().Set().Key(key).Value(string(val)).Build()).Error(); err != nil {
			t.Errorf("failed to setup data in Redis")
		}
	}

	cache := New(client, BatchMultiGets(1000))
	results, err := MGet[name](context.Background(), cache, keys...)
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
	if err := client.Do(context.Background(), client.B().Set().Key("key123").Value(string(val)).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}
	val, _ = msgpack.Marshal(name{
		First:  "Shelly",
		Middle: "Jane",
		Last:   "Bob",
	})
	if err := client.Do(context.Background(), client.B().Set().Key("key456").Value(string(val)).Build()).Error(); err != nil {
		t.Errorf("failed to setup data in Redis")
	}

	cache := New(client)
	results, err := MGetValues[name](context.Background(), cache, "key123", "key456")
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

	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key%d", i)
		keys = append(keys, key)
		expected = append(expected, testVal)
		if err := client.Do(context.Background(), client.B().Set().Key(key).Value(string(val)).Build()).Error(); err != nil {
			t.Errorf("failed to setup data in Redis")
		}
	}

	cache := New(client, BatchMultiGets(1000))
	results, err := MGetValues[name](context.Background(), cache, keys...)
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

	raw, err := client.Do(context.Background(), client.B().Get().Key("BillyBob").Build()).AsBytes()
	assert.NoError(t, err)
	var actual name
	err = msgpack.Unmarshal(raw, &actual)
	assert.NoError(t, err)

	assert.Equal(t, arg, actual)
}

func TestCache_Keys(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("%d", i)
		val, err := msgpack.Marshal(key)
		assert.NoError(t, err)
		err = client.Do(context.Background(), client.B().Set().Key(key).Value(string(val)).Build()).Error()
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

	err := client.Do(context.Background(), client.B().Set().Key("test-nottl").Value("test").Build()).Error()
	assert.NoError(t, err)

	err = client.Do(context.Background(), client.B().Set().Key("test-ttl").Value("test").Ex(time.Second*300).Build()).Error()
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

	err := client.Do(context.Background(), client.B().Set().Key("test-nottl").Value("test").Build()).Error()
	assert.NoError(t, err)

	err = client.Do(context.Background(), client.B().Set().Key("test-ttl").Value("test").Ex(time.Second*300).Build()).Error()
	assert.NoError(t, err)

	err = rdb.Expire(context.Background(), "test-nottl", time.Second*300)
	assert.NoError(t, err)
	ttl, err := client.Do(context.Background(), client.B().Ttl().Key("test-nottl").Build()).AsInt64()
	assert.NoError(t, err)
	assert.Equal(t, int64(300), ttl)

	err = rdb.Expire(context.Background(), "test-ttl", InfiniteTTL)
	assert.NoError(t, err)
	ttl, err = client.Do(context.Background(), client.B().Ttl().Key("test-ttl").Build()).AsInt64()
	assert.NoError(t, err)
	assert.Equal(t, int64(-2), ttl)

	err = rdb.Expire(context.Background(), "random", time.Second*300)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestCache_ExtendTTL(t *testing.T) {
	setup()
	defer tearDown()

	rdb := New(client)

	err := client.Do(context.Background(), client.B().Set().Key("test-nottl").Value("test").Build()).Error()
	assert.NoError(t, err)

	err = client.Do(context.Background(), client.B().Set().Key("test-ttl").Value("test").Ex(time.Second*300).Build()).Error()
	assert.NoError(t, err)

	err = rdb.ExtendTTL(context.Background(), "test-nottl", time.Second*60)
	assert.NoError(t, err)
	ttl, err := client.Do(context.Background(), client.B().Ttl().Key("test-nottl").Build()).AsInt64()
	assert.NoError(t, err)
	assert.Equal(t, int64(59), ttl)

	err = rdb.ExtendTTL(context.Background(), "test-ttl", time.Second*120)
	assert.NoError(t, err)
	ttl, err = client.Do(context.Background(), client.B().Ttl().Key("test-ttl").Build()).AsInt64()
	assert.Equal(t, int64(420), ttl)

	err = rdb.ExtendTTL(context.Background(), "random", time.Hour*1)
	assert.ErrorIs(t, err, ErrKeyNotFound)
}

func TestCache_ScanKeys(t *testing.T) {
	setup()
	defer tearDown()

	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("user:123").Value("user123").Build()).Error())
	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("user:456").Value("user456").Build()).Error())
	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("user:789").Value("user789").Build()).Error())
	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("system:123").Value("system123").Build()).Error())
	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("system:456").Value("system456").Build()).Error())
	assert.NoError(t, client.Do(context.Background(), client.B().Set().Key("system:789").Value("system789").Build()).Error())

	rdb := New(client)

	keys, err := rdb.ScanKeys(context.Background(), "user:*")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"user:123", "user:456", "user:789"}, keys)

	keys, err = rdb.ScanKeys(context.Background(), "system:*")
	assert.NoError(t, err)
	assert.ElementsMatch(t, []string{"system:123", "system:456", "system:789"}, keys)
}
