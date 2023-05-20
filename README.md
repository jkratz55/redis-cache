# Redis Cache

Redis Cache is a library for caching any data structure in Redis. Redis Cache is meant to be used with the official Redis Go client and works by unmarshalling and marshaling data structures from/to bytes automatically. By default, Redis Cache will use msgpack to marshal/unmarshal data, but you can customize the behavior by providing your own `Marshaller` and `Unmarshaller` using the `Serialization` option with the `NewCache` function.

## Requirements

* Go 1.19+ 
* Redis 6+


## Getting Redis Cache

```shell
go get github.com/jkratz55/redis-cache
```

## Usage

Under the hood Redis Cache was designed to be used with [go-redis](https://github.com/redis/go-redis). However, it can work with any type that implements the `RedisClient` interface.

```go
type RedisClient interface {
    Get(ctx context.Context, key string) *redis.StringCmd
    MGet(ctx context.Context, keys ...string) *redis.SliceCmd
    Set(ctx context.Context, key string, val any, ttl time.Duration) *redis.StatusCmd
    SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
    SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd
    Del(ctx context.Context, keys ...string) *redis.IntCmd
    Watch(ctx context.Context, fn func(*redis.Tx) error, keys ...string) error
    Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd
    FlushDB(ctx context.Context) *redis.StatusCmd
    FlushDBAsync(ctx context.Context) *redis.StatusCmd
}
```

This means that the `Cache` type can work with the following types in the go-redis client library.

* `redis.Client`
* `redis.ClusterClient`
* `redis.Ring`

The following example shows the basic usage of the Redis Cache library.

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	rcache "github.com/jkratz55/redis-cache"
)

type Person struct {
	FirstName string
	LastName  string
	Age       int
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	cache := rcache.NewCache(client)

	if err := cache.Set(context.Background(), "person", Person{
		FirstName: "Biily",
		LastName:  "Bob",
		Age:       45,
	}); err != nil {
		panic("ohhhhh snap!")
	}

	var p Person
	if err := cache.Get(context.Background(), "person", &p); err != nil {
		panic("ohhhhh snap")
	}
	fmt.Printf("%v\n", p)

	if err := cache.Delete(context.Background(), "person"); err != nil {
		panic("ohhh snap!")
	}

	if err := cache.Get(context.Background(), "person", &p); err != rcache.ErrKeyNotFound {
		panic("ohhhhh snap, this key should be gone!")
	}
}
```

If you wanted to use json instead of msgpack you could have customized the `Cache` like the example below.

```go
marshaller := func(v any) ([]byte, error) {
    return json.Marshal(v)
}
unmarshaller := func(data []byte, v any) error {
    return json.Unmarshal(data, v)
}
rdb := rcache.NewCache(client, rcache.Serialization(marshaller, unmarshaller))
```

Because of limitations in GO's implementation of generics MGet is a function instead of a method on the `Cache` type. The `MGet` function accepts the `Cache` type as an argument to leverage the same marshaller and unmarshaller.

This library also supports atomic updates of existing keys by using the `Upsert` and `UpsertTTL` functions. If the key was modified while the upsert is in progress it will return `RetryableError` signaling the operation can be retried and the `UpsertCallback` can decide how to handle merging the changes.