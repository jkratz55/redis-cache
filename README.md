# Redis Cache

Redis Cache is a cache library backed by Redis. It is useful for storing any structure that can be marshaled and unmarshalled to/from bytes. Because the cache works with bytes under the hood it is very flexible and versatile compared to Redis built-in hash(maps). By default, this library uses msgpack to handle serialization, but the Cache can be configured to use Json, Gob, Protobuf, etc.

## Requirements

* Go 1.19+ 
* Redis 6

## Getting Redis Cache

```shell
go get github.com/jkratz/redis-cache
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
}
```

This means that the `Cache` type can work with the following types.

* redis.Client
* redis.ClusterClient
* redis.Ring

You'll need to choose the right type based on your particular use cases. More information can be found [here](https://github.com/go-redis/redis)

For the example below we are going to assume a single standalone Redis node and use Client. Out of the box Cache uses msgpack to marshall and unmarshall the value. However, if you want to use a different serialization method you can provide it as an option when creating a Cache instance using NewCache.

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