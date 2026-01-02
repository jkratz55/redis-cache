package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"

	rcache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
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

	codec := lz4.NewCodec()
	c := rcache.New(client, rcache.Compression(codec))

	if err := c.Set(context.Background(), "person", Person{
		FirstName: "Biily",
		LastName:  "Bob",
		Age:       45,
	}, 0); err != nil {
		panic("ohhhhh snap!")
	}

	var p Person
	if err := c.Get(context.Background(), "person", &p); err != nil {
		panic("ohhhhh snap")
	}
	fmt.Printf("%v\n", p)

	if err := c.Delete(context.Background(), "person"); err != nil {
		panic("ohhh snap!")
	}

	if err := c.Get(context.Background(), "person", &p); !errors.Is(err, rcache.ErrKeyNotFound) {
		panic("ohhhhh snap, this key should be gone!")
	}
}
