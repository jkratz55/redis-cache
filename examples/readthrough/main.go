package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	rcache "github.com/jkratz55/redis-cache/v2"
)

type Person struct {
	ID        int
	FirstName string
	LastName  string
}

func main() {

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	c := rcache.New(client)

	loader := func(ctx context.Context) (Person, error) {
		fmt.Println("Simulating database call...")
		time.Sleep(100 * time.Millisecond)
		return Person{
			ID:        1,
			FirstName: "John",
			LastName:  "Doe",
		}, nil
	}

	ctx := context.Background()
	key := "person:1"

	opts := rcache.ReadThroughOptions{
		TTL: 10 * time.Minute,
		OnCacheMiss: func(key string) {
			fmt.Printf("Cache miss for key: %s\n", key)
		},
		OnCacheHit: func(key string) {
			fmt.Printf("Cache hit for key: %s\n", key)
		},
	}

	fmt.Println("--- First Call (Cache Miss) ---")
	person, err := rcache.ReadThrough[Person](ctx, c, key, opts, loader)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Result 1: %+v\n", person)

	fmt.Println("\n--- Second Call (Cache Hit) ---")
	person, err = rcache.ReadThrough[Person](ctx, c, key, opts, loader)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Result 2: %+v\n", person)

	_ = c.Delete(ctx, key)
}
