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

	writer := func(ctx context.Context, key string, person Person) error {
		fmt.Printf("Simulating database update for key: %s\n", key)
		time.Sleep(100 * time.Millisecond)
		return nil
	}

	ctx := context.Background()
	key := "person:1"
	person := Person{
		ID:        1,
		FirstName: "Jane",
		LastName:  "Doe",
	}

	opts := rcache.WriteThroughOptions{
		TTL: 10 * time.Minute,
	}

	fmt.Println("--- Executing WriteThrough ---")
	err := rcache.WriteThrough[Person](ctx, c, key, person, opts, writer)
	if err != nil {
		panic(err)
	}
	fmt.Println("WriteThrough successful")

	fmt.Println("\n--- Verifying Cache ---")
	var cachedPerson Person
	err = c.Get(ctx, key, &cachedPerson)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Cached Person: %+v\n", cachedPerson)

	_ = c.Delete(ctx, key)
}
