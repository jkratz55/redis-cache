package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	cache "github.com/jkratz55/redis-cache"
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

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		err := client.Ping(ctx).Err()
		if err != nil {
			panic(err)
		}
	}()

	rdb := cache.NewTyped[Person](client,
		cache.GZip(),
		cache.JSON(),
		cache.BatchMultiGets(500))

	if err := rdb.Set(context.Background(), "person", Person{
		FirstName: "Biily",
		LastName:  "Bob",
		Age:       45,
	}, 0); err != nil {
		panic("ohhhhh snap!")
	}

	person, err := rdb.Get(context.Background(), "person")
	if err != nil {
		panic("ohhhhh snap")
	}
	fmt.Printf("%v\n", person)

	err = rdb.Delete(context.Background(), "person")
	if err != nil {
		panic("ohhh snap!")
	}
	fmt.Println("deleted")

	keys := make([]string, 0)
	for i := 0; i < 10000; i++ {
		keys = append(keys, fmt.Sprintf("key%d", i))
	}

	for _, key := range keys {
		err := rdb.Set(context.Background(), key, Person{
			FirstName: "Biily",
			LastName:  "Bob",
			Age:       45,
		}, 0)
		if err != nil {
			panic("ohhhhh snap!")
		}
	}

	start := time.Now()
	results, err := rdb.MGet(context.Background(), keys...)
	if err != nil {
		panic("ohhhhh snap")
	}
	fmt.Println(time.Since(start).Milliseconds())
	fmt.Println(results)
	fmt.Println(len(results))
}
