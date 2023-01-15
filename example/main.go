package main

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v9"

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

	c := rcache.NewCache(client)

	if err := c.Set(context.Background(), "person", Person{
		FirstName: "Biily",
		LastName:  "Bob",
		Age:       45,
	}); err != nil {
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

	if err := c.Get(context.Background(), "person", &p); err != rcache.ErrKeyNotFound {
		panic("ohhhhh snap, this key should be gone!")
	}

	cb := rcache.UpsertCallback[Person](func(found bool, oldValue Person, newValue Person) Person {
		fmt.Println(found)
		fmt.Println(oldValue)
		fmt.Println(newValue)
		return newValue
	})
	retries := 3
	for i := 0; i < retries; i++ {
		err := rcache.Upsert[Person](context.Background(), c, "BillyBob", p, cb)
		if rcache.IsRetryable(err) {
			continue
		}
		// do something useful ...
		break
	}

	keys, err := c.Keys(context.Background())
	if err != nil {
		panic(err)
	}
	fmt.Println(keys)
}
