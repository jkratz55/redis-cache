package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/cacheproto"
	"github.com/jkratz55/redis-cache/v2/examples/protobuf/proto"
)

func main() {

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		err := client.Ping(ctx).Err()
		if err != nil {
			panic(err)
		}
	}()

	rdb := cache.NewTyped[*proto.Customer](client,
		cache.Serialization(&cacheproto.ProtobufSerializer{}))

	cust1 := &proto.Customer{
		Id:         100,
		FirstName:  "Donald",
		MiddleName: "Quakers",
		LastName:   "Duck",
		Email:      "dduck@disney.com",
		Loyalty:    true,
	}

	err := rdb.Set(context.Background(), "cust1", cust1, 0)
	if err != nil {
		panic(err)
	}

	cust2, err := rdb.Get(context.Background(), "cust1")
	if err != nil {
		panic(err)
	}
	println(cust2.String())
}
