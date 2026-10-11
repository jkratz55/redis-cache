package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"time"

	"github.com/redis/go-redis/v9"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
)

type Person struct {
	FirstName string
	LastName  string
	Age       int
}

// GobSerializer is a custom Serializer. Since we own the type it declares its own
// format ID by implementing cache.FormatIdentifier. Custom IDs must be in the
// range 128-255 and must never be reused for a different format once values have
// been written with them.
type GobSerializer struct{}

func (GobSerializer) Marshal(v any) ([]byte, error) {
	var buffer bytes.Buffer
	if err := gob.NewEncoder(&buffer).Encode(v); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (GobSerializer) Unmarshal(data []byte, v any) error {
	return gob.NewDecoder(bytes.NewReader(data)).Decode(v)
}

func (GobSerializer) FormatID() uint8 {
	return 200
}

// ZlibCodec represents a third-party CompressionCodec we can't modify. It has no
// format ID, so it is assigned one with cache.CodecWithID.
type ZlibCodec struct{}

func (ZlibCodec) Compress(data []byte) ([]byte, error) {
	var buffer bytes.Buffer
	w := zlib.NewWriter(&buffer)
	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (ZlibCodec) Decompress(data []byte) ([]byte, error) {
	r, err := zlib.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return io.ReadAll(r)
}

const zlibFormatID uint8 = 210

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

	ctx := context.Background()

	// Version 1 of the application uses the custom gob serializer and zlib codec.
	v1 := cache.NewTyped[Person](client,
		cache.Serialization(GobSerializer{}),
		cache.Compression(cache.CodecWithID(zlibFormatID, ZlibCodec{})))

	if err := v1.Set(ctx, "person", Person{
		FirstName: "Billy",
		LastName:  "Bob",
		Age:       45,
	}, time.Hour); err != nil {
		panic(err)
	}

	// Version 2 of the application switches to MessagePack (the default) and S2.
	// Values written by version 1 remain readable until they expire because the
	// old formats are registered for decoding only.
	v2 := cache.NewTyped[Person](client,
		cache.Compression(s2.NewCodec()),
		cache.DecodeSerializers(GobSerializer{}),
		cache.DecodeCompression(cache.CodecWithID(zlibFormatID, ZlibCodec{})))

	person, err := v2.Get(ctx, "person")
	if err != nil {
		panic(err)
	}
	fmt.Printf("read value written by v1: %+v\n", person)

	// New writes use the new format.
	person.Age++
	if err := v2.Set(ctx, "person", person, time.Hour); err != nil {
		panic(err)
	}

	person, err = v2.Get(ctx, "person")
	if err != nil {
		panic(err)
	}
	fmt.Printf("read value written by v2: %+v\n", person)

	// Version 1 doesn't know about S2, so it can't read values written by version 2.
	_, err = v1.Get(ctx, "person")
	fmt.Printf("v1 reading v2 value: %v\n", err)

	if err := v2.Delete(ctx, "person"); err != nil {
		panic(err)
	}
}
