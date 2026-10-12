package cache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
)

type legacyPerson struct {
	FirstName string
	LastName  string
	Birthdate time.Time
}

func TestCache_SwitchFormats(t *testing.T) {
	_, client := newTestClient(t)

	ctx := context.Background()
	expected := legacyPerson{
		FirstName: "Bob",
		LastName:  "Dole",
		Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.UTC),
	}

	writer := cache.New(client, cache.Serialization(&cache.JsonSerializer{}), cache.Compression(gzip.NewCodec(6)))
	for _, key := range []string{"key1", "key2", "key3"} {
		assert.NoError(t, writer.Set(ctx, key, expected, 0))
	}

	for _, batch := range []int{0, 2} {
		t.Run(fmt.Sprintf("batch=%d", batch), func(t *testing.T) {
			opts := []cache.Option{
				cache.Serialization(&cache.MessagePackSerializer{}),
				cache.Compression(xorCodec{}),
				cache.DecodeCompression(gzip.NewCodec(6)),
			}
			if batch > 0 {
				opts = append(opts, cache.BatchMultiGets(batch))
			}
			reader := cache.New(client, opts...)

			var p legacyPerson
			assert.NoError(t, reader.Get(ctx, "key1", &p))
			assert.Equal(t, expected, p)

			p = legacyPerson{}
			assert.NoError(t, reader.GetAndExpire(ctx, "key1", &p, time.Minute))
			assert.Equal(t, expected, p)

			values, err := cache.MGet[legacyPerson](ctx, reader, "key1", "key2", "key3", "missing")
			assert.NoError(t, err)
			assert.Equal(t, []legacyPerson{expected, expected, expected}, values)

			m, err := cache.MGetMap[legacyPerson](ctx, reader, "key1", "key2", "key3")
			assert.NoError(t, err)
			assert.Equal(t, cache.MultiResult[legacyPerson]{"key1": expected, "key2": expected, "key3": expected}, m)

			// The new instance writes in its own format, which the old one can't read
			// unless it registers it.
			assert.NoError(t, reader.Set(ctx, "new", expected, 0))
			raw, err := client.Get(ctx, "new").Bytes()
			assert.NoError(t, err)
			assertHeader(t, raw, cache.SerializerMsgPack, xorCodec{}.FormatID())
			err = writer.Get(ctx, "new", &p)
			assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
		})
	}

	reader := cache.New(client, cache.Compression(xorCodec{}), cache.DecodeCompression(gzip.NewCodec(6)))
	var p legacyPerson
	assert.NoError(t, reader.GetAndDelete(ctx, "key1", &p))
	assert.Equal(t, expected, p)
	assert.ErrorIs(t, reader.Get(ctx, "key1", &p), cache.ErrKeyNotFound)
}

func TestCache_UnsupportedFormat(t *testing.T) {
	_, client := newTestClient(t)

	ctx := context.Background()
	writer := cache.New(client, cache.Compression(gzip.NewCodec(6)))
	assert.NoError(t, writer.Set(ctx, "key1", "value1", 0))
	assert.NoError(t, writer.Set(ctx, "key2", "value2", 0))

	for _, batch := range []int{0, 1} {
		var opts []cache.Option
		if batch > 0 {
			opts = append(opts, cache.BatchMultiGets(batch))
		}
		reader := cache.New(client, opts...)

		var s string
		err := reader.Get(ctx, "key1", &s)
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
		assert.ErrorContains(t, err, "codec id 2")

		_, err = cache.MGet[string](ctx, reader, "key1", "key2")
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)

		_, err = cache.MGetMap[string](ctx, reader, "key1", "key2")
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
	}
}

func TestCache_LegacyValues(t *testing.T) {
	_, client := newTestClient(t)

	ctx := context.Background()
	expected := legacyPerson{
		FirstName: "Bob",
		LastName:  "Dole",
		Birthdate: time.Date(1960, 10, 28, 0, 0, 0, 0, time.UTC),
	}

	// Values written by previous versions of the library have no header.
	raw, err := (&cache.JsonSerializer{}).Marshal(expected)
	assert.NoError(t, err)
	raw, err = gzip.NewCodec(6).Compress(raw)
	assert.NoError(t, err)
	for _, key := range []string{"key1", "key2", "key3"} {
		assert.NoError(t, client.Set(ctx, key, raw, 0).Err())
	}

	for _, batch := range []int{0, 2} {
		opts := []cache.Option{cache.Serialization(&cache.JsonSerializer{}), cache.Compression(gzip.NewCodec(6))}
		if batch > 0 {
			opts = append(opts, cache.BatchMultiGets(batch))
		}
		c := cache.New(client, opts...)

		var p legacyPerson
		assert.NoError(t, c.Get(ctx, "key1", &p))
		assert.Equal(t, expected, p)

		results, err := cache.MGetMap[legacyPerson](ctx, c, "key1", "key2", "key3")
		assert.NoError(t, err)
		assert.Equal(t, cache.MultiResult[legacyPerson]{"key1": expected, "key2": expected, "key3": expected}, results)
	}

	// Upsert reads the legacy value and writes it back with the header.
	c := cache.New(client, cache.Serialization(&cache.JsonSerializer{}), cache.Compression(gzip.NewCodec(6)))
	err = cache.Upsert[legacyPerson](ctx, c, "key1", legacyPerson{}, func(found bool, oldValue legacyPerson, newValue legacyPerson) legacyPerson {
		assert.True(t, found)
		assert.Equal(t, expected, oldValue)
		return oldValue
	}, 0)
	assert.NoError(t, err)
	stored, err := client.Get(ctx, "key1").Bytes()
	assert.NoError(t, err)
	assertHeader(t, stored, cache.SerializerJSON, cache.CodecGzip)
}
