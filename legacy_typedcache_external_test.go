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

func TestTypedCache_SwitchFormats(t *testing.T) {
	_, client := newTestClient(t)

	ctx := context.Background()
	expected := extPerson{FirstName: "Bob", LastName: "Dole", Age: 45}

	writer := cache.NewTyped[extPerson](client, cache.Serialization(&cache.JsonSerializer{}), cache.Compression(gzip.NewCodec(6)))
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
			reader := cache.NewTyped[extPerson](client, opts...)

			p, err := reader.Get(ctx, "key1")
			assert.NoError(t, err)
			assert.Equal(t, expected, p)

			p, err = reader.GetAndExpire(ctx, "key1", time.Minute)
			assert.NoError(t, err)
			assert.Equal(t, expected, p)

			values, err := reader.MGet(ctx, "key1", "key2", "key3", "missing")
			assert.NoError(t, err)
			assert.Equal(t, []extPerson{expected, expected, expected}, values)

			m, err := reader.MGetMap(ctx, "key1", "key2", "key3")
			assert.NoError(t, err)
			assert.Equal(t, cache.MultiResult[extPerson]{"key1": expected, "key2": expected, "key3": expected}, m)

			ptrReader := cache.NewTyped[*extPerson](client, opts...)
			ptr, err := ptrReader.Get(ctx, "key2")
			assert.NoError(t, err)
			assert.Equal(t, &expected, ptr)

			ptrs, err := ptrReader.MGet(ctx, "key1", "key2")
			assert.NoError(t, err)
			assert.Equal(t, []*extPerson{&expected, &expected}, ptrs)

			// The new instance writes in its own format, which the old one can't read.
			assert.NoError(t, reader.Set(ctx, "new", expected, 0))
			raw, err := client.Get(ctx, "new").Bytes()
			assert.NoError(t, err)
			assertHeader(t, raw, cache.SerializerMsgPack, xorCodec{}.FormatID())
			_, err = writer.Get(ctx, "new")
			assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
		})
	}

	reader := cache.NewTyped[extPerson](client, cache.Compression(xorCodec{}), cache.DecodeCompression(gzip.NewCodec(6)))
	p, err := reader.GetAndDelete(ctx, "key1")
	assert.NoError(t, err)
	assert.Equal(t, expected, p)
	_, err = reader.Get(ctx, "key1")
	assert.ErrorIs(t, err, cache.ErrKeyNotFound)
}

func TestTypedCache_UnsupportedFormat(t *testing.T) {
	_, client := newTestClient(t)

	ctx := context.Background()
	writer := cache.NewTyped[string](client, cache.Compression(gzip.NewCodec(6)))
	assert.NoError(t, writer.Set(ctx, "key1", "value1", 0))
	assert.NoError(t, writer.Set(ctx, "key2", "value2", 0))

	for _, batch := range []int{0, 1} {
		var opts []cache.Option
		if batch > 0 {
			opts = append(opts, cache.BatchMultiGets(batch))
		}
		reader := cache.NewTyped[string](client, opts...)

		_, err := reader.Get(ctx, "key1")
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
		assert.ErrorContains(t, err, "codec id 2")

		_, err = reader.MGet(ctx, "key1", "key2")
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)

		_, err = reader.MGetMap(ctx, "key1", "key2")
		assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
	}
}

func TestTypedCache_LegacyValues(t *testing.T) {
	_, client := newTestClient(t)

	ctx := context.Background()
	expected := extPerson{FirstName: "Bob", LastName: "Dole", Age: 45}

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
		c := cache.NewTyped[extPerson](client, opts...)

		p, err := c.Get(ctx, "key1")
		assert.NoError(t, err)
		assert.Equal(t, expected, p)

		values, err := c.MGet(ctx, "key1", "key2", "key3")
		assert.NoError(t, err)
		assert.Equal(t, []extPerson{expected, expected, expected}, values)

		m, err := c.MGetMap(ctx, "key1", "key2", "key3")
		assert.NoError(t, err)
		assert.Equal(t, cache.MultiResult[extPerson]{"key1": expected, "key2": expected, "key3": expected}, m)
	}
}
