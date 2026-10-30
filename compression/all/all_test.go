package all_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/cacheotel"
	"github.com/jkratz55/redis-cache/v2/compression/all"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
	"github.com/jkratz55/redis-cache/v2/compression/zstd"
)

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newCodecs(t *testing.T, opts ...all.Option) *all.Codecs {
	t.Helper()
	codecs, err := all.NewCodecs(opts...)
	if !assert.NoError(t, err) {
		return nil
	}
	t.Cleanup(func() { assert.NoError(t, codecs.Close()) })
	return codecs
}

func TestDecodeCompression_ReportedScenario(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	codecs := newCodecs(t)
	if codecs == nil {
		return
	}

	writer := cache.New(client,
		cache.Serialization(cacheotel.NewInstrumentedSerializer(&cache.MessagePackSerializer{})),
		cache.Compression(cacheotel.NewInstrumentedCompressionCodec(snappy.NewCodec())))
	assert.NoError(t, writer.Set(ctx, "shared", "value", 0))

	readerWithoutCodecs := cache.New(client,
		cache.Serialization(cacheotel.NewInstrumentedSerializer(&cache.JsonSerializer{})),
		cache.Compression(cacheotel.NewInstrumentedCompressionCodec(lz4.NewCodec())))
	var actual string
	err := readerWithoutCodecs.Get(ctx, "shared", &actual)
	assert.ErrorIs(t, err, cache.ErrUnsupportedFormat)
	assert.ErrorContains(t, err, "codec id 5 (snappy): register it with cache.DecodeCompression")

	reader := cache.New(client,
		cache.Serialization(cacheotel.NewInstrumentedSerializer(&cache.JsonSerializer{})),
		cache.Compression(cacheotel.NewInstrumentedCompressionCodec(lz4.NewCodec())),
		codecs.DecodeCompression())
	assert.NoError(t, reader.Get(ctx, "shared", &actual))
	assert.Equal(t, "value", actual)
}

func TestDecodeCompression_AllCodecs(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	codecs := newCodecs(t)
	if codecs == nil {
		return
	}

	codecList := codecs.List()
	assert.Len(t, codecList, 6)
	for i, writerCodec := range codecList {
		t.Run(fmt.Sprintf("codec-%d", i), func(t *testing.T) {
			key := fmt.Sprintf("value-%d", i)
			value := fmt.Sprintf("content-%d", i)
			writer := cache.New(client, cache.Compression(writerCodec))
			assert.NoError(t, writer.Set(ctx, key, value, 0))

			readerCodec := codecList[(i+1)%len(codecList)]
			readerOptions := []cache.Option{
				cache.Serialization(&cache.JsonSerializer{}),
				cache.Compression(readerCodec),
				codecs.DecodeCompression(),
			}
			reader := cache.New(client, readerOptions...)
			var actual string
			assert.NoError(t, reader.Get(ctx, key, &actual))
			assert.Equal(t, value, actual)

			typedReader := cache.NewTyped[string](client, readerOptions...)
			typedActual, err := typedReader.Get(ctx, key)
			assert.NoError(t, err)
			assert.Equal(t, value, typedActual)
		})
	}
}

func TestDecodeCompression_ConfiguredCodecWins(t *testing.T) {
	ctx := context.Background()
	client := newClient(t)
	codecs := newCodecs(t)
	if codecs == nil {
		return
	}

	value := strings.Repeat("configured lz4 codec", 8)
	for _, instrumented := range []bool{false, true} {
		name := "unwrapped"
		if instrumented {
			name = "cacheotel"
		}
		t.Run(name, func(t *testing.T) {
			codec := cache.CompressionCodec(lz4.NewCodec())
			if instrumented {
				codec = cacheotel.NewInstrumentedCompressionCodec(codec)
			}
			writer := cache.New(client, cache.Compression(codec), codecs.DecodeCompression())
			assert.NoError(t, writer.Set(ctx, "configured-cache", value, 0))
			raw, err := client.Get(ctx, "configured-cache").Bytes()
			if assert.NoError(t, err) && assert.GreaterOrEqual(t, len(raw), 4) {
				assert.Equal(t, byte(cache.CodecLZ4), raw[3])
			}

			typedWriter := cache.NewTyped[string](client, cache.Compression(codec), codecs.DecodeCompression())
			assert.NoError(t, typedWriter.Set(ctx, "configured-typed", value, 0))
			raw, err = client.Get(ctx, "configured-typed").Bytes()
			if assert.NoError(t, err) && assert.GreaterOrEqual(t, len(raw), 4) {
				assert.Equal(t, byte(cache.CodecLZ4), raw[3])
			}

			limitedCodec := cache.CompressionCodec(lz4.NewCodec(lz4.WithMaxDecodedSize(1)))
			if instrumented {
				limitedCodec = cacheotel.NewInstrumentedCompressionCodec(limitedCodec)
			}
			readerOptions := []cache.Option{cache.Compression(limitedCodec), codecs.DecodeCompression()}
			reader := cache.New(client, readerOptions...)
			var actual string
			err = reader.Get(ctx, "configured-cache", &actual)
			assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)

			typedReader := cache.NewTyped[string](client, readerOptions...)
			_, err = typedReader.Get(ctx, "configured-typed")
			assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)
		})
	}
}

func TestWithMaxDecodedSize_AppliesToEveryCodec(t *testing.T) {
	codecs := newCodecs(t, all.WithMaxDecodedSize(8))
	if codecs == nil {
		return
	}

	payload := []byte(strings.Repeat("a", 64))
	for _, codec := range codecs.List() {
		compressed, err := codec.Compress(payload)
		if assert.NoError(t, err) {
			_, err = codec.Decompress(compressed)
			assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)
		}
	}
}

func TestCodecs_Close(t *testing.T) {
	codecs, err := all.NewCodecs()
	if !assert.NoError(t, err) {
		return
	}

	var zstdCodec *zstd.Codec
	for _, codec := range codecs.List() {
		if candidate, ok := codec.(*zstd.Codec); ok {
			zstdCodec = candidate
			break
		}
	}
	if !assert.NotNil(t, zstdCodec) {
		return
	}

	assert.NoError(t, codecs.Close())
	_, err = zstdCodec.Decompress(nil)
	assert.True(t, errors.Is(err, zstd.ErrClosed))
	assert.NoError(t, codecs.Close())
}
