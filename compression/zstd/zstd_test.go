package zstd

import (
	"crypto/rand"
	"sync"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
)

func TestCodec(t *testing.T) {
	codec, err := NewCodec(zstd.SpeedDefault)
	assert.NoError(t, err)

	t.Run("Basic compression and decompression", func(t *testing.T) {
		testStr := "This is a test string for zstd compression. It should compress and decompress correctly."
		compressed, err := codec.Compress([]byte(testStr))
		assert.NoError(t, err)
		assert.NotEmpty(t, compressed)

		decompressed, err := codec.Decompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, testStr, string(decompressed))
	})

	t.Run("Empty data", func(t *testing.T) {
		data := []byte("")
		compressed, err := codec.Compress(data)
		assert.NoError(t, err)

		decompressed, err := codec.Decompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(decompressed))
	})

	t.Run("Large data", func(t *testing.T) {
		data := make([]byte, 1024*1024) // 1MB
		_, _ = rand.Read(data)

		compressed, err := codec.Compress(data)
		assert.NoError(t, err)
		assert.NotEmpty(t, compressed)

		decompressed, err := codec.Decompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, data, decompressed)
	})

	t.Run("Concurrent access", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 20
		numIterations := 50

		for i := range numGoroutines {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := range numIterations {
					testStr := "Concurrent test data " + string(rune(id)) + "-" + string(rune(j))
					data := []byte(testStr)
					compressed, err := codec.Compress(data)
					if err != nil {
						t.Errorf("Compress error: %v", err)
						return
					}
					decompressed, err := codec.Decompress(compressed)
					if err != nil {
						t.Errorf("Decompress error: %v", err)
						return
					}
					if string(decompressed) != testStr {
						t.Errorf("Decompressed data mismatch: expected %s, got %s", testStr, string(decompressed))
						return
					}
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("Invalid data decompression", func(t *testing.T) {
		invalidData := []byte("not compressed data and definitely not a valid zstd frame")
		_, err := codec.Decompress(invalidData)
		assert.Error(t, err)
	})

	t.Run("Compression levels", func(t *testing.T) {
		levels := []zstd.EncoderLevel{
			zstd.SpeedFastest,
			zstd.SpeedDefault,
			zstd.SpeedBetterCompression,
			zstd.SpeedBestCompression,
		}

		testStr := "Testing different compression levels for zstd"
		data := []byte(testStr)

		for _, level := range levels {
			lCodec, err := NewCodec(level)
			assert.NoError(t, err)

			compressed, err := lCodec.Compress(data)
			assert.NoError(t, err)

			decompressed, err := lCodec.Decompress(compressed)
			assert.NoError(t, err)
			assert.Equal(t, testStr, string(decompressed))
		}
	})
}
