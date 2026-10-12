package zstd

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync"
	"testing"
	"time"

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

func TestCodec_MaxDecodedSize(t *testing.T) {
	payload := make([]byte, 1<<20)

	unlimited, err := NewCodec(zstd.SpeedDefault)
	assert.NoError(t, err)
	defer unlimited.Close()
	compressed, err := unlimited.Compress(payload)
	assert.NoError(t, err)

	limited, err := NewCodec(zstd.SpeedDefault, WithMaxDecodedSize(1<<10))
	assert.NoError(t, err)
	defer limited.Close()
	_, err = limited.Decompress(compressed)
	assert.Error(t, err)

	// Values within the limit are still decoded.
	small, err := limited.Compress([]byte("hello"))
	assert.NoError(t, err)
	out, err := limited.Decompress(small)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), out)

	// Without a limit large values are decoded.
	out, err = unlimited.Decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, payload, out)
}

func TestCodec_MaxDecodedSizeSmallLimits(t *testing.T) {
	for _, limit := range []uint64{1, 512, 1023, 1024, 1025, 2048} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			codec, err := NewCodec(zstd.SpeedDefault, WithMaxDecodedSize(limit))
			assert.NoError(t, err)
			defer codec.Close()

			atLimit := bytes.Repeat([]byte("a"), int(limit))
			compressed, err := codec.Compress(atLimit)
			assert.NoError(t, err)
			out, err := codec.Decompress(compressed)
			assert.NoError(t, err)
			assert.Equal(t, atLimit, out)

			compressed, err = codec.Compress(append(atLimit, 'a'))
			assert.NoError(t, err)
			_, err = codec.Decompress(compressed)
			assert.Error(t, err)
		})
	}
}

func TestCodec_CloseWhileInUse(t *testing.T) {
	codec, err := NewCodec(zstd.SpeedDefault)
	assert.NoError(t, err)
	compressed, err := codec.Compress(bytes.Repeat([]byte("hello"), 1000))
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 1000 {
				if _, err := codec.Decompress(compressed); err != nil {
					assert.ErrorIs(t, err, ErrClosed)
					return
				}
			}
		})
	}
	time.Sleep(time.Millisecond)
	assert.NoError(t, codec.Close())
	wg.Wait()

	_, err = codec.Compress([]byte("hello"))
	assert.ErrorIs(t, err, ErrClosed)
	_, err = codec.Decompress(compressed)
	assert.ErrorIs(t, err, ErrClosed)
}

func TestCodec_CloseIdempotent(t *testing.T) {
	codec, err := NewCodec(zstd.SpeedDefault, nil)
	assert.NoError(t, err)
	assert.NoError(t, codec.Close())
	assert.NotPanics(t, func() { _ = codec.Close() })
}
