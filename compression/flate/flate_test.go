package flate

import (
	"bytes"
	"compress/flate"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
)

func TestCodec(t *testing.T) {

	testStr := "This is a test string. Hopefully it flates and then deflates to the same value!"
	codec := Codec{
		Level: flate.BestCompression,
	}

	compressed, err := codec.Compress([]byte(testStr))
	assert.NoError(t, err)

	decompressed, err := codec.Decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, testStr, string(decompressed))
}

func TestNewCodec_InvalidLevel(t *testing.T) {
	assert.Panics(t, func() { NewCodec(42) })
	assert.Panics(t, func() { NewCodec(flate.HuffmanOnly - 1) })
	for level := flate.HuffmanOnly; level <= flate.BestCompression; level++ {
		assert.NotPanics(t, func() { NewCodec(level) })
	}
}

func TestCodec_Concurrent(t *testing.T) {
	for name, codec := range map[string]*Codec{
		"zero":    {},
		"default": NewCodec(flate.DefaultCompression),
	} {
		t.Run(name, func(t *testing.T) {
			var wg sync.WaitGroup
			for i := range 16 {
				wg.Go(func() {
					for j := range 50 {
						in := []byte(fmt.Sprintf("value %d-%d %s", i, j, strings.Repeat("x", j*10)))
						compressed, err := codec.Compress(in)
						assert.NoError(t, err)
						out, err := codec.Decompress(compressed)
						assert.NoError(t, err)
						assert.Equal(t, in, out)
					}
				})
			}
			wg.Wait()
		})
	}
}

func TestCodec_DecompressInvalid(t *testing.T) {
	codec := NewCodec(flate.BestSpeed)
	_, err := codec.Decompress([]byte{0xff, 0xff, 0xff})
	assert.Error(t, err)

	// The codec keeps working after a failure.
	compressed, err := codec.Compress([]byte("hello"))
	assert.NoError(t, err)
	out, err := codec.Decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, []byte("hello"), out)
}

func TestCodec_MaxDecodedSize(t *testing.T) {
	const limit = 1024
	codec := NewCodec(flate.DefaultCompression, WithMaxDecodedSize(limit))
	unlimited := NewCodec(flate.DefaultCompression)

	for _, size := range []int{0, 1, limit - 1, limit} {
		data := bytes.Repeat([]byte("a"), size)
		compressed, err := codec.Compress(data)
		assert.NoError(t, err)
		out, err := codec.Decompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, string(data), string(out))
	}

	for _, size := range []int{limit + 1, 1 << 20} {
		data := bytes.Repeat([]byte("a"), size)
		compressed, err := unlimited.Compress(data)
		assert.NoError(t, err)
		_, err = codec.Decompress(compressed)
		assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)

		// Without a limit the value is decoded.
		out, err := unlimited.Decompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, data, out)
	}
}

func TestCodec_AppendCompress(t *testing.T) {
	codec := NewCodec(flate.DefaultCompression)
	data := []byte(strings.Repeat("This is a test string. ", 100))

	compressed, err := codec.Compress(data)
	assert.NoError(t, err)

	prefix := []byte("prefix")
	out, err := codec.AppendCompress(prefix, data)
	assert.NoError(t, err)
	assert.Equal(t, "prefix", string(out[:len(prefix)]))
	assert.Equal(t, compressed, out[len(prefix):])
	assert.Equal(t, "prefix", string(prefix))

	decompressed, err := codec.Decompress(out[len(prefix):])
	assert.NoError(t, err)
	assert.Equal(t, data, decompressed)
}
