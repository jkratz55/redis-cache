package gzip

import (
	"bytes"
	"compress/flate"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
)

func TestCodec(t *testing.T) {

	codec := NewCodec(flate.BestCompression)

	// Looping to test for re-using objects from sync.Pool
	for range 10 {
		testStr := "This is a test string. Hopefully it flates and then deflates to the same value!"
		compressed, err := codec.Compress([]byte(testStr))
		assert.NoError(t, err)

		decompressed, err := codec.Decompress(compressed)
		assert.NoError(t, err)
		assert.Equal(t, testStr, string(decompressed))
	}
}

func TestNewCodec_InvalidLevel(t *testing.T) {
	assert.Panics(t, func() { NewCodec(42) })
	assert.Panics(t, func() { NewCodec(flate.HuffmanOnly - 1) })
	for level := flate.HuffmanOnly; level <= flate.BestCompression; level++ {
		assert.NotPanics(t, func() { NewCodec(level) })
	}
}

func TestCodec_MaxDecodedSize(t *testing.T) {
	const limit = 1024
	codec := NewCodec(6, WithMaxDecodedSize(limit))
	unlimited := NewCodec(6)

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
	codec := NewCodec(6)
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

func TestSizeHint(t *testing.T) {
	codec := NewCodec(6)
	data := []byte(strings.Repeat("a", 10000))
	compressed, err := codec.Compress(data)
	assert.NoError(t, err)

	assert.Equal(t, len(data), sizeHint(compressed))
	assert.Equal(t, 0, sizeHint(compressed[:17]))

	// A corrupt or crafted size is bounded by the maximum ratio of deflate.
	crafted := bytes.Clone(compressed)
	copy(crafted[len(crafted)-4:], []byte{0xff, 0xff, 0xff, 0xff})
	assert.Equal(t, len(crafted)*maxDeflateRatio, sizeHint(crafted))
	_, err = codec.Decompress(crafted)
	assert.Error(t, err)
}
