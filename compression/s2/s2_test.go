package s2

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
)

func TestCodec(t *testing.T) {

	codec := NewCodec()

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

func TestCodec_MaxDecodedSize(t *testing.T) {
	const limit = 1024
	codec := NewCodec(WithMaxDecodedSize(limit))
	unlimited := NewCodec()

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
	codec := NewCodec()
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

func TestCodec_MaxDecodedSizeDeclared(t *testing.T) {
	// A 6-byte value declaring a decompressed size of 4 GiB, which Decode allocates
	// up front.
	bomb := []byte{0xff, 0xff, 0xff, 0xff, 0x0f, 0x00}
	codec := NewCodec(WithMaxDecodedSize(1 << 20))

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := codec.Decompress(bomb)
	runtime.ReadMemStats(&after)
	assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(1<<20))

	// Invalid values still fail.
	_, err = codec.Decompress([]byte{0xff})
	assert.Error(t, err)
}
