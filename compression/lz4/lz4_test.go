package lz4

import (
	"bytes"
	"runtime"
	"strings"
	"testing"

	"github.com/pierrec/lz4/v4"
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

// blockMaxSize returns the block max size index of an LZ4 frame, stored in bits 4-6
// of the BD byte following the magic number and the FLG byte. 4 is 64 KiB, 7 is
// 4 MiB.
func blockMaxSize(frame []byte) byte {
	return frame[5] >> 4 & 0x7
}

func TestCodec_BlockSize(t *testing.T) {
	codec := NewCodec()
	data := []byte(strings.Repeat("This is a test string. ", 100))

	// Pooled writers keep the block size.
	for range 3 {
		compressed, err := codec.Compress(data)
		assert.NoError(t, err)
		assert.Equal(t, byte(4), blockMaxSize(compressed))
	}

	// Frames with 4 MiB blocks, the LZ4 default used by earlier versions, are read.
	var legacy bytes.Buffer
	w := lz4.NewWriter(&legacy)
	_, err := w.Write(data)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())
	assert.Equal(t, byte(7), blockMaxSize(legacy.Bytes()))
	out, err := codec.Decompress(legacy.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, data, out)
}

// TestCodec_AllocationsAfterGC verifies compressing and decompressing small values
// doesn't allocate large buffers once a garbage collection emptied the pools.
func TestCodec_AllocationsAfterGC(t *testing.T) {
	codec := NewCodec()
	data := []byte(strings.Repeat("This is a test string. ", 100))
	compressed, err := codec.Compress(data)
	assert.NoError(t, err)

	allocated := func(fn func()) uint64 {
		var before, after runtime.MemStats
		// The second collection also clears the pools' victim caches.
		runtime.GC()
		runtime.GC()
		runtime.ReadMemStats(&before)
		fn()
		runtime.ReadMemStats(&after)
		return after.TotalAlloc - before.TotalAlloc
	}
	// With 4 MiB blocks each call allocated more than 8 MiB.
	assert.Less(t, allocated(func() { _, _ = codec.Compress(data) }), uint64(1<<20))
	assert.Less(t, allocated(func() { _, _ = codec.Decompress(compressed) }), uint64(1<<20))
}
