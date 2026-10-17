package gzip

import (
	"bytes"
	"compress/flate"
	"math/rand/v2"
	"runtime"
	"strconv"
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

// text returns n bytes of text that compress about 2 times, like cached values.
func text(n int) []byte {
	rng := rand.New(rand.NewPCG(1, 1))
	words := []string{"cache", "redis", "value", "order", "customer", "shipped", "total", "item"}
	var b bytes.Buffer
	for b.Len() < n {
		b.WriteString(words[rng.IntN(len(words))])
		b.WriteString(strconv.Itoa(rng.IntN(1000)))
		b.WriteByte(' ')
	}
	return b.Bytes()[:n]
}

func TestSizeHint(t *testing.T) {
	codec := NewCodec(6)

	// Values compressing less than maxHintRatio times get an exact hint.
	data := text(10_000)
	compressed, err := codec.Compress(data)
	assert.NoError(t, err)
	assert.Less(t, len(data), len(compressed)*maxHintRatio)
	assert.Equal(t, len(data), sizeHint(compressed))
	assert.Equal(t, 0, sizeHint(compressed[:17]))

	// The hint is bounded by maxHintRatio times the compressed size, and values
	// compressing more are still decompressed.
	zeros := make([]byte, 64<<10)
	compressed, err = codec.Compress(zeros)
	assert.NoError(t, err)
	assert.Equal(t, len(compressed)*maxHintRatio, sizeHint(compressed))
	out, err := codec.Decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, zeros, out)

	// And by maxSizeHint.
	large := text(4 << 20)
	compressed, err = codec.Compress(large)
	assert.NoError(t, err)
	assert.Equal(t, maxSizeHint, sizeHint(compressed))
	out, err = codec.Decompress(compressed)
	assert.NoError(t, err)
	assert.Equal(t, large, out)
}

// TestDecompress_CorruptTrailer verifies a corrupt value declaring a large size in
// its trailer doesn't allocate that size before failing.
func TestDecompress_CorruptTrailer(t *testing.T) {
	codec := NewCodec(6)
	valid, err := codec.Compress([]byte("value"))
	assert.NoError(t, err)

	// A 64 KiB value: a valid gzip header, a corrupt body and a trailer declaring
	// 4 GiB.
	crafted := append([]byte{}, valid[:10]...)
	for i := range 64 << 10 {
		crafted = append(crafted, byte(i*7919))
	}
	crafted = append(crafted, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff)

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, err = codec.Decompress(crafted)
	runtime.ReadMemStats(&after)
	assert.Error(t, err)
	// The hint is at most 8 times the 64 KiB value, before the hint was bounded it
	// allocated 64 MiB.
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(2<<20))
}
