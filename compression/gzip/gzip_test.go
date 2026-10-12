package gzip

import (
	"compress/flate"
	"testing"

	"github.com/stretchr/testify/assert"
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
