package brotli

import (
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/stretchr/testify/assert"
)

func TestCodec(t *testing.T) {
	codec := NewCodec(brotli.BestCompression)

	// Looping to test for re-using objects from sync.Pool
	for i := 0; i < 10; i++ {
		testStr := "This is a test string. Hopefully it flates and then deflates to the same value!"
		compressed, err := codec.Flate([]byte(testStr))
		assert.NoError(t, err)

		decompressed, err := codec.Deflate(compressed)
		assert.NoError(t, err)
		assert.Equal(t, testStr, string(decompressed))
	}
}
