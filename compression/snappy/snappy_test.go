package snappy

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodec(t *testing.T) {
	codec := NewCodec()

	testStr := "This is a test string. Hopefully, it compresses and then decompresses to the same value!"
	compressed, err := codec.Flate([]byte(testStr))
	assert.NoError(t, err)

	decompressed, err := codec.Deflate(compressed)
	assert.NoError(t, err)
	assert.Equal(t, testStr, string(decompressed))
}
