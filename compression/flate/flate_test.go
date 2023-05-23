package flate

import (
	"compress/flate"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCodec(t *testing.T) {

	testStr := "This is a test string. Hopefully it flates and then deflates to the same value!"
	codec := Codec{
		Level: flate.BestCompression,
	}

	compressed, err := codec.Flate([]byte(testStr))
	assert.NoError(t, err)

	decompressed, err := codec.Deflate(compressed)
	assert.NoError(t, err)
	assert.Equal(t, testStr, string(decompressed))
}
