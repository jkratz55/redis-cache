package cache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNopCodec_AppendCompress(t *testing.T) {
	out, err := nopCodec{}.AppendCompress([]byte("prefix:"), []byte("value"))
	assert.NoError(t, err)
	assert.Equal(t, "prefix:value", string(out))
}

// compressOnlyCodec doesn't implement CompressAppender.
type compressOnlyCodec struct {
	err error
}

func (c compressOnlyCodec) Compress(data []byte) ([]byte, error) {
	if c.err != nil {
		return nil, c.err
	}
	return append([]byte("compressed:"), data...), nil
}

func (c compressOnlyCodec) Decompress(data []byte) ([]byte, error) { return data, nil }

func TestAppendCompress_Fallback(t *testing.T) {
	out, err := appendCompress(compressOnlyCodec{}, []byte("prefix:"), []byte("value"))
	assert.NoError(t, err)
	assert.Equal(t, "prefix:compressed:value", string(out))

	compressErr := errors.New("compress error")
	_, err = appendCompress(compressOnlyCodec{err: compressErr}, nil, []byte("value"))
	assert.ErrorIs(t, err, compressErr)
}
