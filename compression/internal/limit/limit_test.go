package limit

import (
	"bytes"
	"errors"
	"io"
	"math"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/assert"

	cache "github.com/jkratz55/redis-cache/v2"
)

// endlessReader returns an endless stream of 'a' and counts the bytes read.
type endlessReader struct {
	read int
}

func (r *endlessReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'a'
	}
	r.read += len(p)
	return len(p), nil
}

func TestReadAll(t *testing.T) {
	for _, size := range []int{0, 1, 511, 512, 513, 4096, 100_000} {
		data := strings.Repeat("a", size)
		for _, hint := range []int{0, size / 2, size, size * 2} {
			out, err := ReadAll(strings.NewReader(data), hint, 0)
			assert.NoError(t, err)
			assert.Equal(t, data, string(out))

			// One byte at a time.
			out, err = ReadAll(iotest.OneByteReader(strings.NewReader(data)), hint, 0)
			assert.NoError(t, err)
			assert.Equal(t, data, string(out))
		}
	}
}

func TestReadAll_ExactHintDoesntGrow(t *testing.T) {
	data := bytes.Repeat([]byte("a"), 10_000)
	out, err := ReadAll(bytes.NewReader(data), len(data), 0)
	assert.NoError(t, err)
	assert.Equal(t, data, out)
	assert.Equal(t, len(data)+1, cap(out))
}

func TestReadAll_Limit(t *testing.T) {
	for _, limit := range []uint64{1, 511, 512, 513, 10_000} {
		atLimit := strings.Repeat("a", int(limit))
		for _, hint := range []int{0, int(limit), int(limit) * 2} {
			out, err := ReadAll(strings.NewReader(atLimit), hint, limit)
			assert.NoError(t, err)
			assert.Equal(t, atLimit, string(out))

			_, err = ReadAll(strings.NewReader(atLimit+"a"), hint, limit)
			assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)

			// No more than one byte beyond the limit is read.
			r := &endlessReader{}
			_, err = ReadAll(r, hint, limit)
			assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)
			assert.Equal(t, int(limit)+1, r.read)
		}
	}
}

func TestReadAll_UnreachableLimit(t *testing.T) {
	out, err := ReadAll(strings.NewReader("value"), 0, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, "value", string(out))
}

func TestReadAll_Error(t *testing.T) {
	readErr := errors.New("read error")
	_, err := ReadAll(io.MultiReader(strings.NewReader("value"), iotest.ErrReader(readErr)), 0, 0)
	assert.ErrorIs(t, err, readErr)
}

func TestExceeded(t *testing.T) {
	err := Exceeded(1024)
	assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)
	assert.EqualError(t, err, "max decoded size exceeded: value decompresses to more than 1024 bytes")
}

// writerToReader implements io.WriterTo by writing its data in chunks.
type writerToReader struct {
	data  []byte
	chunk int
	err   error
}

func (r writerToReader) WriteTo(w io.Writer) (int64, error) {
	var n int64
	for data := r.data; len(data) > 0; {
		chunk := data[:min(r.chunk, len(data))]
		data = data[len(chunk):]
		written, err := w.Write(chunk)
		n += int64(written)
		if err != nil {
			return n, err
		}
	}
	return n, r.err
}

func TestWriteTo(t *testing.T) {
	data := bytes.Repeat([]byte("a"), 10_000)

	out, err := WriteTo(writerToReader{data: data, chunk: 1000}, 0)
	assert.NoError(t, err)
	assert.Equal(t, data, out)

	out, err = WriteTo(writerToReader{data: data, chunk: 1000}, math.MaxUint64)
	assert.NoError(t, err)
	assert.Equal(t, data, out)

	for _, limit := range []uint64{10_000, 10_001} {
		out, err = WriteTo(writerToReader{data: data, chunk: 1000}, limit)
		assert.NoError(t, err)
		assert.Equal(t, data, out)
	}

	for _, limit := range []uint64{1, 999, 1000, 9_999} {
		_, err = WriteTo(writerToReader{data: data, chunk: 1000}, limit)
		assert.ErrorIs(t, err, cache.ErrMaxDecodedSizeExceeded)
	}

	writeErr := errors.New("write error")
	_, err = WriteTo(writerToReader{data: data, chunk: 1000, err: writeErr}, 0)
	assert.ErrorIs(t, err, writeErr)
}
