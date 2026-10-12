// Package limit implements the decompressed size limits of the built-in
// compression codecs.
package limit

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"slices"

	cache "github.com/jkratz55/redis-cache/v2"
)

// minRead is the smallest buffer ReadAll reads into.
const minRead = 512

// Exceeded returns an error wrapping cache.ErrMaxDecodedSizeExceeded for a value
// that decompresses to more than maxSize bytes.
func Exceeded(maxSize uint64) error {
	return fmt.Errorf("%w: value decompresses to more than %d bytes", cache.ErrMaxDecodedSizeExceeded, maxSize)
}

// ReadAll reads from r until EOF and returns the data read.
//
// sizeHint is the expected size of the data, or 0 if it isn't known. The buffer is
// allocated with that size up front, so the hint must be bounded by the caller.
//
// If maxSize is positive, ReadAll returns Exceeded(maxSize) once more than maxSize
// bytes are read. At most maxSize+1 bytes are read and buffered.
func ReadAll(r io.Reader, sizeHint int, maxSize uint64) ([]byte, error) {
	// A limit no buffer can reach is no limit.
	if maxSize >= math.MaxInt {
		maxSize = 0
	}
	readLimit := math.MaxInt
	if maxSize > 0 {
		readLimit = int(maxSize) + 1
	}

	// The buffer has one spare byte, so reading EOF after reading the expected
	// size doesn't grow it.
	buf := make([]byte, 0, min(max(sizeHint, minRead-1)+1, readLimit))
	for {
		if len(buf) == cap(buf) {
			// Grow like append does, but not beyond the limit.
			buf = slices.Grow(buf, min(max(cap(buf), minRead), readLimit-len(buf)))
		}
		n, err := r.Read(buf[len(buf):min(cap(buf), readLimit)])
		buf = buf[:len(buf)+n]
		if maxSize > 0 && uint64(len(buf)) > maxSize {
			return nil, Exceeded(maxSize)
		}
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
	}
}

// WriteTo returns the data r writes with its WriteTo method, which is more
// efficient than reading from decompressors that decompress whole blocks, such as
// LZ4.
//
// If maxSize is positive, WriteTo returns Exceeded(maxSize) once r writes more than
// maxSize bytes. At most maxSize bytes are buffered, but r may decompress a whole
// block before writing it.
func WriteTo(r io.WriterTo, maxSize uint64) ([]byte, error) {
	var buf bytes.Buffer
	var w io.Writer = &buf
	if maxSize > 0 && maxSize < math.MaxInt {
		w = &limitedWriter{buf: &buf, maxSize: int(maxSize)}
	}
	if _, err := r.WriteTo(w); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// limitedWriter writes to buf until it would hold more than maxSize bytes.
type limitedWriter struct {
	buf     *bytes.Buffer
	maxSize int
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if len(p) > w.maxSize-w.buf.Len() {
		return 0, Exceeded(uint64(w.maxSize))
	}
	return w.buf.Write(p)
}
