// Package s2 implements a cache.CompressionCodec using the S2 block format, an
// extension of Snappy.
package s2

import (
	"slices"

	"github.com/klauspost/compress/s2"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/internal/limit"
)

// Codec is an implementation of cache.CompressionCodec using the S2 block format.
//
// Use NewCodec to create a new instance. Codec is safe for concurrent use.
type Codec struct {
	maxDecodedSize uint64
}

// Option customizes the behavior of Codec.
type Option func(*options)

type options struct {
	maxDecodedSize uint64
}

// WithMaxDecodedSize limits the size of decompressed values to n bytes. Values
// that declare a decompressed size of more than n bytes fail to decompress with an
// error wrapping cache.ErrMaxDecodedSizeExceeded before any memory is allocated
// for them, which protects against corrupt or malicious values exhausting memory.
//
// By default, there is no limit. S2 allocates the decompressed size declared by a
// value up front, so without a limit a corrupt or malicious value of a few bytes
// can allocate up to 4 GiB.
func WithMaxDecodedSize(n uint64) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

// NewCodec creates a new Codec.
func NewCodec(opts ...Option) *Codec {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Codec{maxDecodedSize: o.maxDecodedSize}
}

// Compress returns data compressed with S2.
func (c *Codec) Compress(data []byte) ([]byte, error) {
	return c.AppendCompress(nil, data)
}

// AppendCompress appends data compressed with S2 to dst and returns the extended
// slice.
func (c *Codec) AppendCompress(dst, data []byte) ([]byte, error) {
	n := s2.MaxEncodedLen(len(data))
	if n < 0 {
		return nil, s2.ErrTooLarge
	}
	dst = slices.Grow(dst, n)
	// Encode writes to the start of the slice it is passed if it has enough
	// capacity, which is the end of dst.
	compressed := s2.Encode(dst[len(dst):], data)
	return dst[:len(dst)+len(compressed)], nil
}

// Decompress returns the data decompressed from S2 compressed data.
func (c *Codec) Decompress(data []byte) ([]byte, error) {
	if c.maxDecodedSize > 0 {
		n, err := s2.DecodedLen(data)
		if err != nil {
			return nil, err
		}
		if uint64(n) > c.maxDecodedSize {
			return nil, limit.Exceeded(c.maxDecodedSize)
		}
	}
	return s2.Decode(nil, data)
}

// FormatID returns cache.CodecS2.
func (c *Codec) FormatID() uint8 {
	return cache.CodecS2
}
