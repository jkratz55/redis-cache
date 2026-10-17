// Package flate implements a cache.CompressionCodec using DEFLATE from the
// standard library.
package flate

import (
	"bytes"
	"compress/flate"
	"fmt"
	"io"
	"sync"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/internal/limit"
)

// Codec is a Compressor that is backed by compress/flate package
// from the standard library. The zero-value is usable but will not perform
// any compression. The compression level can be configured by setting the
// Level field or by using NewCodec, which validates the level. The valid values
// are below:
//
//	HuffmanOnly        = -2
//	DefaultCompression = -1
//	NoCompression      = 0
//	BestSpeed          = 1
//	BestCompression    = 9
//
// Writers and readers are pooled and reused. Level must not be changed after the
// Codec is first used. Codec is safe for concurrent use.
type Codec struct {
	Level int

	maxDecodedSize uint64
	writerPool     sync.Pool
	readerPool     sync.Pool
}

// Option customizes the behavior of Codec.
type Option func(*options)

type options struct {
	maxDecodedSize uint64
}

// WithMaxDecodedSize limits the size of decompressed values to n bytes. Values
// that would decompress to more than n bytes fail to decompress with an error
// wrapping cache.ErrMaxDecodedSizeExceeded, which protects against corrupt or
// malicious values exhausting memory. At most n+1 bytes are decompressed.
//
// By default, there is no limit.
func WithMaxDecodedSize(n uint64) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

// NewCodec creates a new Codec with the specified compression level. It panics
// if the level is not in the range flate.HuffmanOnly to flate.BestCompression.
func NewCodec(level int, opts ...Option) *Codec {
	if level < flate.HuffmanOnly || level > flate.BestCompression {
		panic(fmt.Errorf("flate: invalid compression level %d", level))
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Codec{Level: level, maxDecodedSize: o.maxDecodedSize}
}

// Compress returns data compressed with flate.
func (c *Codec) Compress(data []byte) ([]byte, error) {
	return c.AppendCompress(nil, data)
}

// AppendCompress appends data compressed with flate to dst and returns the
// extended slice.
func (c *Codec) AppendCompress(dst, data []byte) ([]byte, error) {
	b := bytes.NewBuffer(dst)
	w, _ := c.writerPool.Get().(*flate.Writer)
	if w != nil {
		w.Reset(b)
	} else {
		var err error
		w, err = flate.NewWriter(b, c.Level)
		if err != nil {
			return nil, err
		}
	}

	if _, err := w.Write(data); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	c.writerPool.Put(w)
	return b.Bytes(), nil
}

// Decompress returns the data decompressed from flate compressed data.
func (c *Codec) Decompress(data []byte) ([]byte, error) {
	src := bytes.NewReader(data)
	r, _ := c.readerPool.Get().(io.ReadCloser)
	if r != nil {
		if err := r.(flate.Resetter).Reset(src, nil); err != nil {
			return nil, err
		}
	} else {
		r = flate.NewReader(src)
	}

	// Flate doesn't declare the decompressed size, so there is no size hint.
	out, err := limit.ReadAll(r, 0, c.maxDecodedSize)
	if err != nil {
		return nil, err
	}
	if err := r.Close(); err != nil {
		return nil, err
	}
	c.readerPool.Put(r)
	return out, nil
}

// FormatID returns cache.CodecFlate.
func (c *Codec) FormatID() uint8 {
	return cache.CodecFlate
}
