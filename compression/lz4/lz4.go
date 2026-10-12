// Package lz4 implements a cache.CompressionCodec using the LZ4 frame format.
package lz4

import (
	"bytes"
	"sync"

	"github.com/pierrec/lz4/v4"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/internal/limit"
)

// Codec is an implementation of cache.CompressionCodec using the LZ4 frame format.
//
// Use NewCodec to create a new instance. Writers and readers are pooled and reused.
// Codec is safe for concurrent use.
type Codec struct {
	maxDecodedSize uint64
	readerPool     sync.Pool
	writerPool     sync.Pool
}

// Option customizes the behavior of Codec.
type Option func(*options)

type options struct {
	maxDecodedSize uint64
}

// WithMaxDecodedSize limits the size of decompressed values to n bytes. Values
// that would decompress to more than n bytes fail to decompress with an error
// wrapping cache.ErrMaxDecodedSizeExceeded, which protects against corrupt or
// malicious values exhausting memory. Values are decompressed one LZ4 block at a
// time, so at most n bytes plus one block are decompressed.
//
// By default, there is no limit.
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

// Compress returns data compressed with LZ4.
func (c *Codec) Compress(data []byte) ([]byte, error) {
	return c.AppendCompress(nil, data)
}

// AppendCompress appends data compressed with LZ4 to dst and returns the extended
// slice.
func (c *Codec) AppendCompress(dst, data []byte) ([]byte, error) {
	buffer := bytes.NewBuffer(dst)
	lz4Writer, _ := c.writerPool.Get().(*lz4.Writer)
	if lz4Writer != nil {
		lz4Writer.Reset(buffer)
	} else {
		lz4Writer = lz4.NewWriter(buffer)
	}

	defer func() {
		c.writerPool.Put(lz4Writer)
	}()

	_, err := lz4Writer.Write(data)
	if err != nil {
		return nil, err
	}
	err = lz4Writer.Close()
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Decompress returns the data decompressed from LZ4 compressed data.
func (c *Codec) Decompress(data []byte) ([]byte, error) {
	reader := bytes.NewReader(data)
	lz4Reader, _ := c.readerPool.Get().(*lz4.Reader)
	if lz4Reader != nil {
		lz4Reader.Reset(reader)
	} else {
		lz4Reader = lz4.NewReader(reader)
	}

	defer func() {
		c.readerPool.Put(lz4Reader)
	}()

	return limit.WriteTo(lz4Reader, c.maxDecodedSize)
}

// FormatID returns cache.CodecLZ4.
func (c *Codec) FormatID() uint8 {
	return cache.CodecLZ4
}
