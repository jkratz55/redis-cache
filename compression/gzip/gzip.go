// Package gzip implements a cache.CompressionCodec using gzip from the standard
// library.
package gzip

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"sync"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/internal/limit"
)

const (
	// maxHintRatio bounds the size hint to this many times the compressed size.
	// Cached values compress about 2 to 6 times, so real values keep an exact hint.
	maxHintRatio = 8

	// maxSizeHint bounds the size hint in bytes.
	maxSizeHint = 1 << 20
)

// Codec is an implementation of cache.CompressionCodec using gzip from the standard
// library.
//
// Use NewCodec to create a new instance. The zero value is usable but doesn't
// compress, it stores values with the gzip.NoCompression level. Writers and readers
// are pooled and reused. Codec is safe for concurrent use.
type Codec struct {
	level          int
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
// malicious values exhausting memory. At most n+1 bytes are decompressed.
//
// By default, there is no limit.
func WithMaxDecodedSize(n uint64) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

// NewCodec creates a new Codec with the specified compression level. It panics
// if the level is not in the range gzip.HuffmanOnly to gzip.BestCompression.
func NewCodec(level int, opts ...Option) *Codec {
	if level < gzip.HuffmanOnly || level > gzip.BestCompression {
		panic(fmt.Errorf("gzip: invalid compression level %d", level))
	}
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Codec{
		level:          level,
		maxDecodedSize: o.maxDecodedSize,
	}
}

// Compress returns data compressed with gzip.
func (c *Codec) Compress(data []byte) ([]byte, error) {
	return c.AppendCompress(nil, data)
}

// AppendCompress appends data compressed with gzip to dst and returns the
// extended slice.
func (c *Codec) AppendCompress(dst, data []byte) ([]byte, error) {
	b := bytes.NewBuffer(dst)
	var err error
	writer, _ := c.writerPool.Get().(*gzip.Writer)
	if writer != nil {
		writer.Reset(b)
	} else {
		writer, err = gzip.NewWriterLevel(b, c.level)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		c.writerPool.Put(writer)
	}()

	if _, err = writer.Write(data); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// Decompress returns the data decompressed from gzip compressed data.
func (c *Codec) Decompress(data []byte) ([]byte, error) {
	var err error
	reader := bytes.NewReader(data)
	gzipReader, _ := c.readerPool.Get().(*gzip.Reader)
	if gzipReader != nil {
		err = gzipReader.Reset(reader)
	} else {
		gzipReader, err = gzip.NewReader(reader)
	}

	if err != nil {
		return nil, err
	}

	defer func() {
		c.readerPool.Put(gzipReader)
	}()

	return limit.ReadAll(gzipReader, sizeHint(data), c.maxDecodedSize)
}

// sizeHint returns the size of the data decompressed from data declared by its
// gzip trailer, which sizes the buffer it is decompressed into up front.
//
// The trailer is only verified once the whole value is decompressed, so a corrupt
// or crafted value can declare up to 4 GiB. The hint is therefore bounded to
// maxHintRatio times the compressed size and to maxSizeHint, and the buffer grows
// as needed for values decompressing to more.
func sizeHint(data []byte) int {
	// A gzip stream has a 10-byte header and an 8-byte trailer, whose last 4 bytes
	// are the decompressed size modulo 2^32.
	if len(data) < 18 {
		return 0
	}
	size := binary.LittleEndian.Uint32(data[len(data)-4:])
	return int(min(uint64(size), uint64(len(data))*maxHintRatio, maxSizeHint))
}

// FormatID returns cache.CodecGzip.
func (c *Codec) FormatID() uint8 {
	return cache.CodecGzip
}
