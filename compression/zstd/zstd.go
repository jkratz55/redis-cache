// Package zstd implements a cache.CompressionCodec using Zstandard.
package zstd

import (
	"errors"
	"fmt"
	"slices"
	"sync"

	"github.com/klauspost/compress/zstd"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/internal/limit"
)

// ErrClosed is returned by Compress and Decompress once Close has been called.
var ErrClosed = errors.New("zstd: codec closed")

// Codec is an implementation of cache.CompressionCodec using zstd.
//
// The zero-value is not usable. Use NewCodec to create a new instance. Codec is
// safe for concurrent use. Close should be called once the Codec is no longer
// needed to release the resources held by the encoder and decoder.
type Codec struct {
	encoder        *zstd.Encoder
	decoder        *zstd.Decoder
	maxDecodedSize uint64

	// Compress and Decompress hold a read lock, so Close waits for the calls in
	// flight before releasing the encoder and decoder.
	mu       sync.RWMutex
	closed   bool
	closeErr error
}

// Option customizes the behavior of Codec.
type Option func(*options)

type options struct {
	maxDecodedSize uint64
}

// WithMaxDecodedSize limits the size of decompressed values to n bytes. Values
// that would decompress to more than n bytes fail to decompress with an error
// wrapping cache.ErrMaxDecodedSizeExceeded, which protects against corrupt or
// malicious values exhausting memory.
//
// By default, there is no limit beyond the limits of the zstd decoder.
func WithMaxDecodedSize(n uint64) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

// NewCodec creates a new Codec with the specified compression level. It returns an
// error if the zstd encoder or decoder can't be created.
func NewCodec(level zstd.EncoderLevel, opts ...Option) (*Codec, error) {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, err
	}

	var decoderOpts []zstd.DOption
	if o.maxDecodedSize > 0 {
		// The decoder lowers the window to the max memory, and the encoder declares
		// a window of twice MinWindowSize for a value of MinWindowSize bytes, so
		// neither can be smaller than that. Smaller limits are enforced by
		// Decompress after decoding.
		limit := max(o.maxDecodedSize, 2*zstd.MinWindowSize)
		decoderOpts = append(decoderOpts,
			zstd.WithDecoderMaxMemory(limit),
			zstd.WithDecoderMaxWindow(min(limit, zstd.MaxWindowSize)))
	}
	decoder, err := zstd.NewReader(nil, decoderOpts...)
	if err != nil {
		_ = encoder.Close()
		return nil, err
	}

	return &Codec{
		encoder:        encoder,
		decoder:        decoder,
		maxDecodedSize: o.maxDecodedSize,
	}, nil
}

// Compress returns data compressed with zstd. It returns ErrClosed once Close has
// been called.
func (c *Codec) Compress(data []byte) ([]byte, error) {
	return c.AppendCompress(nil, data)
}

// AppendCompress appends data compressed with zstd to dst and returns the extended
// slice. It returns ErrClosed once Close has been called.
func (c *Codec) AppendCompress(dst, data []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	// EncodeAll only sizes the output for small values when dst is empty, so the
	// same is done here for a dst holding a prefix.
	if len(data) < 1<<20 {
		dst = slices.Grow(dst, len(data))
	}
	return c.encoder.EncodeAll(data, dst), nil
}

// Decompress returns the data decompressed from zstd compressed data. It returns
// ErrClosed once Close has been called.
func (c *Codec) Decompress(data []byte) ([]byte, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed {
		return nil, ErrClosed
	}
	out, err := c.decoder.DecodeAll(data, nil)
	if err != nil {
		// The decoder enforces the limit through its maximum memory and window
		// size.
		if c.maxDecodedSize > 0 &&
			(errors.Is(err, zstd.ErrDecoderSizeExceeded) || errors.Is(err, zstd.ErrWindowSizeExceeded)) {
			return nil, fmt.Errorf("%w: %w", limit.Exceeded(c.maxDecodedSize), err)
		}
		return nil, err
	}
	if c.maxDecodedSize > 0 && uint64(len(out)) > c.maxDecodedSize {
		return nil, fmt.Errorf("%w: %w", limit.Exceeded(c.maxDecodedSize), zstd.ErrDecoderSizeExceeded)
	}
	return out, nil
}

// Close releases the resources held by the encoder and decoder. Close waits for
// the Compress and Decompress calls in flight to return, after which they return
// ErrClosed. Calling Close more than once is a no-op.
func (c *Codec) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.closeErr = c.encoder.Close()
		c.decoder.Close()
	}
	return c.closeErr
}

// FormatID returns cache.CodecZstd.
func (c *Codec) FormatID() uint8 {
	return cache.CodecZstd
}
