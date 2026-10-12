package zstd

import (
	"sync"

	"github.com/klauspost/compress/zstd"

	cache "github.com/jkratz55/redis-cache/v2"
)

// Codec is an implementation of cache.CompressionCodec using zstd.
//
// The zero-value is not usable. Use NewCodec to create a new instance. Codec is
// safe for concurrent use. Close should be called once the Codec is no longer
// needed to release the resources held by the encoder and decoder.
type Codec struct {
	encoder   *zstd.Encoder
	decoder   *zstd.Decoder
	closeOnce sync.Once
	closeErr  error
}

// Option customizes the behavior of Codec.
type Option func(*options)

type options struct {
	maxDecodedSize uint64
}

// WithMaxDecodedSize limits the size of decompressed values to n bytes. Values
// that would decompress to more than n bytes fail to decompress, which protects
// against corrupt or malicious values exhausting memory.
//
// By default, there is no limit beyond the limits of the zstd decoder.
func WithMaxDecodedSize(n uint64) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

// NewCodec creates a new Codec with the specified compression level.
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
		// The window can't be larger than the decoded size, but the decoder
		// requires a window of at least MinWindowSize.
		window := max(min(o.maxDecodedSize, zstd.MaxWindowSize), zstd.MinWindowSize)
		decoderOpts = append(decoderOpts,
			zstd.WithDecoderMaxMemory(o.maxDecodedSize),
			zstd.WithDecoderMaxWindow(window))
	}
	decoder, err := zstd.NewReader(nil, decoderOpts...)
	if err != nil {
		_ = encoder.Close()
		return nil, err
	}

	return &Codec{
		encoder: encoder,
		decoder: decoder,
	}, nil
}

func (c *Codec) Compress(data []byte) ([]byte, error) {
	compressed := c.encoder.EncodeAll(data, nil)
	return compressed, nil
}

func (c *Codec) Decompress(data []byte) ([]byte, error) {
	return c.decoder.DecodeAll(data, nil)
}

// Close releases the resources held by the encoder and decoder. The Codec must
// not be used after Close is called. Calling Close more than once is a no-op.
func (c *Codec) Close() error {
	c.closeOnce.Do(func() {
		c.closeErr = c.encoder.Close()
		c.decoder.Close()
	})
	return c.closeErr
}

// FormatID returns cache.CodecZstd.
func (c *Codec) FormatID() uint8 {
	return cache.CodecZstd
}
