// Package all provides every built-in compression codec for decoding values
// written with any of them.
package all

import (
	compressflate "compress/flate"
	compressgzip "compress/gzip"

	kzstd "github.com/klauspost/compress/zstd"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/compression/flate"
	"github.com/jkratz55/redis-cache/v2/compression/gzip"
	"github.com/jkratz55/redis-cache/v2/compression/lz4"
	"github.com/jkratz55/redis-cache/v2/compression/s2"
	"github.com/jkratz55/redis-cache/v2/compression/snappy"
	"github.com/jkratz55/redis-cache/v2/compression/zstd"
)

// Option customizes the behavior of Codecs.
type Option func(*options)

type options struct {
	maxDecodedSize uint64
}

// WithMaxDecodedSize limits the size of decompressed values to n bytes for every
// codec. Values exceeding the limit fail with an error wrapping
// cache.ErrMaxDecodedSizeExceeded.
func WithMaxDecodedSize(n uint64) Option {
	return func(o *options) {
		o.maxDecodedSize = n
	}
}

// Codecs holds one instance of every built-in compression codec. The codecs are
// safe for concurrent use. Close releases the resources held by the zstd codec.
type Codecs struct {
	flate  *flate.Codec
	gzip   *gzip.Codec
	lz4    *lz4.Codec
	s2     *s2.Codec
	snappy *snappy.Codec
	zstd   *zstd.Codec
}

// NewCodecs creates one instance of each built-in compression codec. Options are
// applied to every codec. It returns an error if the zstd codec can't be created.
func NewCodecs(opts ...Option) (*Codecs, error) {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}

	zstdCodec, err := zstd.NewCodec(kzstd.SpeedDefault, zstd.WithMaxDecodedSize(o.maxDecodedSize))
	if err != nil {
		return nil, err
	}

	return &Codecs{
		flate:  flate.NewCodec(compressflate.DefaultCompression, flate.WithMaxDecodedSize(o.maxDecodedSize)),
		gzip:   gzip.NewCodec(compressgzip.DefaultCompression, gzip.WithMaxDecodedSize(o.maxDecodedSize)),
		lz4:    lz4.NewCodec(lz4.WithMaxDecodedSize(o.maxDecodedSize)),
		s2:     s2.NewCodec(s2.WithMaxDecodedSize(o.maxDecodedSize)),
		snappy: snappy.NewCodec(snappy.WithMaxDecodedSize(o.maxDecodedSize)),
		zstd:   zstdCodec,
	}, nil
}

// List returns the codecs in the order flate, gzip, lz4, s2, snappy, zstd.
// The returned slice can be used to wrap the codecs, for example with cacheotel.
func (c *Codecs) List() []cache.CompressionCodec {
	return []cache.CompressionCodec{c.flate, c.gzip, c.lz4, c.s2, c.snappy, c.zstd}
}

// DecodeCompression returns an option that registers all built-in codecs for
// reading. It does not change the codec used to write values; configure writes
// separately with cache.Compression.
func (c *Codecs) DecodeCompression() cache.Option {
	return cache.DecodeCompression(c.List()...)
}

// Close releases the resources held by the zstd codec.
func (c *Codecs) Close() error {
	return c.zstd.Close()
}
