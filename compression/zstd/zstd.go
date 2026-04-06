package zstd

import (
	"github.com/klauspost/compress/zstd"
)

// Codec is an implementation of cache.CompressionCodec using zstd.
//
// The zero-value is not usable. Use NewCodec to create a new instance.
type Codec struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

// NewCodec creates a new Codec with the specified compression level.
func NewCodec(level zstd.EncoderLevel) (*Codec, error) {
	encoder, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
	if err != nil {
		return nil, err
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
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
