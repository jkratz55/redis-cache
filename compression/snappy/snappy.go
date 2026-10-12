package snappy

import (
	"github.com/klauspost/compress/snappy"

	cache "github.com/jkratz55/redis-cache/v2"
)

type Codec struct{}

func NewCodec() *Codec {
	return &Codec{}
}

func (c *Codec) Compress(data []byte) ([]byte, error) {
	compressed := snappy.Encode(nil, data)
	return compressed, nil
}

func (c *Codec) Decompress(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

// FormatID returns cache.CodecSnappy.
func (c *Codec) FormatID() uint8 {
	return cache.CodecSnappy
}
