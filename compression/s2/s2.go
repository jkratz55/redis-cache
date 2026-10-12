package s2

import (
	"github.com/klauspost/compress/s2"

	cache "github.com/jkratz55/redis-cache/v2"
)

type Codec struct{}

func NewCodec() *Codec {
	return &Codec{}
}

func (c *Codec) Compress(data []byte) ([]byte, error) {
	compressed := s2.Encode(nil, data)
	return compressed, nil
}

func (c *Codec) Decompress(data []byte) ([]byte, error) {
	return s2.Decode(nil, data)
}

// FormatID returns cache.CodecS2.
func (c *Codec) FormatID() uint8 {
	return cache.CodecS2
}
