package snappy

import (
	"github.com/klauspost/compress/snappy"
)

type Codec struct{}

func NewCodec() *Codec {
	return &Codec{}
}

func (c Codec) Flate(data []byte) ([]byte, error) {
	compressed := snappy.Encode(nil, data)
	return compressed, nil
}

func (c Codec) Deflate(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}
