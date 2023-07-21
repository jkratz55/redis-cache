package cache

import (
	"compress/flate"
	"fmt"

	"github.com/jkratz55/redis-cache/compression/brotli"
	iflate "github.com/jkratz55/redis-cache/compression/flate"
	"github.com/jkratz55/redis-cache/compression/gzip"
	"github.com/jkratz55/redis-cache/compression/lz4"
)

type CodecType uint8

const (
	None   CodecType = 0
	Flate  CodecType = 1
	GZip   CodecType = 2
	LZ4    CodecType = 3
	Brotli CodecType = 4
)

// Codec is an interface type that defines the behavior for compressing and
// decompressing data.
type Codec interface {
	Flate(data []byte) ([]byte, error)
	Deflate(data []byte) ([]byte, error)
}

// NewCodec creates a default codec for the given CodecType. By default, the codecs
// favor compression over performance where applicable.
//
// An invalid or unsupported CodecType will result in a panic.
func NewCodec(ct CodecType) Codec {
	switch ct {
	case None:
		return nopCodec{}
	case Flate:
		return iflate.Codec{
			Level: flate.BestCompression,
		}
	case GZip:
		return gzip.NewCodec(flate.BestCompression)
	case LZ4:
		return lz4.NewCodec()
	case Brotli:
		return brotli.NewCodec(6)
	default:
		panic(fmt.Errorf("invalid codec: CodecType %d is not a supported or known codec", ct))
	}
}

// nopCodec is a codec that is no-op, basically it does nothing. It is used as
// the default Codec when compression is not used.
type nopCodec struct{}

func (n nopCodec) Flate(data []byte) ([]byte, error) {
	return data, nil
}

func (n nopCodec) Deflate(data []byte) ([]byte, error) {
	return data, nil
}
