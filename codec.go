package cache

import (
	"errors"
)

// ErrMaxDecodedSizeExceeded is returned by the built-in CompressionCodec
// implementations when a value would decompress to more bytes than the limit
// configured with their WithMaxDecodedSize option.
var ErrMaxDecodedSizeExceeded = errors.New("max decoded size exceeded")

// CompressionCodec is an interface type that defines the behavior for compressing and
// decompressing data.
//
// Implementations must be safe for concurrent use and must have a format ID, which
// is stored alongside every cached value, either by implementing FormatIdentifier
// or by being wrapped with CodecWithID.
//
// Cached values are read from a Redis that may be shared with other services, so a
// value can be corrupt or crafted to decompress to a very large size. Implementations
// should allow the size of decompressed values to be limited, as the built-in
// implementations do with their WithMaxDecodedSize option.
type CompressionCodec interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// CompressAppender is an optional interface implemented by CompressionCodecs that
// can append the compressed data to a byte slice. Cache and TypedCache use it to
// compress values directly after the format header, which saves an allocation and
// a copy of the value on every write. All built-in CompressionCodecs implement it.
//
// AppendCompress must append the same bytes Compress returns to dst and return the
// extended slice. It must not modify dst[:len(dst)] or retain dst or data.
type CompressAppender interface {
	AppendCompress(dst, data []byte) ([]byte, error)
}

// appendCompress appends data compressed by c to dst, using AppendCompress if c
// implements CompressAppender.
func appendCompress(c CompressionCodec, dst, data []byte) ([]byte, error) {
	if ca, ok := c.(CompressAppender); ok {
		return ca.AppendCompress(dst, data)
	}
	compressed, err := c.Compress(data)
	if err != nil {
		return nil, err
	}
	return append(dst, compressed...), nil
}

// nopCodec is a codec that is no-op; basically it does nothing. It is used as
// the default CompressionCodec when compression is not used.
type nopCodec struct{}

func (n nopCodec) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n nopCodec) AppendCompress(dst, data []byte) ([]byte, error) {
	return append(dst, data...), nil
}

func (n nopCodec) Decompress(data []byte) ([]byte, error) {
	return data, nil
}

func (n nopCodec) FormatID() uint8 {
	return CodecNone
}
