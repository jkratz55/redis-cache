package cache

// CompressionCodec is an interface type that defines the behavior for compressing and
// decompressing data.
type CompressionCodec interface {
	Compress(data []byte) ([]byte, error)
	Decompress(data []byte) ([]byte, error)
}

// nopCodec is a codec that is no-op; basically it does nothing. It is used as
// the default CompressionCodec when compression is not used.
type nopCodec struct{}

func (n nopCodec) Compress(data []byte) ([]byte, error) {
	return data, nil
}

func (n nopCodec) Decompress(data []byte) ([]byte, error) {
	return data, nil
}
