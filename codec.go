package cache

// Codec is an interface type that defines the behavior for compressing and
// decompressing data.
type Codec interface {
	Flate(data []byte) ([]byte, error)
	Deflate(data []byte) ([]byte, error)
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
