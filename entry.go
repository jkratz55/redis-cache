package cache

type CompressionIdentifier interface {
	comparable
}

type EncodingIdentifier interface {
	comparable
}

type compressionCodec string

const (
	gzip compressionCodec = "gzip"
	lz4  compressionCodec = "lz4"
	snappy compressionCodec = "snappy"
	s2 compressionCodec = "s2"
)

type Entry[C CompressionIdentifier, E EncodingIdentifier] struct {
	CompressionCodec CompressionIdentifier,
	EncodingCodec EncodingIdentifier,
	Data             []byte
}

func testSomething() {
	e := Entry{
		CompressionCodec: gzip,
		EncodingCodec:    "msgpack",
		Data:             []byte("hello world"),
	}
}
