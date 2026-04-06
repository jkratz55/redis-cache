package cacheotel

import (
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

type InstrumentedCompressionCodec struct {
	codec cache.CompressionCodec
	dur   metric.Float64Histogram
	errs  metric.Int64Counter
}

func NewInstrumentedCompressionCodec(codec cache.CompressionCodec) *InstrumentedCompressionCodec {
	return &InstrumentedCompressionCodec{
		codec: codec,
	}
}

func (i *InstrumentedCompressionCodec) Compress(data []byte) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}

func (i *InstrumentedCompressionCodec) Decompress(data []byte) ([]byte, error) {
	// TODO implement me
	panic("implement me")
}
