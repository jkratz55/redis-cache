package cacheotel

import (
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

// InstrumentedCompressionCodec wraps a CompressionCodec and instruments it with OpenTelemetry metrics.
//
// The zero value for InstrumentedCompressionCodec is not usable. Use NewInstrumentedCompressionCodec
// to create a new instance.
type InstrumentedCompressionCodec struct {
	codec      cache.CompressionCodec
	id         uint8
	compress   operationMetrics
	decompress operationMetrics
}

// NewInstrumentedCompressionCodec returns a new InstrumentedCompressionCodec that wraps the given
// CompressionCodec.
//
// The InstrumentedCompressionCodec records the following metrics, with the attribute cache.codec set
// to the name of the wrapped CompressionCodec (flate, gzip, lz4, s2, snappy, zstd, or the format ID
// of a custom CompressionCodec) and the attribute operation set to compress or decompress:
//
//   - cache.compression.duration: histogram of the duration of operations in seconds, with the
//     attribute success.
//   - cache.compression.errors: counter of failed operations.
//
// The InstrumentedCompressionCodec reports the format ID of the wrapped CompressionCodec.
// If the wrapped CompressionCodec is nil, including a nil pointer, or doesn't have a format ID,
// NewInstrumentedCompressionCodec panics. Custom CompressionCodec implementations can be
// assigned a format ID by implementing cache.FormatIdentifier or by wrapping them with
// cache.CodecWithID.
func NewInstrumentedCompressionCodec(codec cache.CompressionCodec, opts ...CompressionOption) *InstrumentedCompressionCodec {
	if isNil(codec) {
		panic(fmt.Errorf("nil CompressionCodec"))
	}
	id, ok := cache.FormatIDOf(codec)
	if !ok {
		panic(fmt.Errorf("CompressionCodec %T has no format id: implement FormatID() uint8 returning an id in the range %d-255 or wrap it with cache.CodecWithID",
			codec, cache.MinCustomFormatID))
	}

	conf := newCommonConfig()
	for _, opt := range opts {
		opt.applyCompression(conf)
	}

	meter := conf.meterProvider.Meter(scope)
	dur, _ := meter.Float64Histogram("cache.compression.duration",
		metric.WithDescription("Duration of compression operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(conf.durationBoundaries...))
	errs, _ := meter.Int64Counter("cache.compression.errors",
		metric.WithDescription("Number of compression errors"))

	component := attribute.String("cache.codec", codecName(id))
	return &InstrumentedCompressionCodec{
		codec:      codec,
		id:         id,
		compress:   newOperationMetrics(conf, dur, errs, component, "compress"),
		decompress: newOperationMetrics(conf, dur, errs, component, "decompress"),
	}
}

// Compress calls Compress on the wrapped CompressionCodec and records it.
func (i *InstrumentedCompressionCodec) Compress(data []byte) ([]byte, error) {
	start := time.Now()
	res, err := i.codec.Compress(data)
	i.compress.record(start, err)
	return res, err
}

// AppendCompress calls AppendCompress on the wrapped CompressionCodec if it
// implements cache.CompressAppender, otherwise it appends the result of Compress to
// dst. It's recorded as a compress operation.
func (i *InstrumentedCompressionCodec) AppendCompress(dst, data []byte) ([]byte, error) {
	ca, ok := i.codec.(cache.CompressAppender)
	if !ok {
		res, err := i.Compress(data)
		if err != nil {
			return nil, err
		}
		return append(dst, res...), nil
	}
	start := time.Now()
	out, err := ca.AppendCompress(dst, data)
	i.compress.record(start, err)
	return out, err
}

// Decompress calls Decompress on the wrapped CompressionCodec and records it.
func (i *InstrumentedCompressionCodec) Decompress(data []byte) ([]byte, error) {
	start := time.Now()
	res, err := i.codec.Decompress(data)
	i.decompress.record(start, err)
	return res, err
}

// FormatID returns the format ID of the wrapped CompressionCodec since instrumentation
// doesn't alter the compressed bytes.
func (i *InstrumentedCompressionCodec) FormatID() uint8 {
	return i.id
}

// Unwrap returns the wrapped CompressionCodec, which defines the format of the
// values written by the InstrumentedCompressionCodec.
func (i *InstrumentedCompressionCodec) Unwrap() cache.CompressionCodec {
	return i.codec
}
