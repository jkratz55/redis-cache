package cacheotel

import (
	"context"
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
	codec cache.CompressionCodec
	id    uint8
	dur   metric.Float64Histogram
	errs  metric.Int64Counter
}

// NewInstrumentedCompressionCodec returns a new InstrumentedCompressionCodec that wraps the given
// CompressionCodec.
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

	return &InstrumentedCompressionCodec{
		codec: codec,
		id:    id,
		dur:   dur,
		errs:  errs,
	}
}

func (i *InstrumentedCompressionCodec) Compress(data []byte) ([]byte, error) {
	start := time.Now()
	res, err := i.codec.Compress(data)
	ctx := context.Background()
	i.dur.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "compress"),
			attribute.Bool("success", err == nil)))
	if err != nil {
		i.errs.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "compress")))
	}
	return res, err
}

func (i *InstrumentedCompressionCodec) Decompress(data []byte) ([]byte, error) {
	start := time.Now()
	res, err := i.codec.Decompress(data)
	ctx := context.Background()
	i.dur.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "decompress"),
			attribute.Bool("success", err == nil)))
	if err != nil {
		i.errs.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "decompress")))
	}
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
