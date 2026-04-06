package cacheotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

type InstrumentedCompressionCodec struct {
	codec cache.CompressionCodec
	dur   metric.Float64Histogram
	errs  metric.Int64Counter
}

func NewInstrumentedCompressionCodec(codec cache.CompressionCodec, opts ...CompressionOption) *InstrumentedCompressionCodec {
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
