package cacheotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

type InstrumentedSerializer struct {
	serializer cache.Serializer
	duration   metric.Float64Histogram
	errs       metric.Int64Counter
}

func NewInstrumentedSerializer(serializer cache.Serializer, opts ...SerializerOption) *InstrumentedSerializer {
	conf := newCommonConfig()
	for _, opt := range opts {
		opt.applySerializer(conf)
	}

	meter := conf.meterProvider.Meter(scope)
	duration, _ := meter.Float64Histogram("cache.serialization.duration",
		metric.WithDescription("Duration of serialization operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(conf.durationBoundaries...))
	errs, _ := meter.Int64Counter("cache.serialization.errors",
		metric.WithDescription("Number of serialization errors"))

	return &InstrumentedSerializer{
		serializer: serializer,
		duration:   duration,
		errs:       errs,
	}
}

func (i *InstrumentedSerializer) Marshal(v interface{}) ([]byte, error) {
	start := time.Now()
	data, err := i.serializer.Marshal(v)
	ctx := context.Background()
	i.duration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "marshal"),
			attribute.Bool("success", err == nil)))
	if err != nil {
		i.errs.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "marshal")))
	}
	return data, err
}

func (i *InstrumentedSerializer) Unmarshal(bytes []byte, v interface{}) error {
	start := time.Now()
	err := i.serializer.Unmarshal(bytes, v)
	ctx := context.Background()
	i.duration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "unmarshal"),
			attribute.Bool("success", err == nil)))
	if err != nil {
		i.errs.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "unmarshal")))
	}
	return err
}
