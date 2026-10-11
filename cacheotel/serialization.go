package cacheotel

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

// InstrumentedSerializer wraps a cache.Serializer and instruments it with OpenTelemetry metrics.
//
// The zero value for InstrumentedSerializer is not usable. Use NewInstrumentedSerializer
// to create a new instance.
type InstrumentedSerializer struct {
	serializer cache.Serializer
	id         uint8
	duration   metric.Float64Histogram
	errs       metric.Int64Counter
}

// NewInstrumentedSerializer returns a new InstrumentedSerializer that wraps the given Serializer.
//
// The InstrumentedSerializer reports the format ID of the wrapped Serializer. If the
// wrapped Serializer is nil or doesn't have a format ID, NewInstrumentedSerializer
// panics. Custom Serializer implementations can be assigned a format ID by
// implementing cache.FormatIdentifier or by wrapping them with cache.SerializerWithID.
func NewInstrumentedSerializer(serializer cache.Serializer, opts ...SerializerOption) *InstrumentedSerializer {
	if serializer == nil {
		panic(fmt.Errorf("nil Serializer"))
	}
	id, ok := cache.FormatIDOf(serializer)
	if !ok {
		panic(fmt.Errorf("Serializer %T has no format id: implement FormatID() uint8 returning an id in the range %d-255 or wrap it with cache.SerializerWithID",
			serializer, cache.MinCustomFormatID))
	}

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
		id:         id,
		duration:   duration,
		errs:       errs,
	}
}

func (i *InstrumentedSerializer) Marshal(v any) ([]byte, error) {
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

func (i *InstrumentedSerializer) Unmarshal(bytes []byte, v any) error {
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

// FormatID returns the format ID of the wrapped Serializer since instrumentation
// doesn't alter the encoded bytes.
func (i *InstrumentedSerializer) FormatID() uint8 {
	return i.id
}
