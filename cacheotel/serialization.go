package cacheotel

import (
	"context"
	"fmt"
	"reflect"
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
// wrapped Serializer is nil, including a nil pointer, or doesn't have a format ID, NewInstrumentedSerializer
// panics. Custom Serializer implementations can be assigned a format ID by
// implementing cache.FormatIdentifier or by wrapping them with cache.SerializerWithID.
func NewInstrumentedSerializer(serializer cache.Serializer, opts ...SerializerOption) *InstrumentedSerializer {
	if isNil(serializer) {
		panic(fmt.Errorf("nil Serializer"))
	}
	id, ok := cache.FormatIDOf(serializer)
	if !ok {
		panic(fmt.Errorf("serializer %T has no format id: implement FormatID() uint8 returning an id in the range %d-255 or wrap it with cache.SerializerWithID",
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
	i.recordUnmarshal(start, err)
	return err
}

// UnmarshalStrict calls UnmarshalStrict on the wrapped Serializer if it implements
// cache.StrictUnmarshaler, otherwise Unmarshal. It's recorded as an unmarshal
// operation.
func (i *InstrumentedSerializer) UnmarshalStrict(bytes []byte, v any) error {
	start := time.Now()
	var err error
	if su, ok := i.serializer.(cache.StrictUnmarshaler); ok {
		err = su.UnmarshalStrict(bytes, v)
	} else {
		err = i.serializer.Unmarshal(bytes, v)
	}
	i.recordUnmarshal(start, err)
	return err
}

func (i *InstrumentedSerializer) recordUnmarshal(start time.Time, err error) {
	ctx := context.Background()
	i.duration.Record(ctx, time.Since(start).Seconds(),
		metric.WithAttributes(
			attribute.String("operation", "unmarshal"),
			attribute.Bool("success", err == nil)))
	if err != nil {
		i.errs.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "unmarshal")))
	}
}

// FormatID returns the format ID of the wrapped Serializer since instrumentation
// doesn't alter the encoded bytes.
func (i *InstrumentedSerializer) FormatID() uint8 {
	return i.id
}

// Unwrap returns the wrapped Serializer, which defines the format of the values
// written by the InstrumentedSerializer.
func (i *InstrumentedSerializer) Unwrap() cache.Serializer {
	return i.serializer
}

// isNil reports whether v is nil, including interfaces holding a typed nil
// pointer, map, slice, func or channel.
func isNil(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
		return rv.IsNil()
	default:
		return false
	}
}
