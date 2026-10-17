package cacheotel

import (
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
	marshal    operationMetrics
	unmarshal  operationMetrics
}

// NewInstrumentedSerializer returns a new InstrumentedSerializer that wraps the given Serializer.
//
// The InstrumentedSerializer records the following metrics, with the attribute cache.serializer set
// to the name of the wrapped Serializer (msgpack, json, protobuf, or the format ID of a custom
// Serializer) and the attribute operation set to marshal or unmarshal:
//
//   - cache.serialization.duration: histogram of the duration of operations in seconds, with the
//     attribute success.
//   - cache.serialization.errors: counter of failed operations.
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

	component := attribute.String("cache.serializer", serializerName(id))
	return &InstrumentedSerializer{
		serializer: serializer,
		id:         id,
		marshal:    newOperationMetrics(conf, duration, errs, component, "marshal"),
		unmarshal:  newOperationMetrics(conf, duration, errs, component, "unmarshal"),
	}
}

// Marshal calls Marshal on the wrapped Serializer and records it.
func (i *InstrumentedSerializer) Marshal(v any) ([]byte, error) {
	start := time.Now()
	data, err := i.serializer.Marshal(v)
	i.marshal.record(start, err)
	return data, err
}

// AppendMarshal calls AppendMarshal on the wrapped Serializer if it implements
// cache.MarshalAppender, otherwise it appends the result of Marshal to dst. It's
// recorded as a marshal operation.
func (i *InstrumentedSerializer) AppendMarshal(dst []byte, v any) ([]byte, error) {
	ma, ok := i.serializer.(cache.MarshalAppender)
	if !ok {
		data, err := i.Marshal(v)
		if err != nil {
			return nil, err
		}
		return append(dst, data...), nil
	}
	start := time.Now()
	out, err := ma.AppendMarshal(dst, v)
	i.marshal.record(start, err)
	return out, err
}

// Unmarshal calls Unmarshal on the wrapped Serializer and records it.
func (i *InstrumentedSerializer) Unmarshal(bytes []byte, v any) error {
	start := time.Now()
	err := i.serializer.Unmarshal(bytes, v)
	i.unmarshal.record(start, err)
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
	i.unmarshal.record(start, err)
	return err
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
