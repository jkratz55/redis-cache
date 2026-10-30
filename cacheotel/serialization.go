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
	conf, dur, errs := newSerializationInstruments(opts)
	return newInstrumentedSerializer(serializer, conf, dur, errs)
}

// InstrumentSerializers wraps every serializer in serializers with an
// InstrumentedSerializer configured with opts, and returns them in the same order.
// The result can be passed to cache.DecodeSerializers:
//
//	cache.DecodeSerializers(cacheotel.InstrumentSerializers([]cache.Serializer{
//		&cache.MessagePackSerializer{}, cacheproto.ProtobufSerializer{},
//	})...)
//
// Registering instrumented MessagePack or JSON serializers replaces the built-in
// defaults used for reading, so their unmarshal operations are recorded too.
// The wrappers record the same metrics as the ones returned by
// NewInstrumentedSerializer. serializers isn't modified, and nil or empty
// serializers return an empty slice. Don't pass serializers that are already
// instrumented, otherwise their operations are recorded twice.
// InstrumentSerializers panics in the same cases as NewInstrumentedSerializer.
func InstrumentSerializers(serializers []cache.Serializer, opts ...SerializerOption) []cache.Serializer {
	conf, dur, errs := newSerializationInstruments(opts)
	wrapped := make([]cache.Serializer, len(serializers))
	for i, serializer := range serializers {
		wrapped[i] = newInstrumentedSerializer(serializer, conf, dur, errs)
	}
	return wrapped
}

// newSerializationInstruments applies opts and creates the instruments shared by
// the InstrumentedSerializer instances configured with them.
func newSerializationInstruments(opts []SerializerOption) (*commonConfig, metric.Float64Histogram, metric.Int64Counter) {
	conf := newCommonConfig()
	for _, opt := range opts {
		opt.applySerializer(conf)
	}

	meter := conf.meterProvider.Meter(scope)
	dur, _ := meter.Float64Histogram("cache.serialization.duration",
		metric.WithDescription("Duration of serialization operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(conf.durationBoundaries...))
	errs, _ := meter.Int64Counter("cache.serialization.errors",
		metric.WithDescription("Number of serialization errors"))
	return conf, dur, errs
}

// newInstrumentedSerializer validates serializer and wraps it. It panics if
// serializer is nil or doesn't have a format ID.
func newInstrumentedSerializer(serializer cache.Serializer, conf *commonConfig,
	dur metric.Float64Histogram, errs metric.Int64Counter) *InstrumentedSerializer {

	if isNil(serializer) {
		panic(fmt.Errorf("nil Serializer"))
	}
	id, ok := cache.FormatIDOf(serializer)
	if !ok {
		panic(fmt.Errorf("serializer %T has no format id: implement FormatID() uint8 returning an id in the range %d-255 or wrap it with cache.SerializerWithID",
			serializer, cache.MinCustomFormatID))
	}

	component := attribute.String("cache.serializer", serializerName(id))
	return &InstrumentedSerializer{
		serializer: serializer,
		id:         id,
		marshal:    newOperationMetrics(conf, dur, errs, component, "marshal"),
		unmarshal:  newOperationMetrics(conf, dur, errs, component, "unmarshal"),
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
