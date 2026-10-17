// Package cacheotel instruments Cache and TypedCache with OpenTelemetry metrics.
//
// NewInstrumentedSerializer and NewInstrumentedCompressionCodec wrap a Serializer
// and a CompressionCodec to record how long serialization and compression take.
// NewHooks records cache hits and misses, the size of cached values, decode errors
// and single-flight loads, and ObserveFailoverClient reports which client of a
// FailoverClient is active. Metrics and tracing for the Redis commands themselves
// come from instrumenting the Redis client, for example with redisotel-native for
// metrics and redisotel for tracing.
package cacheotel

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

const (
	scope = "github.com/jkratz55/redis-cache/v2/cacheotel"
)

// operationMetrics records the duration and errors of one operation, such as
// marshal or compress. The attribute sets are computed once, so recording doesn't
// allocate.
type operationMetrics struct {
	dur       metric.Float64Histogram
	errs      metric.Int64Counter
	succeeded metric.MeasurementOption
	failed    metric.MeasurementOption
	errAttrs  metric.MeasurementOption
}

func newOperationMetrics(conf *commonConfig, dur metric.Float64Histogram, errs metric.Int64Counter,
	component attribute.KeyValue, op string) operationMetrics {

	opAttr := attribute.String("operation", op)
	return operationMetrics{
		dur:       dur,
		errs:      errs,
		succeeded: conf.attributeSet(component, opAttr, attribute.Bool("success", true)),
		failed:    conf.attributeSet(component, opAttr, attribute.Bool("success", false)),
		errAttrs:  conf.attributeSet(component, opAttr),
	}
}

// record records an operation that started at start and returned err.
//
// Serializer and CompressionCodec methods don't take a context, so measurements
// are recorded with context.Background() and aren't associated with a span. The
// metrics recorded by NewHooks are.
func (m *operationMetrics) record(start time.Time, err error) {
	ctx := context.Background()
	if err != nil {
		m.dur.Record(ctx, time.Since(start).Seconds(), m.failed)
		m.errs.Add(ctx, 1, m.errAttrs)
		return
	}
	m.dur.Record(ctx, time.Since(start).Seconds(), m.succeeded)
}

// serializerName returns the name of the serializer with the format ID id, which
// is recorded as the cache.serializer attribute. Custom serializers are named by
// their ID.
func serializerName(id uint8) string {
	switch id {
	case cache.SerializerMsgPack:
		return "msgpack"
	case cache.SerializerJSON:
		return "json"
	case cache.SerializerProtobuf:
		return "protobuf"
	default:
		return strconv.Itoa(int(id))
	}
}

// codecName returns the name of the compression codec with the format ID id, which
// is recorded as the cache.codec attribute. Custom codecs are named by their ID.
func codecName(id uint8) string {
	switch id {
	case cache.CodecNone:
		return "none"
	case cache.CodecFlate:
		return "flate"
	case cache.CodecGzip:
		return "gzip"
	case cache.CodecLZ4:
		return "lz4"
	case cache.CodecS2:
		return "s2"
	case cache.CodecSnappy:
		return "snappy"
	case cache.CodecZstd:
		return "zstd"
	default:
		return strconv.Itoa(int(id))
	}
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
