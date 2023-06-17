package otel

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/jkratz55/redis-cache"
)

type metricsHook struct {
	attributes          []attribute.KeyValue
	processTime         metric.Float64Histogram
	compressionTime     metric.Float64Histogram
	serializationTime   metric.Float64Histogram
	bytesRead           metric.Int64Counter
	bytesWritten        metric.Int64Counter
	hits                metric.Int64Counter
	misses              metric.Int64Counter
	clientErrors        metric.Int64Counter
	compressionErrors   metric.Int64Counter
	serializationErrors metric.Int64Counter
	cancels             metric.Int64Counter
}

func (m *metricsHook) ProcessHook(next cache.ProcessHook) cache.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := next(ctx, cmd)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attributes)+1)
		attrs = append(attrs, m.attributes...)

		processAttrs := append(attrs, attribute.String("command", cmd.Name()))

		m.processTime.Record(ctx, dur, metric.WithAttributes(processAttrs...))

		if strings.ToLower(cmd.Name()) == "get" {
			if strCmd, ok := cmd.(*redis.StringCmd); ok {
				if strCmd.Err() == redis.Nil {
					m.misses.Add(ctx, 1, metric.WithAttributes(attrs...))
				}
				if strCmd.Err() == nil {
					m.hits.Add(ctx, 1, metric.WithAttributes(attrs...))
				}
			}
		}

		if errors.Is(err, context.Canceled) {
			m.cancels.Add(ctx, 1)
		}

		if !ignoreError(err) {
			m.clientErrors.Add(ctx, 1, metric.WithAttributes(processAttrs...))
		}

		return err
	}
}

func (m *metricsHook) CompressHook(next cache.CompressHook) cache.CompressHook {
	return func(in []byte) ([]byte, error) {
		start := time.Now()

		data, err := next(in)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attributes)+1)
		attrs = append(attrs, m.attributes...)
		attrs = append(attrs, attribute.String("operation", "compress"))

		m.compressionTime.Record(context.TODO(), dur, metric.WithAttributes(attrs...))
		m.bytesWritten.Add(context.TODO(), int64(len(data)))

		if err != nil {
			m.compressionErrors.Add(context.TODO(), 1, metric.WithAttributes(attrs...))
		}

		return data, err
	}
}

func (m *metricsHook) DecompressHook(next cache.DecompressHook) cache.DecompressHook {
	return func(in []byte) ([]byte, error) {
		start := time.Now()

		data, err := next(in)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attributes)+1)
		attrs = append(attrs, m.attributes...)
		attrs = append(attrs, attribute.String("operation", "decompress"))

		m.compressionTime.Record(context.TODO(), dur, metric.WithAttributes(attrs...))
		m.bytesRead.Add(context.TODO(), int64(len(in)))

		if err != nil {
			m.compressionErrors.Add(context.TODO(), 1, metric.WithAttributes(attrs...))
		}

		return data, err
	}
}

func (m *metricsHook) MarshalHook(next cache.MarshalHook) cache.MarshalHook {
	return func(marshaller cache.Marshaller) ([]byte, error) {
		start := time.Now()

		data, err := next(marshaller)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attributes)+1)
		attrs = append(attrs, m.attributes...)
		attrs = append(attrs, attribute.String("operation", "marshal"))

		m.serializationTime.Record(context.TODO(), dur, metric.WithAttributes(attrs...))

		if err != nil {
			m.serializationErrors.Add(context.TODO(), 1, metric.WithAttributes(attrs...))
		}

		return data, err
	}
}

func (m *metricsHook) UnmarshalHook(next cache.UnmarshalHook) cache.UnmarshalHook {
	return func(um cache.Unmarshaller) error {
		start := time.Now()

		err := next(um)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attributes)+1)
		attrs = append(attrs, m.attributes...)
		attrs = append(attrs, attribute.String("operation", "unmarshall"))

		m.serializationTime.Record(context.TODO(), dur, metric.WithAttributes(attrs...))

		if err != nil {
			m.serializationErrors.Add(context.TODO(), 1, metric.WithAttributes(attrs...))
		}

		return err
	}
}

// ignoreError returns a boolean indicating if an error can be ignored for the
// purposes of metrics. Errors such as context.Canceled and Nil from Redis
// are not errors in the sense they are failures.
func ignoreError(err error) bool {
	if err == nil {
		return true
	}

	if errors.Is(err, context.Canceled) {
		return true
	}

	if errors.Is(err, redis.Nil) {
		return true
	}

	return false
}
