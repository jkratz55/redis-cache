package otel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache"
)

func InstrumentMetrics(c *cache.Cache, opts ...MetricsOption) error {
	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	conf := newConfig(baseOpts...)

	if conf.meter == nil {
		conf.meter = conf.meterProvider.Meter(
			name,
			metric.WithInstrumentationVersion("semver"+cache.Version()))
	}

	if err := addMetricHook(c, conf); err != nil {
		otel.Handle(err)
	}
	return nil
}

func addMetricHook(cache *cache.Cache, conf *config) error {
	serializationTime, err := conf.meter.Float64Histogram("redis.cache.serialization_time",
		metric.WithDescription("Time taken to marshal/unmarshal data to/from Redis"),
		metric.WithUnit("s"))
	if err != nil {
		return err
	}

	serializationErrors, err := conf.meter.Int64Counter("redis.cache.serialization_errors",
		metric.WithDescription("Count of errors during marshaling and unmarshalling operations"))
	if err != nil {
		return err
	}

	compressionTime, err := conf.meter.Float64Histogram("redis.cache.compression_time",
		metric.WithDescription("Time taken to compress/decompress data to/from Redis"),
		metric.WithUnit("s"))
	if err != nil {
		return err
	}

	compressionErrors, err := conf.meter.Int64Counter("redis.cache.compression_errors",
		metric.WithDescription("Count of error during compression/decompression operations"))
	if err != nil {
		return err
	}

	cache.AddHook(&metricsHook{
		attrs:               conf.atts,
		serializationTime:   serializationTime,
		serializationErrors: serializationErrors,
		compressionTime:     compressionTime,
		compressionErrors:   compressionErrors,
	})
	return nil
}

type metricsHook struct {
	attrs               []attribute.KeyValue
	serializationTime   metric.Float64Histogram
	serializationErrors metric.Int64Counter
	compressionTime     metric.Float64Histogram
	compressionErrors   metric.Int64Counter
}

func (m *metricsHook) MarshalHook(next cache.Marshaller) cache.Marshaller {
	return func(v any) ([]byte, error) {
		start := time.Now()

		data, err := next(v)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attrs)+1)
		attrs = append(attrs, m.attrs...)
		attrs = append(attrs, attribute.String("operation", "marshal"))

		m.serializationTime.Record(context.Background(), dur, metric.WithAttributes(attrs...))

		if err != nil {
			m.serializationErrors.Add(context.Background(), 1, metric.WithAttributes(attrs...))
		}

		return data, err
	}
}

func (m *metricsHook) UnmarshallHook(next cache.Unmarshaller) cache.Unmarshaller {
	return func(b []byte, v any) error {
		start := time.Now()

		err := next(b, v)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attrs)+1)
		attrs = append(attrs, m.attrs...)
		attrs = append(attrs, attribute.String("operation", "unmarshal"))

		m.serializationTime.Record(context.Background(), dur, metric.WithAttributes(attrs...))

		if err != nil {
			m.serializationErrors.Add(context.Background(), 1, metric.WithAttributes(attrs...))
		}

		return err
	}
}

func (m *metricsHook) CompressHook(next cache.CompressionHook) cache.CompressionHook {
	return func(data []byte) ([]byte, error) {
		start := time.Now()

		compressed, err := next(data)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attrs)+1)
		attrs = append(attrs, m.attrs...)
		attrs = append(attrs, attribute.String("operation", "compress"))

		m.compressionTime.Record(context.Background(), dur, metric.WithAttributes(attrs...))

		if err != nil {
			m.compressionErrors.Add(context.Background(), 1, metric.WithAttributes(attrs...))
		}

		return compressed, err
	}
}

func (m *metricsHook) DecompressHook(next cache.CompressionHook) cache.CompressionHook {
	return func(data []byte) ([]byte, error) {
		start := time.Now()

		decompressed, err := next(data)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(m.attrs)+1)
		attrs = append(attrs, m.attrs...)
		attrs = append(attrs, attribute.String("operation", "decompress"))

		m.compressionTime.Record(context.Background(), dur, metric.WithAttributes(attrs...))

		if err != nil {
			m.compressionErrors.Add(context.Background(), 1, metric.WithAttributes(attrs...))
		}

		return decompressed, err
	}
}
