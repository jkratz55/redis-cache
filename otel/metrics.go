package otel

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidishook"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

func InstrumentClient(c rueidis.Client, opts ...Option) (rueidis.Client, error) {
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

	hook, err := newInstrumentingHook(conf)
	if err != nil {
		return nil, err
	}
	return rueidishook.WithHook(c, hook), nil
}

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
		return fmt.Errorf("add metric hook: %w", err)
	}

	return nil
}

func addMetricHook(cache *cache.Cache, conf *config) error {
	serializationTime, err := conf.meter.Float64Histogram("redis.cache.serialization_time_seconds",
		metric.WithDescription("Duration of time in seconds to marshal/unmarshal data"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(ExponentialBuckets(0.001, 2, 5)...))
	if err != nil {
		return err
	}

	serializationErrors, err := conf.meter.Int64Counter("redis.cache.serialization_errors_total",
		metric.WithDescription("Count of errors during marshaling and unmarshalling operations"),
		metric.WithUnit("count"))
	if err != nil {
		return err
	}

	compressionTime, err := conf.meter.Float64Histogram("redis.cache.compression_time_seconds",
		metric.WithDescription("Duration of time in seconds to compress/decompress data"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(ExponentialBuckets(0.001, 2, 5)...))
	if err != nil {
		return err
	}

	compressionErrors, err := conf.meter.Int64Counter("redis.cache.compression_errors_total",
		metric.WithDescription("Count of error during compression/decompression operations"),
		metric.WithUnit("count"))
	if err != nil {
		return err
	}

	cache.AddHook(&metricsHook{
		attrs:               conf.attrs,
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

type instrumentingHook struct {
	attrs       []attribute.KeyValue
	cmdDuration metric.Float64Histogram
	cmdErrors   metric.Int64Counter
}

func newInstrumentingHook(conf *config) (*instrumentingHook, error) {
	cmdDuration, err := conf.meter.Float64Histogram("redis.command.duration_seconds",
		metric.WithDescription("Duration of time in seconds to execute a command"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(conf.cmdBoundaries...))
	if err != nil {
		return nil, err
	}

	cmdErrors, err := conf.meter.Int64Counter("redis.command.errors_total",
		metric.WithDescription("Count of errors during command execution"),
		metric.WithUnit("count"))
	if err != nil {
		return nil, err
	}

	return &instrumentingHook{
		attrs:       conf.attrs,
		cmdDuration: cmdDuration,
		cmdErrors:   cmdErrors,
	}, nil
}

func (i *instrumentingHook) Do(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) (resp rueidis.RedisResult) {
	cmdName := cmd.Commands()[0]

	start := time.Now()
	resp = client.Do(ctx, cmd)
	dur := time.Since(start)

	attrs := make([]attribute.KeyValue, len(i.attrs)+1)
	copy(attrs, i.attrs)
	attrs[len(attrs)-1] = attribute.String("command", cmdName)
	i.cmdDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))

	if resp.Error() != nil {
		i.cmdErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	return resp
}

func (i *instrumentingHook) DoMulti(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) (resps []rueidis.RedisResult) {
	start := time.Now()
	resps = client.DoMulti(ctx, multi...)
	dur := time.Since(start)

	attrs := make([]attribute.KeyValue, len(i.attrs)+1)
	copy(attrs, attrs)
	attrs[len(attrs)-1] = attribute.String("command", "pipeline")
	i.cmdDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))

	go func() {
		errMap := make(map[string]int)
		for i := 0; i < len(resps); i++ {
			if resps[i].Error() != nil {
				errMap[multi[i].Commands()[0]]++
			}
		}

		for cmd, count := range errMap {
			attrs := make([]attribute.KeyValue, len(i.attrs)+1)
			copy(attrs, i.attrs)
			attrs[len(i.attrs)] = attribute.String("command", cmd)
			i.cmdErrors.Add(context.Background(), int64(count), metric.WithAttributes(attrs...))
		}
	}()

	return resps
}

func (i *instrumentingHook) DoCache(client rueidis.Client, ctx context.Context, cmd rueidis.Cacheable, ttl time.Duration) (resp rueidis.RedisResult) {
	start := time.Now()
	resp = client.DoCache(ctx, cmd, ttl)
	dur := time.Since(start)

	attrs := make([]attribute.KeyValue, len(i.attrs)+2)
	copy(attrs, i.attrs)
	attrs[len(attrs)-2] = attribute.String("command", cmd.Commands()[0])
	attrs[len(attrs)-1] = attribute.String("cacheHit", strconv.FormatBool(resp.IsCacheHit()))
	i.cmdDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))

	if resp.Error() != nil {
		i.cmdErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	return resp
}

func (i *instrumentingHook) DoMultiCache(client rueidis.Client, ctx context.Context, multi ...rueidis.CacheableTTL) (resps []rueidis.RedisResult) {
	start := time.Now()
	resps = client.DoMultiCache(ctx, multi...)
	dur := time.Since(start)

	attrs := make([]attribute.KeyValue, len(i.attrs)+1)
	copy(attrs, i.attrs)
	attrs[len(attrs)-1] = attribute.String("command", "pipeline")
	i.cmdDuration.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))

	go func() {
		errMap := make(map[string]int)
		for i := 0; i < len(resps); i++ {
			if resps[i].Error() != nil {
				errMap[multi[i].Cmd.Commands()[0]]++
			}
			// todo: capture cache hit/miss?
		}

		for cmd, count := range errMap {
			attrs := make([]attribute.KeyValue, len(i.attrs)+1)
			copy(attrs, i.attrs)
			attrs[len(i.attrs)] = attribute.String("command", cmd)
			i.cmdErrors.Add(context.Background(), int64(count), metric.WithAttributes(attrs...))
		}
	}()

	return resps
}

func (i *instrumentingHook) Receive(client rueidis.Client, ctx context.Context, subscribe rueidis.Completed, fn func(msg rueidis.PubSubMessage)) (err error) {
	return client.Receive(ctx, subscribe, fn)
}

func (i *instrumentingHook) DoStream(client rueidis.Client, ctx context.Context, cmd rueidis.Completed) rueidis.RedisResultStream {
	return client.DoStream(ctx, cmd)
}

func (i *instrumentingHook) DoMultiStream(client rueidis.Client, ctx context.Context, multi ...rueidis.Completed) rueidis.MultiRedisResultStream {
	return client.DoMultiStream(ctx, multi...)
}
