package otel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
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

func InstrumentClientMetrics(rdb redis.UniversalClient, opts ...MetricsOption) error {

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

	switch rdb := rdb.(type) {
	case *redis.Client:
		if conf.poolName == "" {
			opt := rdb.Options()
			conf.poolName = opt.Addr
		}
		conf.attrs = append(conf.attrs, attribute.String("pool.name", conf.poolName))

		if err := reportPoolStats(rdb, conf); err != nil {
			return err
		}
		if err := addClientMetricHook(rdb, conf); err != nil {
			return err
		}
		return nil
	case *redis.ClusterClient:
		rdb.OnNewNode(func(client *redis.Client) {
			if conf.poolName == "" {
				opt := client.Options()
				conf.poolName = opt.Addr
			}
			conf.attrs = append(conf.attrs, attribute.String("pool.name", conf.poolName))

			if err := reportPoolStats(client, conf); err != nil {
				otel.Handle(err)
			}
			if err := addClientMetricHook(client, conf); err != nil {
				otel.Handle(err)
			}
		})
		return nil
	case *redis.Ring:
		rdb.OnNewNode(func(client *redis.Client) {
			if conf.poolName == "" {
				opt := client.Options()
				conf.poolName = opt.Addr
			}
			conf.attrs = append(conf.attrs, attribute.String("pool.name", conf.poolName))

			if err := reportPoolStats(client, conf); err != nil {
				otel.Handle(err)
			}
			if err := addClientMetricHook(client, conf); err != nil {
				otel.Handle(err)
			}
		})
		return nil
	default:
		return fmt.Errorf("type %T is not supported", rdb)
	}
}

func addClientMetricHook(rdb redis.UniversalClient, conf *config) error {
	processTime, err := conf.meter.Float64Histogram("db.client.connections.use_time",
		metric.WithDescription("Time between borrowing a connection and returning it to the pool"),
		metric.WithUnit("s"))
	if err != nil {
		return err
	}

	dialTime, err := conf.meter.Float64Histogram("db.client.connection.create_time",
		metric.WithDescription("Time taken to create a new connection"),
		metric.WithUnit("s"))
	if err != nil {
		return err
	}

	hits, err := conf.meter.Int64Counter("db.client.cache.hits",
		metric.WithDescription("Count of cache hits"))
	if err != nil {
		return err
	}

	misses, err := conf.meter.Int64Counter("db.client.cache.misses",
		metric.WithDescription("Count of cache misses"))
	if err != nil {
		return err
	}

	errCounter, err := conf.meter.Int64Counter("db.client.errors",
		metric.WithDescription("Count of errors returned by the client"))
	if err != nil {
		return err
	}

	cancelCounter, err := conf.meter.Int64Counter("db.client.canceled_operations",
		metric.WithDescription("Count of instances where an operation was cancelled in flight by the caller"))
	if err != nil {
		return err
	}

	rdb.AddHook(&clientMetricsHook{
		attributes:  conf.attrs,
		processTime: processTime,
		dialTime:    dialTime,
		hits:        hits,
		misses:      misses,
		errors:      errCounter,
		cancels:     cancelCounter,
	})
	return nil
}

type clientMetricsHook struct {
	attributes  []attribute.KeyValue
	processTime metric.Float64Histogram
	dialTime    metric.Float64Histogram
	hits        metric.Int64Counter
	misses      metric.Int64Counter
	errors      metric.Int64Counter
	cancels     metric.Int64Counter
}

func (c *clientMetricsHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()

		conn, err := next(ctx, network, addr)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(c.attributes)+1)
		attrs = append(attrs, c.attributes...)

		c.dialTime.Record(ctx, dur, metric.WithAttributes(attrs...))

		if errors.Is(err, context.Canceled) {
			c.cancels.Add(ctx, 1, metric.WithAttributes(attrs...))
		}

		if !ignoreError(err) {
			c.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
		}

		return conn, err
	}
}

func (c *clientMetricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := next(ctx, cmd)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(c.attributes)+1)
		attrs = append(attrs, c.attributes...)

		processAttrs := append(attrs, attribute.String("command", cmd.Name()))

		c.processTime.Record(ctx, dur, metric.WithAttributes(processAttrs...))

		if strings.ToLower(cmd.Name()) == "get" {
			if strCmd, ok := cmd.(*redis.StringCmd); ok {
				if strCmd.Err() == redis.Nil {
					c.misses.Add(ctx, 1, metric.WithAttributes(attrs...))
				}
				if strCmd.Err() == nil {
					c.hits.Add(ctx, 1, metric.WithAttributes(attrs...))
				}
			}
		}

		if errors.Is(err, context.Canceled) {
			c.cancels.Add(ctx, 1, metric.WithAttributes(attrs...))
		}

		if !ignoreError(err) {
			c.errors.Add(ctx, 1, metric.WithAttributes(processAttrs...))
		}

		return err
	}
}

func (c *clientMetricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()

		err := next(ctx, cmds)

		dur := time.Since(start).Seconds()

		attrs := make([]attribute.KeyValue, 0, len(c.attributes)+2)
		attrs = append(attrs, c.attributes...)
		attrs = append(attrs, attribute.String("type", "pipeline"))

		c.processTime.Record(ctx, dur, metric.WithAttributes(attrs...))

		if errors.Is(err, context.Canceled) {
			c.cancels.Add(ctx, 1, metric.WithAttributes(attrs...))
		}

		if !ignoreError(err) {
			c.errors.Add(ctx, 1, metric.WithAttributes(attrs...))
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

func reportPoolStats(rdb *redis.Client, conf *config) error {
	labels := conf.attrs
	idleAttrs := append(labels, attribute.String("state", "idle"))
	usedAttrs := append(labels, attribute.String("state", "used"))

	idleMax, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.idle.max",
		metric.WithDescription("The maximum number of idle open connections allowed"),
	)
	if err != nil {
		return err
	}

	idleMin, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.idle.min",
		metric.WithDescription("The minimum number of idle open connections allowed"),
	)
	if err != nil {
		return err
	}

	connsMax, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.max",
		metric.WithDescription("The maximum number of open connections allowed"),
	)
	if err != nil {
		return err
	}

	usage, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.usage",
		metric.WithDescription("The number of connections that are currently in state described by the state attribute"),
	)
	if err != nil {
		return err
	}

	timeouts, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.timeouts",
		metric.WithDescription("The number of connection timeouts that have occurred trying to obtain a connection from the pool"),
	)
	if err != nil {
		return err
	}

	opts := rdb.Options()
	_, err = conf.meter.RegisterCallback(
		func(ctx context.Context, observer metric.Observer) error {
			stats := rdb.PoolStats()
			observer.ObserveInt64(idleMax, int64(opts.MaxIdleConns), metric.WithAttributes(labels...))
			observer.ObserveInt64(idleMin, int64(opts.MinIdleConns), metric.WithAttributes(labels...))
			observer.ObserveInt64(connsMax, int64(opts.PoolSize), metric.WithAttributes(labels...))

			observer.ObserveInt64(usage, int64(stats.IdleConns), metric.WithAttributes(idleAttrs...))
			observer.ObserveInt64(usage, int64(stats.TotalConns-stats.IdleConns), metric.WithAttributes(usedAttrs...))

			observer.ObserveInt64(timeouts, int64(stats.Timeouts), metric.WithAttributes(labels...))
			return nil
		},
		idleMax,
		idleMin,
		connsMax,
		usage,
		timeouts,
	)
	return err
}
