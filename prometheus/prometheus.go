package prometheus

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.uber.org/multierr"

	cache "github.com/jkratz55/redis-cache"
)

var cacheLabels = []string{"operation"}

const (
	operationMarshal    = "marshal"
	operationUnmarshal  = "unmarshal"
	operationCompress   = "compress"
	operationDecompress = "decompress"
)

func InstrumentMetrics(c *cache.Cache, opts ...Option) error {

	conf := newConfig()
	for _, opt := range opts {
		opt(conf)
	}

	serializationTime := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "serialization_time",
		Help:        "Time in seconds to handle marshaling and unmarshalling operations",
		ConstLabels: conf.globalLabels,
		Buckets:     conf.buckets,
	}, cacheLabels)

	serializationErrs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "serialization_errs",
		Help:        "Count of errors during marshaling and unmarshalling operations",
		ConstLabels: conf.globalLabels,
	}, cacheLabels)

	compressionTime := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "compression_time",
		Help:        "Time in seconds to handle compression and decompression operations",
		ConstLabels: conf.globalLabels,
		Buckets:     conf.buckets,
	}, cacheLabels)

	compressionErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "compression_errs",
		Help:        "Count of errors during compression and decompression operations",
		ConstLabels: conf.globalLabels,
	}, cacheLabels)

	bytesIn := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "bytes_in",
		Help:        "Bytes received from Redis prior to decompression",
		ConstLabels: conf.globalLabels,
	})

	bytesOut := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "bytes_out",
		Help:        "Bytes transmitted to Redis after compression",
		ConstLabels: conf.globalLabels,
	})

	statusCollector := newRedisStatusCollector(conf, c)

	err := multierr.Combine(
		prometheus.Register(serializationTime),
		prometheus.Register(serializationErrs),
		prometheus.Register(compressionTime),
		prometheus.Register(compressionErrors),
		prometheus.Register(bytesIn),
		prometheus.Register(bytesOut),
		prometheus.Register(statusCollector))

	if err != nil {
		return err
	}

	c.AddHook(&metricsHook{
		serializationTime: serializationTime,
		serializationErrs: serializationErrs,
		compressionTime:   compressionTime,
		compressionErrors: compressionErrors,
		bytesIn:           bytesIn,
		bytesOut:          bytesOut,
	})

	return nil
}

type metricsHook struct {
	serializationTime *prometheus.HistogramVec
	serializationErrs *prometheus.CounterVec
	compressionTime   *prometheus.HistogramVec
	compressionErrors *prometheus.CounterVec
	bytesIn           prometheus.Counter
	bytesOut          prometheus.Counter
}

func (m *metricsHook) MarshalHook(next cache.Marshaller) cache.Marshaller {
	return func(v any) ([]byte, error) {
		start := time.Now()

		data, err := next(v)

		dur := time.Since(start).Seconds()

		m.serializationTime.WithLabelValues(operationMarshal).
			Observe(dur)

		if err != nil {
			m.serializationErrs.WithLabelValues(operationMarshal).Inc()
		}

		return data, err
	}
}

func (m *metricsHook) UnmarshallHook(next cache.Unmarshaller) cache.Unmarshaller {
	return func(b []byte, v any) error {
		start := time.Now()

		err := next(b, v)

		dur := time.Since(start).Seconds()

		m.serializationTime.WithLabelValues(operationUnmarshal).
			Observe(dur)

		if err != nil {
			m.serializationErrs.WithLabelValues(operationUnmarshal).Inc()
		}

		return err
	}
}

func (m *metricsHook) CompressHook(next cache.CompressionHook) cache.CompressionHook {
	return func(data []byte) ([]byte, error) {
		start := time.Now()

		compressed, err := next(data)

		dur := time.Since(start).Seconds()

		m.compressionTime.WithLabelValues(operationCompress).
			Observe(dur)

		m.bytesOut.Add(float64(len(compressed)))

		if err != nil {
			m.compressionErrors.WithLabelValues(operationCompress).Inc()
		}

		return compressed, err
	}
}

func (m *metricsHook) DecompressHook(next cache.CompressionHook) cache.CompressionHook {
	return func(data []byte) ([]byte, error) {
		start := time.Now()

		uncompressed, err := next(data)

		dur := time.Since(start).Seconds()

		m.compressionTime.WithLabelValues(operationDecompress).
			Observe(dur)

		m.bytesIn.Add(float64(len(data)))

		if err != nil {
			m.compressionErrors.WithLabelValues(operationDecompress).Inc()
		}

		return uncompressed, err
	}
}

func InstrumentClientMetrics(rdb redis.UniversalClient, opts ...Option) error {
	conf := newConfig()
	for _, opt := range opts {
		opt(conf)
	}

	processTime := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "process_time",
		Help:        "Duration in seconds to execute operations against Redis",
		ConstLabels: conf.globalLabels,
		Buckets:     conf.buckets,
	}, []string{"operation"})
	dialTime := prometheus.NewHistogram(prometheus.HistogramOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "dial_time",
		Help:        "Duration in seconds to create a new connection",
		ConstLabels: conf.globalLabels,
		Buckets:     conf.buckets,
	})
	hits := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "hits",
		Help:        "Number of times a key lookup existed in Redis",
		ConstLabels: conf.globalLabels,
	})
	misses := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "misses",
		Help:        "Number of times a key lookup did not exist in Redis",
		ConstLabels: conf.globalLabels,
	})
	errs := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "errors",
		Help:        "Number of errors returned by Redis",
		ConstLabels: conf.globalLabels,
	}, []string{"operation"})
	cancels := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace:   conf.namespace,
		Subsystem:   conf.subSystem,
		Name:        "canceled_ops",
		Help:        "Number of operations cancelled in flight",
		ConstLabels: conf.globalLabels,
	}, []string{"operation"})

	err := multierr.Combine(
		prometheus.Register(processTime),
		prometheus.Register(dialTime),
		prometheus.Register(hits),
		prometheus.Register(misses),
		prometheus.Register(errs),
		prometheus.Register(cancels))
	if err != nil {
		return fmt.Errorf("failed to register prometheus collectors: %w", err)
	}

	hook := &clientMetricsHook{
		processTime: processTime,
		dialTime:    dialTime,
		hits:        hits,
		misses:      misses,
		errors:      errs,
		cancels:     cancels,
	}
	rdb.AddHook(hook)

	poolReporter := newConnPoolStatsCollector(conf)
	prometheus.MustRegister(poolReporter)

	switch client := rdb.(type) {
	case *redis.Client:
		poolReporter.addPool(client)
		return nil
	case *redis.ClusterClient:
		client.OnNewNode(func(c *redis.Client) {
			poolReporter.addPool(c)
		})
		return nil
	case *redis.Ring:
		client.OnNewNode(func(c *redis.Client) {
			poolReporter.addPool(c)
		})
		return nil
	default:
		return fmt.Errorf("type %T is not supported", rdb)
	}
}

type clientMetricsHook struct {
	processTime *prometheus.HistogramVec
	dialTime    prometheus.Histogram
	hits        prometheus.Counter
	misses      prometheus.Counter
	errors      *prometheus.CounterVec
	cancels     *prometheus.CounterVec
}

func (c *clientMetricsHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()

		conn, err := next(ctx, network, addr)

		dur := time.Since(start).Seconds()

		c.dialTime.Observe(dur)

		if errors.Is(err, context.Canceled) {
			c.cancels.WithLabelValues("dial").Inc()
		}

		if !ignoreError(err) {
			c.errors.WithLabelValues("dial").Inc()
		}

		return conn, err
	}
}

func (c *clientMetricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := next(ctx, cmd)

		dur := time.Since(start).Seconds()

		c.processTime.WithLabelValues(cmd.Name()).Observe(dur)

		if strings.ToLower(cmd.Name()) == "get" {
			if strCmd, ok := cmd.(*redis.StringCmd); ok {
				if errors.Is(strCmd.Err(), redis.Nil) || (strCmd.Val() == "" && strCmd.Err() == nil) {
					c.misses.Inc()
				}
				if strCmd.Err() == nil {
					c.hits.Inc()
				}
			}
		}

		if errors.Is(err, context.Canceled) {
			c.cancels.WithLabelValues(cmd.Name()).Inc()
		}

		if !ignoreError(err) {
			c.errors.WithLabelValues(cmd.Name()).Inc()
		}

		return err
	}
}

func (c *clientMetricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()

		err := next(ctx, cmds)

		dur := time.Since(start).Seconds()

		c.processTime.WithLabelValues("pipeline").Observe(dur)

		if errors.Is(err, context.Canceled) {
			c.cancels.WithLabelValues("pipeline").Inc()
		}

		if !ignoreError(err) {
			c.errors.WithLabelValues("pipeline").Inc()
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
