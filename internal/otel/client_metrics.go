package otel

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type ClientMetricsHook struct {
	attributes  []attribute.KeyValue
	processTime metric.Float64Histogram
	dialTime    metric.Float64Histogram
	hits        metric.Int64Counter
	misses      metric.Int64Counter
	errors      metric.Int64Counter
	cancels     metric.Int64Counter
}

func NewClientMetricsHook(meter metric.Meter, attrs []attribute.KeyValue) *ClientMetricsHook {

	processTime, err := meter.Float64Histogram("db.client.connections.use_time",
		metric.WithDescription("Time between borrowing a connection and returning it to the pool"),
		metric.WithUnit("s"))
	if err != nil {
		// todo: handle error
	}

	dialTime, err := meter.Float64Histogram("db.client.connection.create_time",
		metric.WithDescription("Time taken to create a new connection"),
		metric.WithUnit("s"))
	if err != nil {
		// todo: handle error
	}

	hits, err := meter.Int64Counter("cache.hits",
		metric.WithDescription("Count of cache hits"))
	if err != nil {
		// todo: handle error
	}

	misses, err := meter.Int64Counter("cache.misses",
		metric.WithDescription("Count of cache misses"))
	if err != nil {
		// todo: handle error
	}

	errCounter, err := meter.Int64Counter("db.client.errors",
		metric.WithDescription("Count of errors returned by the client"))
	if err != nil {
		// todo: handle error
	}

	cancelCounter, err := meter.Int64Counter("db.client.canceled_operations",
		metric.WithDescription("Count of instances where an operation was cancelled in flight by the caller"))
	if err != nil {
		// todo: handle error
	}

	return &ClientMetricsHook{
		attributes:  attrs,
		processTime: processTime,
		dialTime:    dialTime,
		hits:        hits,
		misses:      misses,
		errors:      errCounter,
		cancels:     cancelCounter,
	}
}

func (c *ClientMetricsHook) DialHook(next redis.DialHook) redis.DialHook {
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

func (c *ClientMetricsHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
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

func (c *ClientMetricsHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
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
