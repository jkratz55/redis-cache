package otel

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/metric"

	"github.com/jkratz55/redis-cache"
)

type metricsHook struct {
	processTime         metric.Float64Histogram
	compressionTime     metric.Float64Histogram
	serializationTime   metric.Float64Histogram
	hits                metric.Int64Counter
	misses              metric.Int64Counter
	clientErrors        metric.Int64Counter
	compressionErrors   metric.Int64Counter
	serializationErrors metric.Int64Counter
	cancels             metric.Int64Counter
}

func (m metricsHook) ProcessHook(next cache.ProcessHook) cache.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := next(ctx, cmd)

		m.processTime.Record(ctx, time.Since(start).Seconds(), metric.WithAttributes())

		if strings.ToLower(cmd.Name()) == "get" {
			if strCmd, ok := cmd.(*redis.StringCmd); ok {
				if strCmd.Err() == redis.Nil {
					m.misses.Add(ctx, 1)
				}
				if strCmd.Err() == nil {
					m.hits.Add(ctx, 1)
				}
				if strCmd.Err() != nil && strCmd.Err() == redis.Nil {
					m.clientErrors.Add(ctx, 1)
				}
			}
		}

		if errors.Is(err, context.Canceled) {
			m.cancels.Add(ctx, 1)
		}

		return err
	}
}

func (m metricsHook) CompressHook(next cache.CompressHook) cache.CompressHook {
	// TODO implement me
	panic("implement me")
}

func (m metricsHook) DecompressHook(next cache.DecompressHook) cache.DecompressHook {
	// TODO implement me
	panic("implement me")
}

func (m metricsHook) MarshalHook(next cache.MarshalHook) cache.MarshalHook {
	return func(m cache.Marshaller) ([]byte, error) {
		start := time.Now()

		data, err := next(m)

	}
}

func (m metricsHook) UnmarshalHook(next cache.UnmarshalHook) cache.UnmarshalHook {
	// TODO implement me
	panic("implement me")
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
