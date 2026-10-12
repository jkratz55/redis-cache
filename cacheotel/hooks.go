package cacheotel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

// NewHooks returns cache.Hooks recording OpenTelemetry metrics for the operations of a
// cache.Cache or cache.TypedCache:
//
//   - cache.lookups: counter of keys read from Redis, with the attribute cache.hit set to true
//     for keys that were found and false for keys that weren't.
//   - cache.value.size: histogram of the size in bytes of values as stored in Redis, compressed
//     and including the format header, with the attribute operation set to encode or decode.
//   - cache.value.serialized_size: histogram of the size in bytes of values after serialization
//     and before compression, recorded when values are encoded.
//   - cache.decode.errors: counter of values read from Redis that couldn't be decoded, with the
//     attribute reason set to unsupported_format if the value was written with a Serializer or
//     CompressionCodec that isn't registered, size_limit if a CompressionCodec rejected it for
//     exceeding its size limit, or invalid otherwise, for example for corrupt values or values
//     that don't match the destination type.
//   - cache.singleflight.calls: counter of calls to cache.ReadThroughSingleFlight after a cache
//     miss, with the attribute shared set to true if the call joined a load already in flight
//     and false if it started one.
//
// Measurements are recorded with the context of the cache operation, so they can be associated
// with the active span, for example as exemplars. Configure the Hooks with the
// cache.Instrumentation option:
//
//	c := cache.New(client,
//		cache.Instrumentation(cacheotel.NewHooks(
//			cacheotel.WithAttributes(attribute.String("cache.name", "users")))))
func NewHooks(opts ...InstrumentOption) cache.Hooks {
	conf := newCommonConfig()
	for _, opt := range opts {
		opt.apply(conf)
	}

	meter := conf.meterProvider.Meter(scope)
	lookups, _ := meter.Int64Counter("cache.lookups",
		metric.WithDescription("Number of keys read from Redis"),
		metric.WithUnit("{key}"))
	size, _ := meter.Int64Histogram("cache.value.size",
		metric.WithDescription("Size of values as stored in Redis"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(conf.sizeBoundaries...))
	serializedSize, _ := meter.Int64Histogram("cache.value.serialized_size",
		metric.WithDescription("Size of values after serialization and before compression"),
		metric.WithUnit("By"),
		metric.WithExplicitBucketBoundaries(conf.sizeBoundaries...))
	decodeErrs, _ := meter.Int64Counter("cache.decode.errors",
		metric.WithDescription("Number of values that couldn't be decoded"))
	singleFlight, _ := meter.Int64Counter("cache.singleflight.calls",
		metric.WithDescription("Number of ReadThroughSingleFlight calls after a cache miss"))

	var (
		hit             = conf.attributeSet(attribute.Bool("cache.hit", true))
		miss            = conf.attributeSet(attribute.Bool("cache.hit", false))
		encode          = conf.attributeSet(attribute.String("operation", "encode"))
		decode          = conf.attributeSet(attribute.String("operation", "decode"))
		base            = conf.attributeSet()
		unsupported     = conf.attributeSet(attribute.String("reason", "unsupported_format"))
		sizeLimit       = conf.attributeSet(attribute.String("reason", "size_limit"))
		invalid         = conf.attributeSet(attribute.String("reason", "invalid"))
		sharedFlight    = conf.attributeSet(attribute.Bool("shared", true))
		notSharedFlight = conf.attributeSet(attribute.Bool("shared", false))
	)

	return cache.Hooks{
		OnLookup: func(ctx context.Context, hits, misses int) {
			if hits > 0 {
				lookups.Add(ctx, int64(hits), hit)
			}
			if misses > 0 {
				lookups.Add(ctx, int64(misses), miss)
			}
		},
		OnEncode: func(ctx context.Context, serialized, stored int) {
			serializedSize.Record(ctx, int64(serialized), base)
			size.Record(ctx, int64(stored), encode)
		},
		OnDecode: func(ctx context.Context, stored int, err error) {
			size.Record(ctx, int64(stored), decode)
			switch {
			case err == nil:
			case errors.Is(err, cache.ErrUnsupportedFormat):
				decodeErrs.Add(ctx, 1, unsupported)
			case errors.Is(err, cache.ErrMaxDecodedSizeExceeded):
				decodeErrs.Add(ctx, 1, sizeLimit)
			default:
				decodeErrs.Add(ctx, 1, invalid)
			}
		},
		OnSingleFlight: func(ctx context.Context, shared bool) {
			if shared {
				singleFlight.Add(ctx, 1, sharedFlight)
			} else {
				singleFlight.Add(ctx, 1, notSharedFlight)
			}
		},
	}
}
