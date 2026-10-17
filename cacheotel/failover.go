package cacheotel

import (
	"context"
	"errors"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache/v2"
)

// ObserveFailoverClient registers an asynchronous gauge, cache.failover.active, reporting
// which client of fc is active. The gauge reports 1 for the active client and 0 for the
// other one, with the attribute client set to primary or secondary.
//
// Call Unregister on the returned Registration to stop observing fc, for example once fc
// is closed. ObserveFailoverClient panics if fc is nil.
func ObserveFailoverClient(fc *cache.FailoverClient, opts ...InstrumentOption) (metric.Registration, error) {
	if fc == nil {
		panic(errors.New("nil FailoverClient"))
	}

	conf := newCommonConfig()
	for _, opt := range opts {
		opt.apply(conf)
	}

	meter := conf.meterProvider.Meter(scope)
	active, err := meter.Int64ObservableGauge("cache.failover.active",
		metric.WithDescription("Whether the primary or secondary client of a FailoverClient is active"))
	if err != nil {
		return nil, err
	}

	primary := conf.attributeSet(attribute.String("client", "primary"))
	secondary := conf.attributeSet(attribute.String("client", "secondary"))
	return meter.RegisterCallback(func(ctx context.Context, o metric.Observer) error {
		primaryActive, secondaryActive := int64(1), int64(0)
		if fc.IsSecondary() {
			primaryActive, secondaryActive = 0, 1
		}
		o.ObserveInt64(active, primaryActive, primary)
		o.ObserveInt64(active, secondaryActive, secondary)
		return nil
	}, active)
}
