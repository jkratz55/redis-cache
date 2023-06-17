package otel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	cache "github.com/jkratz55/redis-cache"
)

const name = "github.com/jkratz55/redis-cache/otel"

type config struct {
	system        string
	attrs         []attribute.KeyValue
	meterProvider metric.MeterProvider
	meter         metric.Meter
}

func newConfig() *config {
	conf := &config{
		system:        "redis",
		attrs:         []attribute.KeyValue{},
		meterProvider: otel.GetMeterProvider(),
	}

	// todo: this needs to be late initialized
	conf.meter = conf.meterProvider.Meter(name,
		metric.WithInstrumentationVersion("semver:"+cache.Version()))

	return conf
}
