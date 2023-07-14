package otel

import (
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	name          = "github.com/jkratz55/redis-cache"
	defaultSystem = "db.redis"
)

type config struct {
	system        string
	attrs         []attribute.KeyValue
	meterProvider metric.MeterProvider
	meter         metric.Meter
}

func newConfig() *config {
	conf := &config{
		system:        defaultSystem,
		attrs:         []attribute.KeyValue{},
		meterProvider: otel.GetMeterProvider(),
	}

	return conf
}

type Option func(c *config)

var nopOption = func(c *config) {}

func WithAttributes(attrs []attribute.KeyValue) Option {
	return func(c *config) {
		c.attrs = attrs
	}
}

func WithMeterProvider(mp metric.MeterProvider) Option {
	if mp == nil {
		return nopOption
	}
	return func(c *config) {
		c.meterProvider = mp
	}
}

func WithSystem(system string) Option {
	if strings.TrimSpace(system) == "" {
		return nopOption
	}
	return func(c *config) {
		c.system = system
	}
}
