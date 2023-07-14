package otel

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type config struct {
	atts          []attribute.KeyValue
	meterProvider metric.MeterProvider
	meter         metric.Meter
	poolName      string
}

func newConfig(opts ...baseOption) *config {
	conf := &config{
		atts:          []attribute.KeyValue{},
		meterProvider: otel.GetMeterProvider(),
	}

	for _, opt := range opts {
		opt.apply(conf)
	}

	return conf
}

type baseOption interface {
	apply(cong *config)
}

type Option interface {
	baseOption
	metrics()
}

type option func(conf *config)

func (fn option) apply(conf *config) {
	fn(conf)
}

func (fn option) metrics() {}

func WithAtributes(atts ...attribute.KeyValue) Option {
	return option(func(conf *config) {
		conf.atts = atts
	})
}

type MetricsOption interface {
	baseOption
	metrics()
}

type metricOption func(conf *config)

func (m metricOption) apply(conf *config) {
	m(conf)
}

func (m metricOption) metrics() {}

var _ MetricsOption = (*metricOption)(nil)

func WithMeterProvider(mp metric.MeterProvider) MetricsOption {
	return metricOption(func(conf *config) {
		conf.meterProvider = mp
	})
}
