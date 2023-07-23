package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
)

var defaultBuckets = prometheus.ExponentialBuckets(0.005, 2, 8)

type config struct {
	namespace    string
	subSystem    string
	globalLabels map[string]string
	buckets      []float64
}

func newConfig() *config {
	return &config{
		namespace:    "redis",
		subSystem:    "cache",
		globalLabels: make(map[string]string),
		buckets:      defaultBuckets,
	}
}

type Option func(c *config)

// WithNamespace overrides the default namespace used for Prometheus time series
func WithNamespace(namespace string) Option {
	return func(c *config) {
		c.namespace = namespace
	}
}

// WithSubsystem overrides the default subsystem used for Prometheus time series
func WithSubsystem(subSystem string) Option {
	return func(c *config) {
		c.subSystem = subSystem
	}
}

// WithConstLabels sets const labels for all Prometheus time series
func WithConstLabels(labels map[string]string) Option {
	return func(c *config) {
		c.globalLabels = labels
	}
}

// WithBuckets overrides the default buckets used for Prometheus histograms
func WithBuckets(buckets []float64) Option {
	return func(c *config) {
		c.buckets = buckets
	}
}
