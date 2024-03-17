package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	defaultBuckets         = prometheus.ExponentialBuckets(0.005, 2, 8)
	defaultMgetKeysBuckets = []float64{50, 100, 200, 500, 1000, 2000, 4000, 8000, 16000}
)

type config struct {
	namespace       string
	subSystem       string
	globalLabels    map[string]string
	buckets         []float64
	mgetKeysBuckets []float64
}

func newConfig() *config {
	return &config{
		namespace:       "redis",
		subSystem:       "cache",
		globalLabels:    make(map[string]string),
		buckets:         defaultBuckets,
		mgetKeysBuckets: defaultMgetKeysBuckets,
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

// WithMGETKeysBuckets overrides the default buckets used for the Prometheus
// histogram that tracks the number of keys requested in MGET commands
func WithMGETKeysBuckets(buckets []float64) Option {
	return func(c *config) {
		c.mgetKeysBuckets = buckets
	}
}
