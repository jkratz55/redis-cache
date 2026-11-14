package prometheus

import (
	"errors"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

type sample struct {
	count uint64
	sum   float64
}

func newTestMetricsHook(t *testing.T) (*metricsHook, *prometheus.Registry) {
	t.Helper()

	conf := newConfig()

	compressionTime := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: conf.namespace,
		Subsystem: conf.subSystem,
		Name:      "compression_time",
		Buckets:   conf.buckets,
	}, cacheLabels)
	compressionErrors := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: conf.namespace,
		Subsystem: conf.subSystem,
		Name:      "compression_errs",
	}, cacheLabels)
	bytesIn := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: conf.namespace,
		Subsystem: conf.subSystem,
		Name:      "bytes_in",
	})
	bytesOut := prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: conf.namespace,
		Subsystem: conf.subSystem,
		Name:      "bytes_out",
	})
	compressionSize := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: conf.namespace,
		Subsystem: conf.subSystem,
		Name:      "compression_payload_size_bytes",
		Buckets:   conf.compressionSizeBuckets,
	}, compressionSizeLabels)

	reg := prometheus.NewRegistry()
	reg.MustRegister(compressionTime, compressionErrors, bytesIn, bytesOut, compressionSize)

	return &metricsHook{
		compressionTime:   compressionTime,
		compressionErrors: compressionErrors,
		bytesIn:           bytesIn,
		bytesOut:          bytesOut,
		compressionSize:   compressionSize,
	}, reg
}

// sizeSamples returns the sample count and sum of the compression size
// histogram keyed by "operation/stage".
func sizeSamples(t *testing.T, reg *prometheus.Registry) map[string]sample {
	t.Helper()

	families, err := reg.Gather()
	assert.NoError(t, err)

	samples := make(map[string]sample)
	for _, mf := range families {
		if mf.GetName() != "redis_cache_compression_payload_size_bytes" {
			continue
		}
		for _, m := range mf.GetMetric() {
			var operation, stage string
			for _, lp := range m.GetLabel() {
				switch lp.GetName() {
				case "operation":
					operation = lp.GetValue()
				case "stage":
					stage = lp.GetValue()
				}
			}
			samples[operation+"/"+stage] = sample{
				count: m.GetHistogram().GetSampleCount(),
				sum:   m.GetHistogram().GetSampleSum(),
			}
		}
	}
	return samples
}

// counterValue returns the value of the counter with the given name, optionally
// filtered by the operation label.
func counterValue(t *testing.T, reg *prometheus.Registry, name, operation string) float64 {
	t.Helper()

	families, err := reg.Gather()
	assert.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if operation == "" {
				return m.GetCounter().GetValue()
			}
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "operation" && lp.GetValue() == operation {
					return m.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

func histogramCount(t *testing.T, reg *prometheus.Registry, name, operation string) uint64 {
	t.Helper()

	families, err := reg.Gather()
	assert.NoError(t, err)

	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				if lp.GetName() == "operation" && lp.GetValue() == operation {
					return m.GetHistogram().GetSampleCount()
				}
			}
		}
	}
	return 0
}

func TestMetricsHook_CompressHook(t *testing.T) {
	hook, reg := newTestMetricsHook(t)

	compress := hook.CompressHook(func(data []byte) ([]byte, error) {
		return make([]byte, 10), nil
	})

	out, err := compress(make([]byte, 100))
	assert.NoError(t, err)
	assert.Len(t, out, 10)

	samples := sizeSamples(t, reg)
	assert.Equal(t, sample{count: 1, sum: 100}, samples["compress/pre"])
	assert.Equal(t, sample{count: 1, sum: 10}, samples["compress/post"])
	assert.NotContains(t, samples, "decompress/pre")
	assert.NotContains(t, samples, "decompress/post")

	// Existing metrics continue to be recorded
	assert.Equal(t, uint64(1), histogramCount(t, reg, "redis_cache_compression_time", operationCompress))
	assert.Equal(t, float64(10), counterValue(t, reg, "redis_cache_bytes_out", ""))
}

func TestMetricsHook_DecompressHook(t *testing.T) {
	hook, reg := newTestMetricsHook(t)

	decompress := hook.DecompressHook(func(data []byte) ([]byte, error) {
		return make([]byte, 100), nil
	})

	out, err := decompress(make([]byte, 10))
	assert.NoError(t, err)
	assert.Len(t, out, 100)

	samples := sizeSamples(t, reg)
	assert.Equal(t, sample{count: 1, sum: 10}, samples["decompress/pre"])
	assert.Equal(t, sample{count: 1, sum: 100}, samples["decompress/post"])
	assert.NotContains(t, samples, "compress/pre")
	assert.NotContains(t, samples, "compress/post")

	// Existing metrics continue to be recorded
	assert.Equal(t, uint64(1), histogramCount(t, reg, "redis_cache_compression_time", operationDecompress))
	assert.Equal(t, float64(10), counterValue(t, reg, "redis_cache_bytes_in", ""))
}

func TestMetricsHook_CompressionHooks_Error(t *testing.T) {
	hook, reg := newTestMetricsHook(t)

	failing := func(data []byte) ([]byte, error) {
		return nil, errors.New("boom")
	}

	_, err := hook.CompressHook(failing)(make([]byte, 50))
	assert.Error(t, err)
	_, err = hook.DecompressHook(failing)(make([]byte, 20))
	assert.Error(t, err)

	samples := sizeSamples(t, reg)
	assert.Equal(t, sample{count: 1, sum: 50}, samples["compress/pre"])
	assert.Equal(t, sample{count: 1, sum: 20}, samples["decompress/pre"])
	assert.NotContains(t, samples, "compress/post")
	assert.NotContains(t, samples, "decompress/post")

	assert.Equal(t, float64(1), counterValue(t, reg, "redis_cache_compression_errs", operationCompress))
	assert.Equal(t, float64(1), counterValue(t, reg, "redis_cache_compression_errs", operationDecompress))
}

func TestMetricsHook_CompressHook_EmptyInput(t *testing.T) {
	hook, reg := newTestMetricsHook(t)

	compress := hook.CompressHook(func(data []byte) ([]byte, error) {
		return []byte{}, nil
	})

	_, err := compress(nil)
	assert.NoError(t, err)

	samples := sizeSamples(t, reg)
	assert.Equal(t, sample{count: 1, sum: 0}, samples["compress/pre"])
	assert.Equal(t, sample{count: 1, sum: 0}, samples["compress/post"])
}

func TestConfig_CompressionSizeBuckets(t *testing.T) {
	conf := newConfig()
	assert.Equal(t, prometheus.ExponentialBuckets(64, 4, 10), conf.compressionSizeBuckets)

	WithCompressionSizeBuckets([]float64{1, 2})(conf)
	assert.Equal(t, []float64{1, 2}, conf.compressionSizeBuckets)
}
