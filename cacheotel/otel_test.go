package cacheotel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	cache "github.com/jkratz55/redis-cache/v2"
)

// newTestMeterProvider returns a MeterProvider whose metrics are read with the
// returned ManualReader.
func newTestMeterProvider(t *testing.T) (*sdkmetric.MeterProvider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = mp.Shutdown(context.Background()) })
	return mp, reader
}

// collect returns the metrics recorded by the meter provider of reader by name.
func collect(t *testing.T, reader *sdkmetric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	metrics := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		assert.Equal(t, scope, sm.Scope.Name)
		for _, m := range sm.Metrics {
			metrics[m.Name] = m
		}
	}
	return metrics
}

// counterValue returns the value of the data point of an int64 counter or gauge
// with exactly attrs.
func counterValue(t *testing.T, m metricdata.Metrics, attrs ...attribute.KeyValue) int64 {
	t.Helper()
	set := attribute.NewSet(attrs...)
	var points []metricdata.DataPoint[int64]
	switch data := m.Data.(type) {
	case metricdata.Sum[int64]:
		points = data.DataPoints
	case metricdata.Gauge[int64]:
		points = data.DataPoints
	default:
		t.Fatalf("metric %s has unexpected data %T", m.Name, m.Data)
	}
	for _, dp := range points {
		if dp.Attributes.Equals(&set) {
			return dp.Value
		}
	}
	return 0
}

// histogramCount returns the number of measurements of the data point of a
// histogram with exactly attrs.
func histogramCount(t *testing.T, m metricdata.Metrics, attrs ...attribute.KeyValue) uint64 {
	t.Helper()
	set := attribute.NewSet(attrs...)
	switch data := m.Data.(type) {
	case metricdata.Histogram[int64]:
		for _, dp := range data.DataPoints {
			if dp.Attributes.Equals(&set) {
				return dp.Count
			}
		}
	case metricdata.Histogram[float64]:
		for _, dp := range data.DataPoints {
			if dp.Attributes.Equals(&set) {
				return dp.Count
			}
		}
	default:
		t.Fatalf("metric %s has unexpected data %T", m.Name, m.Data)
	}
	return 0
}

// histogramBounds returns the bucket boundaries of the first data point of a
// histogram.
func histogramBounds(t *testing.T, m metricdata.Metrics) []float64 {
	t.Helper()
	switch data := m.Data.(type) {
	case metricdata.Histogram[int64]:
		return data.DataPoints[0].Bounds
	case metricdata.Histogram[float64]:
		return data.DataPoints[0].Bounds
	default:
		t.Fatalf("metric %s has unexpected data %T", m.Name, m.Data)
		return nil
	}
}

func TestFormatNames(t *testing.T) {
	assert.Equal(t, "msgpack", serializerName(cache.SerializerMsgPack))
	assert.Equal(t, "json", serializerName(cache.SerializerJSON))
	assert.Equal(t, "protobuf", serializerName(cache.SerializerProtobuf))
	assert.Equal(t, "200", serializerName(200))

	for id, name := range map[uint8]string{
		cache.CodecNone:   "none",
		cache.CodecFlate:  "flate",
		cache.CodecGzip:   "gzip",
		cache.CodecLZ4:    "lz4",
		cache.CodecS2:     "s2",
		cache.CodecSnappy: "snappy",
		cache.CodecZstd:   "zstd",
		201:               "201",
	} {
		assert.Equal(t, name, codecName(id))
	}
}
