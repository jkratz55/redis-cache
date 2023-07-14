package otel

import (
	cache "github.com/jkratz55/redis-cache"
)

func InstrumentMetrics(c *cache.Cache) error {
	// todo: implement code
	return nil
}

// todo: implement metric collection for cache
type metricsHook struct {
}

func (m *metricsHook) MarshalHook(next cache.Marshaller) cache.Marshaller {
	// TODO implement me
	panic("implement me")
}

func (m *metricsHook) UnmarshallHook(next cache.Unmarshaller) cache.Unmarshaller {
	// TODO implement me
	panic("implement me")
}

func (m *metricsHook) CompressHook(next cache.CompressionHook) cache.CompressionHook {
	// TODO implement me
	panic("implement me")
}

func (m *metricsHook) DecompressHook(next cache.CompressionHook) cache.CompressionHook {
	// TODO implement me
	panic("implement me")
}
