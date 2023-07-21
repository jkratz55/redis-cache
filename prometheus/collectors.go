package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type connPoolStatReporter interface {
	PoolStats() *redis.PoolStats
}

type connPoolStatsCollector struct {
	reporters connPoolStatReporter
	hits      *prometheus.Desc
	misses    *prometheus.Desc
	timeouts  *prometheus.Desc
	total     *prometheus.Desc
	idle      *prometheus.Desc
	stale     *prometheus.Desc
}

func newConnPoolStatsCollector(conf *config, reporter connPoolStatReporter) *connPoolStatsCollector {
	return &connPoolStatsCollector{
		reporter: reporter,
		hits: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_hit_total"),
			"Number of times an idle connection was retrieved from the connection pool",
			nil,
			conf.globalLabels),
		misses: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_miss_total"),
			"Number of times an idle connection was not available in the connection pool",
			nil,
			conf.globalLabels),
		timeouts: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_timeout_total"),
			"Number of times a timeout occurred attempting to retrieve a connection from the pool",
			nil,
			conf.globalLabels),
		total: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_conn_total"),
			"Current number of connections in the pool",
			nil,
			conf.globalLabels),
		idle: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_conn_idle"),
			"Current number of idle connections in the pool",
			nil,
			conf.globalLabels),
		stale: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_conn_stale"),
			"Number of times a connection was removed from the pool because it was stale",
			nil,
			conf.globalLabels),
	}
}

func (c connPoolStatsCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.hits
	descs <- c.misses
	descs <- c.timeouts
	descs <- c.total
	descs <- c.idle
	descs <- c.stale
}

func (c connPoolStatsCollector) Collect(metrics chan<- prometheus.Metric) {
	stats := c.reporter.PoolStats()
	metrics <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(stats.Hits))
	metrics <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(stats.Misses))
	metrics <- prometheus.MustNewConstMetric(c.timeouts, prometheus.CounterValue, float64(stats.Timeouts))
	metrics <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(stats.TotalConns))
	metrics <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(stats.IdleConns))
	metrics <- prometheus.MustNewConstMetric(c.stale, prometheus.CounterValue, float64(stats.StaleConns))
}
