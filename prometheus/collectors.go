package prometheus

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
)

type connPoolStatReporter interface {
	PoolStats() *redis.PoolStats
	Options() *redis.Options
}

type connPoolStatsCollector struct {
	reporters []connPoolStatReporter
	hits      *prometheus.Desc
	misses    *prometheus.Desc
	timeouts  *prometheus.Desc
	total     *prometheus.Desc
	idle      *prometheus.Desc
	stale     *prometheus.Desc
}

func newConnPoolStatsCollector(conf *config, reporters ...connPoolStatReporter) *connPoolStatsCollector {
	return &connPoolStatsCollector{
		reporters: reporters,
		hits: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_hit_total"),
			"Number of times an idle connection was retrieved from the connection pool",
			[]string{"pool"},
			conf.globalLabels),
		misses: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_miss_total"),
			"Number of times an idle connection was not available in the connection pool",
			[]string{"pool"},
			conf.globalLabels),
		timeouts: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_timeout_total"),
			"Number of times a timeout occurred attempting to retrieve a connection from the pool",
			[]string{"pool"},
			conf.globalLabels),
		total: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_conn_total"),
			"Current number of connections in the pool",
			[]string{"pool"},
			conf.globalLabels),
		idle: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_conn_idle"),
			"Current number of idle connections in the pool",
			[]string{"pool"},
			conf.globalLabels),
		stale: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "pool_conn_stale"),
			"Number of times a connection was removed from the pool because it was stale",
			[]string{"pool"},
			conf.globalLabels),
	}
}

func (c *connPoolStatsCollector) addPool(pool connPoolStatReporter) {
	addr := pool.Options().Addr
	for _, existingPool := range c.reporters {
		if addr == existingPool.Options().Addr {
			return
		}
	}
	c.reporters = append(c.reporters, pool)
}

func (c *connPoolStatsCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- c.hits
	descs <- c.misses
	descs <- c.timeouts
	descs <- c.total
	descs <- c.idle
	descs <- c.stale
}

func (c *connPoolStatsCollector) Collect(metrics chan<- prometheus.Metric) {
	for _, reporter := range c.reporters {
		addr := reporter.Options().Addr
		stats := reporter.PoolStats()
		metrics <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(stats.Hits), addr)
		metrics <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(stats.Misses), addr)
		metrics <- prometheus.MustNewConstMetric(c.timeouts, prometheus.CounterValue, float64(stats.Timeouts), addr)
		metrics <- prometheus.MustNewConstMetric(c.total, prometheus.GaugeValue, float64(stats.TotalConns), addr)
		metrics <- prometheus.MustNewConstMetric(c.idle, prometheus.GaugeValue, float64(stats.IdleConns), addr)
		metrics <- prometheus.MustNewConstMetric(c.stale, prometheus.CounterValue, float64(stats.StaleConns), addr)
	}
}

type redisHealthCheck interface {
	Healthy(ctx context.Context) bool
}

type redisStatusCollector struct {
	redisHealthCheck

	status *prometheus.Desc
}

func newRedisStatusCollector(conf *config, pinger redisHealthCheck) *redisStatusCollector {
	return &redisStatusCollector{
		redisHealthCheck: pinger,
		status: prometheus.NewDesc(
			prometheus.BuildFQName(conf.namespace, conf.subSystem, "status"),
			"Indicates if Redis is currently available, values of 1 means reachable while a value of 0 means Redis isn't available",
			[]string{"status"},
			conf.globalLabels),
	}
}

func (r redisStatusCollector) Describe(descs chan<- *prometheus.Desc) {
	descs <- r.status
}

func (r redisStatusCollector) Collect(metrics chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	if r.Healthy(ctx) {
		metrics <- prometheus.MustNewConstMetric(r.status, prometheus.GaugeValue, float64(1), "UP")
	} else {
		metrics <- prometheus.MustNewConstMetric(r.status, prometheus.GaugeValue, float64(0), "UP")
	}
}
