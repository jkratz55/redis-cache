package main

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"

	cache "github.com/jkratz55/redis-cache"
	"github.com/jkratz55/redis-cache/otel"
)

func main() {

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	exporter, err := prometheus.New(prometheus.WithAggregationSelector(func(kind metric.InstrumentKind) metric.Aggregation {
		if kind == metric.InstrumentKindHistogram {
			return metric.AggregationExplicitBucketHistogram{
				Boundaries: []float64{0.005, 0.010, 0.020, 0.040, 0.080, 0.120},
				NoMinMax:   false,
			}
		}
		return metric.DefaultAggregationSelector(kind)
	}))
	if err != nil {
		panic(err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))

	if err := otel.InstrumentClientMetrics(redisClient, otel.WithMeterProvider(provider)); err != nil {
		panic(err)
	}

	rdb := cache.New(redisClient)
	if err := otel.InstrumentMetrics(rdb, otel.WithMeterProvider(provider)); err != nil {
		panic(err)
	}

	http.Handle("/metrics", promhttp.Handler())
	http.Handle("/get", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")

		var res string
		err := rdb.Get(r.Context(), key, &res)
		if errors.Is(err, cache.ErrKeyNotFound) {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Write([]byte(res))
	}))
	http.Handle("/set", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")

		err := rdb.SetWithTTL(r.Context(), key, value, time.Minute*2)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))

	http.ListenAndServe(":8080", nil)
}
