package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/rueidis"
	"github.com/redis/rueidis/rueidisotel"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/otel"

	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/sdk/metric"
)

func main() {

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: false,
	}))

	exporter, err := prometheus.New()
	if err != nil {
		logger.Error("error creating prometheus exporter", slog.String("err", err.Error()))
		panic(err)
	}
	provider := metric.NewMeterProvider(metric.WithReader(exporter))

	redisClient, err := rueidisotel.NewClient(rueidis.ClientOption{
		InitAddress:       []string{"localhost:6379"},
		ForceSingleClient: true,
	}, rueidisotel.WithMeterProvider(provider))
	if err != nil {
		logger.Error("error creating redis client", slog.String("err", err.Error()))
		panic(err)
	}
	defer redisClient.Close()

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := redisClient.Do(ctx, redisClient.B().Ping().Build()).Error()
		if err != nil {
			logger.Error("error pinging redis", slog.String("err", err.Error()))
			panic(err)
		}
	}()

	redisClient, err = otel.InstrumentClient(redisClient,
		otel.WithMeterProvider(provider),
		otel.WithExplicitBucketBoundaries(otel.ExponentialBuckets(0.001, 2, 6)))

	rdb := cache.New(redisClient)
	err = otel.InstrumentMetrics(rdb, otel.WithMeterProvider(provider))

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

		err := rdb.Set(r.Context(), key, value, time.Minute*10)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))

	http.ListenAndServe(":8080", nil)
}
