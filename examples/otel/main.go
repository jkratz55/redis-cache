package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"

	cache "github.com/jkratz55/redis-cache/v2"
	"github.com/jkratz55/redis-cache/v2/cacheotel"
)

func main() {

	// Initialize a logger. You can use any logger you wish but in this example
	// we are sticking to the standard library using slog.
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		AddSource: true,
	}))

	// Setup OpenTelemetry trace exporter
	traceExporter, err := otlptracehttp.New(context.Background())
	if err != nil {
		logger.Error("error creating trace exporter", slog.String("err", err.Error()))
		panic(err)
	}

	// Configure OpenTelemetry resource and TracerProvider
	otelResource := resource.NewWithAttributes(
		semconv.SchemaURL,
		semconv.ServiceNameKey.String("cacheotel-example"),
		semconv.ServiceVersionKey.String("1.0.0"))
	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter),
		trace.WithResource(otelResource))
	defer func() {
		err := traceProvider.Shutdown(context.Background())
		if err != nil {
			logger.Error("error shutting down trace provider", slog.String("err", err.Error()))
		}
	}()

	// Set the TraceProvider and TextMapPropagator globally
	otel.SetTracerProvider(traceProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	exporter, err := prometheus.New()
	if err != nil {
		panic(err)
	}

	provider := metric.NewMeterProvider(metric.WithReader(exporter))
	otel.SetMeterProvider(provider)

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	if err := redisotel.InstrumentTracing(client); err != nil {
		panic(err)
	}
	if err := redisotel.InstrumentMetrics(client); err != nil {
		panic(err)
	}

	func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		defer cancel()

		err := client.Ping(ctx).Err()
		if err != nil {
			panic(err)
		}
	}()

	// Create instance of the instrumented cache instead of cache.Cache. This will automatically
	// capture execution time of not only the client, but compression and serialization.
	rdb := cacheotel.InstrumentCache(cache.New(client))

	ctx := context.Background()

	// Perform some Redis operations to capture instrumentation
	for i := 0; i < 10; i++ {
		err := rdb.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), 0)
		if err != nil {
			panic(err)
		}
	}

	var val string
	if err := rdb.Get(ctx, "key0", &val); err != nil {
		panic(err)
	}
	if val != "value0" {
		panic("unexpected value")
	}

	keys := []string{"key0", "key1", "key2", "key3", "key4", "key5", "key6", "key7", "key8", "key9"}
	vals, err := cacheotel.MGet[string](ctx, rdb, keys...)
	if err != nil {
		panic(err)
	}
	fmt.Println(vals)

	// Setup Prometheus metrics endpoint and other handlers for testing/demo
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

		err := rdb.Set(r.Context(), key, value, time.Minute*2)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))

	http.ListenAndServe(":8080", nil)
}
