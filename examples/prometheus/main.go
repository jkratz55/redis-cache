package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	cache "github.com/jkratz55/redis-cache"
	"github.com/jkratz55/redis-cache/prometheus"
)

func main() {

	redisClient := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		MinIdleConns: 10,
		MaxIdleConns: 100,
		PoolSize:     1000,
	})

	if err := redisClient.Ping(context.Background()).Err(); err != nil {
		fmt.Println("Opps ping to Redis failed!", err)
	}

	if err := prometheus.InstrumentClientMetrics(redisClient); err != nil {
		panic(err)
	}

	rdb := cache.New(redisClient)

	if err := prometheus.InstrumentMetrics(rdb); err != nil {
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
	http.Handle("/mget", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

		type request struct {
			Keys []string `json:"keys"`
		}

		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		res, err := cache.MGet[string](r.Context(), rdb, req.Keys...)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(res)
	}))

	http.ListenAndServe(":8080", nil)
}
