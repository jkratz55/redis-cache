package main

import (
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	cache "github.com/jkratz55/redis-cache"
	"github.com/jkratz55/redis-cache/prometheus"
)

func main() {

	redisClient := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    []string{"192.168.50.163:6379"},
		Password: "an0slrEJ5I",
	})

	if err := prometheus.InstrumentClientMetrics(redisClient); err != nil {
		panic(err)
	}

	rdb := cache.NewCache(redisClient)

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

		err := rdb.SetTTL(r.Context(), key, value, time.Minute*2)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}))

	http.ListenAndServe(":8080", nil)
}
