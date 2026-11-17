package cache

import (
	"context"
	"log/slog"
)

// largeKeyThreshold is the size in bytes at which a value loaded from Redis is
// considered a large key (1Mb).
const largeKeyThreshold = 1024 * 1024

// nopLogger returns a *slog.Logger that discards all log records.
func nopLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// warnIfLargeKey logs a warning if the size of the value loaded from Redis is
// 1Mb or greater.
func warnIfLargeKey(ctx context.Context, logger *slog.Logger, key string, size int) {
	if size < largeKeyThreshold || logger == nil {
		return
	}
	logger.WarnContext(ctx, "Large key loaded from Redis. Large keys are problematic for both "+
		"Redis and the application: they increase latency, memory usage, and network "+
		"bandwidth, and can block Redis while being processed. Consider reducing the "+
		"size of the value or splitting it into smaller keys.",
		slog.String("key", key),
		slog.Int("size_bytes", size))
}
