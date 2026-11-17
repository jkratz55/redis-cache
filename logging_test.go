package cache

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
)

func newLogTestClient(t *testing.T) *redis.Client {
	t.Helper()
	s := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func newBufLogger(buf *bytes.Buffer) *slog.Logger {
	return slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

func TestLogger_PanicsOnNil(t *testing.T) {
	assert.Panics(t, func() { Logger(nil) })
}

func TestCache_LargeKeyWarning(t *testing.T) {
	ctx := context.Background()
	rc := newLogTestClient(t)

	large := strings.Repeat("a", largeKeyThreshold)
	small := "small"

	// Use plain string values stored with the default (msgpack) serialization.
	var buf bytes.Buffer
	c := New(rc, Logger(newBufLogger(&buf)))
	assert.NoError(t, c.Set(ctx, "large", large))
	assert.NoError(t, c.Set(ctx, "small", small))

	var out string
	assert.NoError(t, c.Get(ctx, "small", &out))
	assert.Empty(t, buf.String())

	assert.NoError(t, c.Get(ctx, "large", &out))
	logged := buf.String()
	assert.Contains(t, logged, "level=WARN")
	assert.Contains(t, logged, "key=large")
	assert.Contains(t, logged, "size_bytes=")
	assert.Contains(t, logged, "problematic")

	buf.Reset()
	assert.NoError(t, c.GetAndExpire(ctx, "large", &out, 0))
	assert.Contains(t, buf.String(), "key=large")

	buf.Reset()
	_, err := MGet[string](ctx, c, "small", "large")
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "key=large")
	assert.NotContains(t, buf.String(), "key=small")

	buf.Reset()
	_, err = MGetValues[string](ctx, c, "small", "large")
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "key=large")
}

func TestCache_DefaultLoggerIsNop(t *testing.T) {
	ctx := context.Background()
	rc := newLogTestClient(t)
	c := New(rc)
	assert.NoError(t, c.Set(ctx, "large", strings.Repeat("a", largeKeyThreshold)))
	var out string
	assert.NoError(t, c.Get(ctx, "large", &out))
}

func TestTypedCache_LargeKeyWarning(t *testing.T) {
	ctx := context.Background()
	rc := newLogTestClient(t)

	var buf bytes.Buffer
	c := NewTyped[string](rc, Logger(newBufLogger(&buf)))
	assert.NoError(t, c.Set(ctx, "large", strings.Repeat("a", largeKeyThreshold), 0))
	assert.NoError(t, c.Set(ctx, "small", "small", 0))

	_, err := c.Get(ctx, "small")
	assert.NoError(t, err)
	assert.Empty(t, buf.String())

	_, err = c.Get(ctx, "large")
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "level=WARN")
	assert.Contains(t, buf.String(), "key=large")

	buf.Reset()
	_, err = c.MGet(ctx, "small", "large")
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "key=large")
	assert.NotContains(t, buf.String(), "key=small")

	buf.Reset()
	_, err = c.GetAndDelete(ctx, "large")
	assert.NoError(t, err)
	assert.Contains(t, buf.String(), "key=large")
}
