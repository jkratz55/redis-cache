package cacheotel

import (
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/attribute"

	cache "github.com/jkratz55/redis-cache/v2"
)

func TestObserveFailoverClient(t *testing.T) {
	primaryServer := miniredis.RunT(t)
	secondaryServer := miniredis.RunT(t)
	primary := redis.NewClient(&redis.Options{Addr: primaryServer.Addr()})
	secondary := redis.NewClient(&redis.Options{Addr: secondaryServer.Addr()})
	fc := cache.NewFailoverClient(primary, secondary, cache.FailoverOptions().
		SetCheckInterval(time.Second).
		SetCheckTimeout(100*time.Millisecond).
		SetFailureThreshold(1))
	defer fc.Close()

	mp, reader := newTestMeterProvider(t)
	name := attribute.String("cache.name", "users")
	reg, err := ObserveFailoverClient(fc, WithMeterProvider(mp), WithAttributes(name))
	assert.NoError(t, err)

	primaryAttr := attribute.String("client", "primary")
	secondaryAttr := attribute.String("client", "secondary")
	active := collect(t, reader)["cache.failover.active"]
	assert.Equal(t, int64(1), counterValue(t, active, name, primaryAttr))
	assert.Equal(t, int64(0), counterValue(t, active, name, secondaryAttr))

	primaryServer.Close()
	assert.Eventually(t, fc.IsSecondary, 5*time.Second, 50*time.Millisecond)
	active = collect(t, reader)["cache.failover.active"]
	assert.Equal(t, int64(0), counterValue(t, active, name, primaryAttr))
	assert.Equal(t, int64(1), counterValue(t, active, name, secondaryAttr))

	assert.NoError(t, reg.Unregister())
	_, ok := collect(t, reader)["cache.failover.active"]
	assert.False(t, ok)
}

func TestObserveFailoverClient_Nil(t *testing.T) {
	assert.Panics(t, func() {
		_, _ = ObserveFailoverClient(nil)
	})
}
