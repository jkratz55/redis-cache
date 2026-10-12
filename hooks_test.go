package cache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// hookRecorder records the invocations of Hooks.
type hookRecorder struct {
	mu           sync.Mutex
	hits, misses int
	encodes      [][2]int
	decodes      []int
	decodeErrs   []error
	singleFlight []bool
}

func (r *hookRecorder) hooks() Hooks {
	return Hooks{
		OnLookup: func(ctx context.Context, hits, misses int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.hits += hits
			r.misses += misses
		},
		OnEncode: func(ctx context.Context, serializedSize, storedSize int) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.encodes = append(r.encodes, [2]int{serializedSize, storedSize})
		},
		OnDecode: func(ctx context.Context, storedSize int, err error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.decodes = append(r.decodes, storedSize)
			if err != nil {
				r.decodeErrs = append(r.decodeErrs, err)
			}
		},
		OnSingleFlight: func(ctx context.Context, shared bool) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.singleFlight = append(r.singleFlight, shared)
		},
	}
}

func TestHooks_Cache(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	var rec hookRecorder
	c := New(client, Instrumentation(rec.hooks()))

	assert.NoError(t, c.Set(ctx, "a", "value", 0))
	assert.NoError(t, c.Set(ctx, "b", "value", 0))
	if assert.Len(t, rec.encodes, 2) {
		// msgpack encodes "value" in 6 bytes, the header adds 4.
		assert.Equal(t, [2]int{6, 10}, rec.encodes[0])
	}

	var val string
	assert.NoError(t, c.Get(ctx, "a", &val))
	assert.ErrorIs(t, c.Get(ctx, "missing", &val), ErrKeyNotFound)
	assert.ErrorIs(t, c.GetAndExpire(ctx, "missing", &val, time.Minute), ErrKeyNotFound)
	assert.NoError(t, c.GetAndDelete(ctx, "a", &val))
	assert.Equal(t, 2, rec.hits)
	assert.Equal(t, 2, rec.misses)

	_, err := MGet[string](ctx, c, "a", "b", "c")
	assert.NoError(t, err)
	_, err = MGetMap[string](ctx, c, "b")
	assert.NoError(t, err)
	assert.Equal(t, 4, rec.hits)
	assert.Equal(t, 4, rec.misses)
	assert.Equal(t, []int{10, 10, 10, 10}, rec.decodes)

	// A value in a format that isn't registered counts as a hit and its decode
	// error is reported.
	assert.NoError(t, client.Set(ctx, "unsupported", []byte{0xC1, 0x01, 250, CodecNone, 0x01}, 0).Err())
	assert.ErrorIs(t, c.Get(ctx, "unsupported", &val), ErrUnsupportedFormat)
	assert.Equal(t, 5, rec.hits)
	if assert.Len(t, rec.decodeErrs, 1) {
		assert.ErrorIs(t, rec.decodeErrs[0], ErrUnsupportedFormat)
	}

	// Lookups aren't reported when Redis fails.
	server.SetError("boom")
	assert.Error(t, c.Get(ctx, "b", &val))
	_, err = MGet[string](ctx, c, "b")
	assert.Error(t, err)
	assert.Equal(t, 5, rec.hits)
	assert.Equal(t, 4, rec.misses)
}

func TestHooks_TypedCache(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	var rec hookRecorder
	c := NewTyped[string](client, Instrumentation(rec.hooks()))

	assert.NoError(t, c.Set(ctx, "a", "value", 0))
	_, err := c.SetIfAbsent(ctx, "b", "value", 0)
	assert.NoError(t, err)
	assert.NoError(t, c.MSet(ctx, map[string]string{"c": "value"}))
	assert.Len(t, rec.encodes, 3)

	_, err = c.Get(ctx, "a")
	assert.NoError(t, err)
	_, err = c.Get(ctx, "missing")
	assert.ErrorIs(t, err, ErrKeyNotFound)
	_, err = c.MGetMap(ctx, "a", "b", "missing")
	assert.NoError(t, err)
	assert.Equal(t, 3, rec.hits)
	assert.Equal(t, 2, rec.misses)
	assert.Len(t, rec.decodes, 3)
}

func TestHooks_SingleFlight(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	flights := make(chan bool, 2)
	c := New(client, Instrumentation(Hooks{
		OnSingleFlight: func(ctx context.Context, shared bool) { flights <- shared },
	}))

	release := make(chan struct{})
	loader := func(ctx context.Context) (string, error) {
		<-release
		return "value", nil
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		_, err := ReadThroughSingleFlight[string](ctx, c, "key", ReadThroughOptions{}, loader)
		assert.NoError(t, err)
	})
	// The first caller starts the load.
	assert.False(t, <-flights)

	wg.Go(func() {
		_, err := ReadThroughSingleFlight[string](ctx, c, "key", ReadThroughOptions{}, loader)
		assert.NoError(t, err)
	})
	// The second caller joins it.
	assert.True(t, <-flights)

	close(release)
	wg.Wait()
}

func TestHooks_PanicsAreRecovered(t *testing.T) {
	setup()
	defer tearDown()

	ctx := context.Background()
	c := New(client, Instrumentation(Hooks{
		OnLookup: func(context.Context, int, int) { panic("lookup") },
		OnEncode: func(context.Context, int, int) { panic("encode") },
		OnDecode: func(context.Context, int, error) { panic("decode") },
		OnSingleFlight: func(context.Context, bool) {
			panic("single flight")
		},
	}))

	assert.NotPanics(t, func() {
		assert.NoError(t, c.Set(ctx, "key", "value", 0))
		var val string
		assert.NoError(t, c.Get(ctx, "key", &val))
		assert.Equal(t, "value", val)
		_, err := ReadThroughSingleFlight[string](ctx, c, "missing", ReadThroughOptions{},
			func(ctx context.Context) (string, error) {
				return "loaded", nil
			})
		assert.NoError(t, err)
	})
}

func TestHooks_Unset(t *testing.T) {
	var h Hooks
	assert.NotPanics(t, func() {
		h.lookup(context.Background(), 1, 1)
		h.encoded(context.Background(), 1, 1)
		h.decoded(context.Background(), 1, errors.New("error"))
		h.singleFlight(context.Background(), true)
	})
}
