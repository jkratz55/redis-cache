package cache

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBatchMultiGets(t *testing.T) {
	assert.PanicsWithError(t, "invalid batch size -1: must not be negative", func() {
		BatchMultiGets(-1)
	})
	assert.NotPanics(t, func() {
		BatchMultiGets(0)
		BatchMultiGets(100)
	})
}

func TestInstrumentation(t *testing.T) {
	setup()
	defer tearDown()

	var first, last bool
	c := New(client,
		Instrumentation(Hooks{OnSingleFlight: func(context.Context, bool) { first = true }}),
		Instrumentation(Hooks{OnSingleFlight: func(context.Context, bool) { last = true }}))

	c.formats.hooks.singleFlight(context.Background(), true)
	assert.False(t, first)
	assert.True(t, last)
}
