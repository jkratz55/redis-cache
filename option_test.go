package cache

import (
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
