package cache

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryable(t *testing.T) {
	plainErr := errors.New("plain ass error")
	assert.False(t, IsRetryable(plainErr))
	assert.False(t, IsRetryable(nil))

	coolErr := RetryableError{
		retryable: true,
		cause:     plainErr,
	}
	assert.True(t, IsRetryable(coolErr))

	lameErr := RetryableError{
		retryable: false,
		cause:     plainErr,
	}
	assert.False(t, IsRetryable(lameErr))

	// Wrapped errors are inspected as well.
	assert.True(t, IsRetryable(fmt.Errorf("upsert: %w", coolErr)))
	assert.True(t, IsRetryable(fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", coolErr))))
	assert.False(t, IsRetryable(fmt.Errorf("upsert: %w", lameErr)))
	assert.True(t, IsRetryable(errors.Join(plainErr, coolErr)))
}

func TestRetryableError_ZeroValue(t *testing.T) {
	var err RetryableError
	assert.NotPanics(t, func() {
		assert.Equal(t, "retryable error", err.Error())
	})
	assert.Nil(t, err.Unwrap())
	assert.False(t, IsRetryable(err))

	plainErr := errors.New("boom")
	err = RetryableError{retryable: true, cause: plainErr}
	assert.Equal(t, "boom", err.Error())
	assert.ErrorIs(t, err, plainErr)
}

func TestMultiKeyError(t *testing.T) {
	errA := errors.New("error a")
	errB := errors.New("error b")

	single := &MultiKeyError{Errors: map[string]error{"a": errA}}
	assert.EqualError(t, single, "error a")
	assert.ErrorIs(t, single, errA)

	multi := &MultiKeyError{Errors: map[string]error{"b": errB, "a": errA}}
	assert.EqualError(t, multi, "2 keys failed: error a; error b")
	assert.Equal(t, []error{errA, errB}, multi.Unwrap())
	assert.ErrorIs(t, multi, errA)
	assert.ErrorIs(t, multi, errB)
}
