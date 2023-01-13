package cache

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsRetryable(t *testing.T) {
	plainErr := errors.New("plain ass error")
	assert.False(t, IsRetryable(plainErr))

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
}
