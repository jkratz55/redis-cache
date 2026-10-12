package cache

import (
	"errors"
)

type retryable interface {
	IsRetryable() bool
}

// IsRetryable accepts an error and returns a boolean indicating if the operation
// that generated the error is retryable. Wrapped errors are inspected as well.
func IsRetryable(err error) bool {
	var re retryable
	return errors.As(err, &re) && re.IsRetryable()
}

type RetryableError struct {
	retryable bool
	cause     error
}

func (e RetryableError) IsRetryable() bool {
	return e.retryable
}

func (e RetryableError) Error() string {
	if e.cause == nil {
		return "retryable error"
	}
	return e.cause.Error()
}

func (e RetryableError) Unwrap() error {
	return e.cause
}
