package cache

type retryable interface {
	IsRetryable() bool
}

// IsRetryable accepts an error and returns a boolean indicating if the operation
// that generated the error is retryable.
func IsRetryable(err error) bool {
	re, ok := err.(retryable)
	return ok && re.IsRetryable()
}

type RetryableError struct {
	retryable bool
	cause     error
}

func (e RetryableError) IsRetryable() bool {
	return e.retryable
}

func (e RetryableError) Error() string {
	return e.cause.Error()
}
