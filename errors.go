package cache

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
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

// RetryableError is an error indicating whether the operation that returned it can
// be retried, such as an Upsert whose transaction failed because the key was
// modified concurrently. Use IsRetryable to check an error, including a wrapped one.
type RetryableError struct {
	retryable bool
	cause     error
}

// IsRetryable reports whether the operation that returned the error can be retried.
func (e RetryableError) IsRetryable() bool {
	return e.retryable
}

// Error returns the message of the underlying error.
func (e RetryableError) Error() string {
	if e.cause == nil {
		return "retryable error"
	}
	return e.cause.Error()
}

// Unwrap returns the underlying error.
func (e RetryableError) Unwrap() error {
	return e.cause
}

// MultiKeyError is returned by operations on multiple keys, such as MGet and
// MGetMap, when some keys failed while others succeeded. The results of the keys
// that succeeded are returned along with the MultiKeyError.
type MultiKeyError struct {
	// Errors holds the error of every key that failed, by key.
	Errors map[string]error
}

// Error returns the error messages of the keys that failed, sorted by key.
func (e *MultiKeyError) Error() string {
	errs := e.Unwrap()
	if len(errs) == 1 {
		return errs[0].Error()
	}
	msgs := make([]string, len(errs))
	for i, err := range errs {
		msgs[i] = err.Error()
	}
	return fmt.Sprintf("%d keys failed: %s", len(errs), strings.Join(msgs, "; "))
}

// Unwrap returns the errors of the keys that failed, sorted by key, so errors.Is and
// errors.As match the error of any key.
func (e *MultiKeyError) Unwrap() []error {
	keys := slices.Sorted(maps.Keys(e.Errors))
	errs := make([]error, len(keys))
	for i, key := range keys {
		errs[i] = e.Errors[key]
	}
	return errs
}
