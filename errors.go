package cache

import (
	"bytes"
	"fmt"
	"strings"
)

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

type CommandError struct {
	Command string
	Key     string
	Err     error
}

func (r CommandError) Error() string {
	return fmt.Sprintf("%s %s: %s", r.Command, r.Key, r.Err)
}

type CommandErrors []CommandError

func (r CommandErrors) Error() string {
	buff := bytes.NewBufferString("")

	for i := 0; i < len(r); i++ {
		buff.WriteString(r[i].Error())
		buff.WriteString("\n")
	}

	return strings.TrimSpace(buff.String())
}

func (r CommandErrors) Keys() []string {
	keys := make([]string, len(r))
	for i, e := range r {
		keys[i] = e.Key
	}
	return keys
}
