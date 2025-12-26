package cache

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

type nopLogger struct{}

func (n nopLogger) Debug(msg string, args ...any) {}

func (n nopLogger) Info(msg string, args ...any) {}

func (n nopLogger) Warn(msg string, args ...any) {}

func (n nopLogger) Error(msg string, args ...any) {}
