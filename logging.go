package streamrpc

import "context"

type Logger interface {
	Infof(fmt string, args ...interface{})
	Errorf(fmt string, args ...interface{})
}

type discardLogger struct{}

func (discardLogger) Errorf(fmt string, args ...interface{}) {}
func (discardLogger) Infof(fmt string, args ...interface{})  {}

type contextKey int

const (
	contextKeyLogger contextKey = iota
)

func ContextWithLogger(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, contextKeyLogger, l)
}

func logger(ctx context.Context) Logger {
	logger, ok := ctx.Value(contextKeyLogger).(Logger)
	if !ok {
		return discardLogger{}
	}
	return logger
}
