package streamrpc

import "context"

type Logger interface {
	Printf(fmt string, args... interface{})
}

type discardLogger struct {}

func (discardLogger) Printf(fmt string, args... interface{}) {}

type contextKey int

const ContextKeyLogger contextKey = 0

func logger(ctx context.Context) Logger {
	logger, ok := ctx.Value(ContextKeyLogger).(Logger)
	if !ok {
		return discardLogger{}
	}
	return logger
}
