package jobmanager

import (
	"context"
	"log/slog"
)

const contextAttrsKey = "log_params"

func CtxWithValue(ctx context.Context, attrs ...slog.Attr) context.Context {
	oldAttrs := getContextAttrs(ctx)

	return context.WithValue(ctx, contextAttrsKey, append(oldAttrs, attrs...))
}

func getContextAttrs(ctx context.Context) []slog.Attr {
	attrs, ok := ctx.Value(contextAttrsKey).([]slog.Attr)
	if !ok {
		attrs = make([]slog.Attr, 0)
	}

	return attrs
}
