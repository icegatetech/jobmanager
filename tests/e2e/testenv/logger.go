package testenv

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"testing"

	"github.com/icegatetech/jobmanager"
)

const contextAttrsKey = "log_params"

func getContextAttrs(ctx context.Context) []slog.Attr {
	attrs, ok := ctx.Value(contextAttrsKey).([]slog.Attr)
	if !ok {
		attrs = make([]slog.Attr, 0)
	}
	return attrs
}

// TestLogger implements jobmanager.Logger for testing
type TestLogger struct {
	t *testing.T
}

// NewTestLogger creates a logger that writes to testing.T
func NewTestLogger(t *testing.T) *TestLogger {
	return &TestLogger{t: t}
}

func (l *TestLogger) formatAttrs(args ...slog.Attr) string {
	if len(args) == 0 {
		return ""
	}
	result := "{"
	for _, attr := range args {
		result += fmt.Sprintf(" %s=%v;", attr.Key, attr.Value)
	}
	return result + "}"
}

func (l *TestLogger) Info(msg string, args ...slog.Attr) {
	l.t.Log("[INFO]", msg+l.formatAttrs(args...))
}

func (l *TestLogger) Error(msg string, args ...slog.Attr) {
	l.t.Error("[ERROR]", msg+l.formatAttrs(args...))
}

func (l *TestLogger) Debug(msg string, args ...slog.Attr) {
	l.t.Log("[DEBUG]", msg+l.formatAttrs(args...))
}

func (l *TestLogger) Warn(msg string, args ...slog.Attr) {
	l.t.Log("[WARN]", msg+l.formatAttrs(args...))
}

func (l *TestLogger) InfoContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.t.Log("[INFO]", msg+l.formatAttrs(slices.Concat(args, getContextAttrs(ctx))...))
}

func (l *TestLogger) ErrorContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.t.Error("[ERROR]", msg+l.formatAttrs(slices.Concat(args, getContextAttrs(ctx))...))
}

func (l *TestLogger) DebugContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.t.Log("[DEBUG]", msg+l.formatAttrs(slices.Concat(args, getContextAttrs(ctx))...))
}

func (l *TestLogger) WarnContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.t.Log("[WARN]", msg+l.formatAttrs(slices.Concat(args, getContextAttrs(ctx))...))
}

func (l *TestLogger) With(args ...slog.Attr) jobmanager.Logger {
	return l
}

func (l *TestLogger) CtxWithLogAttrs(ctx context.Context, args ...slog.Attr) context.Context {
	return jobmanager.CtxWithValue(ctx, args...)
}
