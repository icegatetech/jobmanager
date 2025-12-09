package jobmanager

import (
	"context"
	"log/slog"
	"slices"
)

// Logger interface for logging
type Logger interface {
	Debug(msg string, args ...slog.Attr)
	DebugContext(ctx context.Context, msg string, args ...slog.Attr)
	Info(msg string, args ...slog.Attr)
	InfoContext(ctx context.Context, msg string, args ...slog.Attr)
	Warn(msg string, args ...slog.Attr)
	WarnContext(ctx context.Context, msg string, args ...slog.Attr)
	Error(msg string, args ...slog.Attr)
	ErrorContext(ctx context.Context, msg string, args ...slog.Attr)
	CtxWithLogAttrs(ctx context.Context, attrs ...slog.Attr) context.Context
	With(args ...slog.Attr) Logger
}

type LoggerStub struct{}

func (LoggerStub) Debug(msg string, args ...slog.Attr)                             {}
func (LoggerStub) DebugContext(ctx context.Context, msg string, args ...slog.Attr) {}
func (LoggerStub) Info(msg string, args ...slog.Attr)                              {}
func (LoggerStub) InfoContext(ctx context.Context, msg string, args ...slog.Attr)  {}
func (LoggerStub) Warn(msg string, args ...slog.Attr)                              {}
func (LoggerStub) WarnContext(ctx context.Context, msg string, args ...slog.Attr)  {}
func (LoggerStub) Error(msg string, args ...slog.Attr)                             {}
func (LoggerStub) ErrorContext(ctx context.Context, msg string, args ...slog.Attr) {}
func (LoggerStub) CtxWithLogAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	return context.TODO()
}
func (l LoggerStub) With(args ...slog.Attr) Logger {
	return l
}

type slogLogger struct {
	logger *slog.Logger
}

func NewSlogLogger(logger *slog.Logger) Logger {
	return &slogLogger{logger: logger}
}

func (l *slogLogger) Debug(msg string, args ...slog.Attr) {
	l.logger.LogAttrs(context.Background(), slog.LevelDebug, msg, args...)
}

func (l *slogLogger) DebugContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.logger.LogAttrs(ctx, slog.LevelDebug, msg, slices.Concat(args, getContextAttrs(ctx))...)
}

func (l *slogLogger) Info(msg string, args ...slog.Attr) {
	l.logger.LogAttrs(context.Background(), slog.LevelInfo, msg, args...)
}

func (l *slogLogger) InfoContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.logger.LogAttrs(ctx, slog.LevelInfo, msg, slices.Concat(args, getContextAttrs(ctx))...)
}

func (l *slogLogger) Warn(msg string, args ...slog.Attr) {
	l.logger.LogAttrs(context.Background(), slog.LevelWarn, msg, args...)
}

func (l *slogLogger) WarnContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.logger.LogAttrs(ctx, slog.LevelWarn, msg, slices.Concat(args, getContextAttrs(ctx))...)
}

func (l *slogLogger) Error(msg string, args ...slog.Attr) {
	l.logger.LogAttrs(context.Background(), slog.LevelError, msg, args...)
}

func (l *slogLogger) ErrorContext(ctx context.Context, msg string, args ...slog.Attr) {
	l.logger.LogAttrs(ctx, slog.LevelError, msg, slices.Concat(args, getContextAttrs(ctx))...)
}

func (l *slogLogger) CtxWithLogAttrs(ctx context.Context, attrs ...slog.Attr) context.Context {
	return CtxWithValue(ctx, attrs...)
}

func (l *slogLogger) With(args ...slog.Attr) Logger {
	attrs := make([]any, 0, len(args)*2)
	for _, attr := range args {
		attrs = append(attrs, attr.Key, attr.Value)
	}
	return &slogLogger{logger: l.logger.With(attrs...)}
}
