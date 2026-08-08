package engineio

import (
	"log/slog"
	"os"
)

// Logger is the logging interface used across the package.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

// slogLogger adapts log/slog to the Logger interface.
type slogLogger struct {
	l *slog.Logger
}

func (s slogLogger) Debugf(format string, args ...any) { s.l.Debug(format, args...) }
func (s slogLogger) Infof(format string, args ...any)  { s.l.Info(format, args...) }
func (s slogLogger) Warnf(format string, args ...any)  { s.l.Warn(format, args...) }
func (s slogLogger) Errorf(format string, args ...any) { s.l.Error(format, args...) }

// defaultLogger logs debug/warn lines to stderr.
var defaultLogger Logger = slogLogger{l: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))}

// NopLogger discards all log output.
var NopLogger Logger = slogLogger{l: slog.New(slog.NewTextHandler(ioDiscard{}, nil))}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
