package socketio

import (
	"log/slog"
	"os"

	"github.com/kingecg/gosocketio/engineio"
)

// defaultLogger logs info/warn lines to stderr.
var defaultLogger engineio.Logger = slogLogger{l: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
	Level: slog.LevelInfo,
}))}

// NopLogger discards all log output.
var NopLogger engineio.Logger = slogLogger{l: slog.New(slog.NewTextHandler(ioDiscard{}, nil))}

type slogLogger struct {
	l *slog.Logger
}

func (s slogLogger) Debugf(format string, args ...any) { s.l.Debug(format, args...) }
func (s slogLogger) Infof(format string, args ...any)  { s.l.Info(format, args...) }
func (s slogLogger) Warnf(format string, args ...any)  { s.l.Warn(format, args...) }
func (s slogLogger) Errorf(format string, args ...any) { s.l.Error(format, args...) }

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }
