package logger

import (
	"io"
	"log/slog"
	"os"
)

var Logger *slog.Logger

func InitLogger(level slog.Level) {
	var writer io.Writer = os.Stderr
	Logger = slog.New(slog.NewTextHandler(writer, &slog.HandlerOptions{
		Level: level,
	}))
}
