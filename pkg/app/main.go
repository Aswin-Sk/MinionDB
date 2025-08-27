package main

import (
	"MinionDB/internal/logger"
	"log/slog"
)

func main() {
	logger.InitLogger(slog.LevelInfo)
}
