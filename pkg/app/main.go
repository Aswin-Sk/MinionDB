package app

import (
	"MinionDB/internal/keystore"
	"MinionDB/internal/logger"
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

var db *keystore.ShardedKV
var path = "C:\\Users\\HP\\Documents\\applications\\MinionDB\\MinionDB\\data"

func Start() {
	logger.InitLogger(slog.LevelInfo)

	var err error
	db, err = keystore.NewShardedKV(path, 16)
	if err != nil {
		panic(err)
	}

	stopChannel := make(chan struct{})
	go db.Compact(path)

	r := gin.Default()
	r.GET("/get/:key", handleGet)
	r.POST("/set", handleSet)
	r.DELETE("/delete/:key", handleDelete)

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Logger.Error("server error", "error", err)
		}
	}()

	ShutdownServer(srv, stopChannel)

}

func ShutdownServer(srv *http.Server, stopChannel chan struct{}) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Logger.Info("Shutting down server...")

	close(stopChannel)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Logger.Error("Server forced to shutdown", "error", err)
	}

	if err := db.Close(); err != nil {
		logger.Logger.Error("DB close error", "error", err)
	}

	logger.Logger.Info("Server exited")
}
