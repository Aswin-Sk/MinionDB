package main

import (
	"MinionDB/internal/keystore"
	"MinionDB/internal/logger"
	"log/slog"

	"github.com/gin-gonic/gin"
)

var db *keystore.MiniKV

func main() {
	logger.InitLogger(slog.LevelInfo)
	// Open DB
	var err error
	db, err = keystore.Open("minikv.data")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Setup Gin
	r := gin.Default()

	r.GET("/get/:key", handleGet)
	r.POST("/set", handleSet)
	r.DELETE("/delete/:key", handleDelete)
	r.POST("/compact", handleCompact)

	r.Run(":8080")
}
