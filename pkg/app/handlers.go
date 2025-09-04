package app

import (
	"MinionDB/internal/keystore"
	"net/http"

	"github.com/gin-gonic/gin"
)

func handleGet(c *gin.Context) {
	key := c.Param("key")
	val, err := db.Get(key)
	if !err {
		c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}
	valString := string(val)
	if valString == keystore.Tombstone {
		c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"key": key, "value": string(val)})
}

type setRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func handleSet(c *gin.Context) {
	var req setRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid payload"})
		return
	}

	if err := db.Set(req.Key, []byte(req.Value)); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func handleDelete(c *gin.Context) {
	key := c.Param("key")
	if err := db.Delete(key); err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "key not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
