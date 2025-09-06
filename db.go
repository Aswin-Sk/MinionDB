package miniondb

import (
	"errors"

	"github.com/Aswin-Sk/MinionDB/internal/keystore"
	"github.com/Aswin-Sk/MinionDB/internal/logger"
)

type DB struct {
	skv *keystore.ShardedKV
}

// Open creates or opens a MinionDB instance at the given path.
// `shards` controls the number of shard partitions (parallelism).
func Open(path string, shards int) (*DB, error) {
	skv, err := keystore.NewShardedKV(path, shards)
	if err != nil {
		return nil, err
	}
	logger.Logger.Info("MinionDB opened", "path", path, "shards", shards)
	return &DB{skv: skv}, nil
}

// Set stores a value for the given key.
func (db *DB) Set(key string, value []byte) error {
	if db.skv == nil {
		return errors.New("miniondb: db is closed")
	}
	return db.skv.Set(key, value)
}

// Get retrieves the value for a given key.
func (db *DB) Get(key string) ([]byte, bool) {
	if db.skv == nil {
		return nil, false
	}
	return db.skv.Get(key)
}

// Delete removes a key from the database.
func (db *DB) Delete(key string) error {
	if db.skv == nil {
		return errors.New("miniondb: db is closed")
	}
	return db.skv.Delete(key)
}

// Close flushes all WALs, stops background tasks, and closes the DB.
func (db *DB) Close() error {
	if db.skv == nil {
		return errors.New("miniondb: db already closed")
	}
	err := db.skv.Close()
	db.skv = nil
	logger.Logger.Info("MinionDB closed")
	return err
}
