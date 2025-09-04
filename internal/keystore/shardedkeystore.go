package keystore

import (
	"fmt"
	"hash/fnv"
	"path/filepath"
)

type ShardedKV struct {
	shards        []*MiniKV
	n             int
	baseDirectory string
}

func NewShardedKV(path string, shards int) (*ShardedKV, error) {
	skv := &ShardedKV{
		n:             shards,
		baseDirectory: path,
	}
	for i := range shards {
		kv, err := open(filepath.Join(path, fmt.Sprintf("shard-%d", i)))
		if err != nil {
			return nil, err
		}
		skv.shards = append(skv.shards, kv)
	}
	return skv, nil
}

func (skv *ShardedKV) getShard(key string) *MiniKV {
	h := fnv.New32a()
	h.Write([]byte(key))
	return skv.shards[int(h.Sum32())%skv.n]
}

func (skv *ShardedKV) Set(key string, val []byte) error {
	return skv.getShard(key).Set(key, val)
}

func (skv *ShardedKV) Get(key string) ([]byte, bool) {
	return skv.getShard(key).Get(key)
}

func (skv *ShardedKV) Delete(key string) error {
	return skv.getShard(key).Delete(key)
}

func (skv *ShardedKV) Close() error {
	for _, s := range skv.shards {
		err := s.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (skv *ShardedKV) Compact(basePath string) error {
	for i, shard := range skv.shards {
		if err := shard.Compact(filepath.Join(basePath, fmt.Sprintf("shard-%d.wal", i))); err != nil {
			return err
		}
	}
	return nil
}
