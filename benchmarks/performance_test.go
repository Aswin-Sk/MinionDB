package benchmarks

import (
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/Aswin-Sk/MinionDB/internal/keystore"
)

// setupShardedDB creates a ShardedKV with N shards and pre-populates each shard.
func setupShardedDB(basePath string, numShards int, keysPerShard int) (*keystore.ShardedKV, error) {
	os.RemoveAll(basePath)
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return nil, err
	}

	skv, err := keystore.NewShardedKV(basePath, numShards)
	if err != nil {
		return nil, err
	}

	for i := 0; i < keysPerShard*numShards; i++ {
		key := "key" + strconv.Itoa(i)
		val := []byte("value" + strconv.Itoa(rand.Int()))
		if err := skv.Set(key, val); err != nil {
			return nil, err
		}
	}

	return skv, nil
}

// BenchmarkShardedReads benchmarks concurrent reads across shards.
func BenchmarkShardedReads(b *testing.B) {
	basePath := "testshards"
	numShards := 8
	keysPerShard := 1000

	skv, err := setupShardedDB(basePath, numShards, keysPerShard)
	if err != nil {
		b.Fatalf("failed to setup DB: %v", err)
	}
	defer skv.Close()

	const numGoroutines = 8

	var wg sync.WaitGroup
	ch := make(chan int, b.N)
	for i := 0; b.Loop(); i++ {
		ch <- i
	}
	close(ch)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ch {
				idx := rand.Intn(numShards * keysPerShard)
				key := "key" + strconv.Itoa(idx)
				if _, ok := skv.Get(key); !ok {
					b.Errorf("Get failed for key %s", key)
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkShardedWrites benchmarks concurrent writes across shards.
func BenchmarkShardedWrites(b *testing.B) {
	basePath := "testshards"
	numShards := 8
	keysPerShard := 1000

	skv, err := setupShardedDB(basePath, numShards, keysPerShard)
	if err != nil {
		b.Fatalf("failed to setup DB: %v", err)
	}
	defer skv.Close()

	const numGoroutines = 8
	b.ResetTimer()

	var wg sync.WaitGroup
	ch := make(chan int, b.N)
	for i := 0; i < b.N; i++ {
		ch <- i
	}
	close(ch)

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range ch {
				idx := rand.Intn(numShards * keysPerShard)
				key := "key" + strconv.Itoa(idx)
				val := []byte("value" + strconv.Itoa(rand.Int()))
				if err := skv.Set(key, val); err != nil {
					b.Errorf("Set failed for key %s: %v", key, err)
				}
			}
		}()
	}
	wg.Wait()
}

// BenchmarkShardedDeletes benchmarks deletes across shards.
func BenchmarkShardedDeletes(b *testing.B) {
	basePath := "testshards"
	numShards := 8
	keysPerShard := 1000

	skv, err := setupShardedDB(basePath, numShards, keysPerShard)
	if err != nil {
		b.Fatalf("failed to setup DB: %v", err)
	}
	defer skv.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i%(numShards*keysPerShard))
		if err := skv.Delete(key); err != nil {
			b.Errorf("Delete failed for key %s: %v", key, err)
		}
	}
}

// BenchmarkShardedCompaction benchmarks compaction across shards.
func BenchmarkShardedCompaction(b *testing.B) {
	basePath := "testshards"
	numShards := 8
	keysPerShard := 1000

	skv, err := setupShardedDB(basePath, numShards, keysPerShard)
	if err != nil {
		b.Fatalf("failed to setup DB: %v", err)
	}
	defer skv.Close()

	// Delete some keys to make compaction meaningful
	for i := 0; i < 500; i++ {
		_ = skv.Delete("key" + strconv.Itoa(i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := skv.Compact(basePath); err != nil {
			b.Errorf("Compaction failed: %v", err)
		}
	}
}
