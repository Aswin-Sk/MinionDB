package benchmarks

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"MinionDB/internal/keystore"
)

func BenchmarkMiniKVLoad(b *testing.B) {
	db, err := keystore.NewShardedKV("testshards", 8)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	numWorkers := 16
	numOps := b.N

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	start := time.Now()
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := workerID; j < numOps; j += numWorkers {
				key := fmt.Sprintf("key-%d", j)
				val := []byte(fmt.Sprintf("value-%d", j))
				_ = db.Set(key, val)
				if j%10 == 0 {
					db.Get(key)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)

	opsPerSec := float64(numOps) / elapsed.Seconds()
	b.ReportMetric(opsPerSec, "ops/s")
}
