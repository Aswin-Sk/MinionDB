package main

import (
	"fmt"
	"sync"
	"time"

	"MinionDB/internal/keystore"
)

func main() {
	db, err := keystore.NewShardedKV("C:\\Users\\HP\\Documents\\applications\\MinionDB\\MinionDB\\data", 8)
	if err != nil {
		panic(err)
	}
	numOps := 10000
	numWorkers := 16

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOps/numWorkers; j++ {
				key := fmt.Sprintf("key-%d-%d", workerID, j)
				value := []byte(fmt.Sprintf("value-%d-%d", workerID, j))
				db.Set(key, value)
				if j%5 == 0 {
					db.Get(key)
				}
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)
	opsPerSec := float64(numOps) / elapsed.Seconds()
	fmt.Printf("Completed %d ops in %.2f seconds (%.2f ops/sec)\n", numOps, elapsed.Seconds(), opsPerSec)
}
