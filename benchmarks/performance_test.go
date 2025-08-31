package benchmarks

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"

	"MinionDB/internal/keystore"
)

func setupDB() *keystore.MiniKV {
	os.Remove("testdb.data")
	db, err := keystore.Open("testdb.data")
	if err != nil {
		panic(err)
	}

	for i := 0; i < 1000; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(rand.Int())
		if err := db.Set(key, []byte(value)); err != nil {
			panic(err)
		}
	}
	return db
}

func BenchmarkConcurrentReads(b *testing.B) {
	db := setupDB()
	defer db.Close()

	b.ResetTimer()

	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			value := "value" + strconv.Itoa(rand.Intn(1000+1))
			if _, err := db.Get(fmt.Sprintf("key%s", []byte(value))); err {
				b.Error(fmt.Errorf("Get failed"))
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkDeletes(b *testing.B) {
	db := setupDB()
	defer db.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i%1000)
		if err := db.Delete(key); err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkCompaction(b *testing.B) {
	db := setupDB()
	defer db.Close()

	for i := 0; i < 500; i++ {
		_ = db.Delete("key" + strconv.Itoa(i))
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := db.Compact(); err != nil {
			b.Error(err)
		}
	}
}
