package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

// Lock contention example written by Parker to demo WVT-40 (multiple threads writing to bucket concurrently)
func TestMemtableLockContention(t *testing.T) {
	const numWorkers = 10000
	const numKeys = 1000000
	const operationsPerWorker = 1000

	ctx := context.Background()
	dirName := t.TempDir()

	b, err := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet))
	require.Nil(t, err)

	defer b.Shutdown(ctx)

	wg := sync.WaitGroup{}
	// create some keys upfront:
	//
	// - If we create one key per goroutine, we spend a ton of time on GC which
	//	 skews results
	// - If we use the same key for every entry, the memtable's BST will never
	//   grow, so we won't need to hold the lock for very long
	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	for i := 0; i < numWorkers; i++ {
		i := i
		wg.Add(1)
		go func() {
			// pick a random start key, so not all routines start with the same key
			keyIndex := rand.Intn(len(keys))
			for j := 0; j < operationsPerWorker; j++ {
				err = b.RoaringSetAddOne(keys[keyIndex], uint64(i*numWorkers+j))
				if err != nil {
					fmt.Printf("err: %v", err)
				}

				keyIndex = (keyIndex + 1) % len(keys)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
