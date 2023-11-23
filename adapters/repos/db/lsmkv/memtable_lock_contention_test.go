package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type Times struct {
	Setup    int
	Insert   int
	Copy     int
	Flatten  int
	Merge    int
	Shutdown int
}

// Lock contention example written by Parker to demo WVT-40 (multiple threads writing to bucket concurrently)
func TestMemtableLockContention(t *testing.T) {
	const numKeys = 1000000
	const operationsPerClient = 1000
	const numClients = 10000

	// Could this be replaced by subtests?
	times := Times{}
	startTime := time.Now()

	ctx := context.Background()
	dirName := t.TempDir()

	b, err := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet))
	require.Nil(t, err)

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

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	for i := 0; i < numClients; i++ {
		i := i
		wg.Add(1)
		go func() {
			// pick a random start key, so not all routines start with the same key
			keyIndex := rand.Intn(len(keys))
			for j := 0; j < operationsPerClient; j++ {
				err = b.RoaringSetAddOne(keys[keyIndex], uint64(i*numClients+j))
				if err != nil {
					fmt.Printf("err: %v", err)
				}

				keyIndex = (keyIndex + 1) % len(keys)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	fmt.Println("Total size:", b.active.Size())
	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	b.Shutdown(ctx)

	times.Shutdown = int(time.Since(startTime).Milliseconds())

	fmt.Println()
	fmt.Println("Setup:", times.Setup)
	fmt.Println("Insert:", times.Insert)
	fmt.Println("Shutdown:", times.Shutdown)
}
