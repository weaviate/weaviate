package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type Request struct {
	key        []byte
	value      uint64
	ResponseCh chan Response
}

// TODO: ensure errors are handled and output is the same as the non-concurrent version
type Response struct {
	Data string
}

// Worker goroutine: adds to the RoaringSet
// TODO: wrap this code to support an interface similar to the non-concurrent version on Bucket, instead of this worker model
func worker(id int, b *Bucket, requests <-chan Request) {
	for req := range requests {
		// Do the work
		b.RoaringSetAddOne(req.key, req.value)
		// Send a response back
		req.ResponseCh <- Response{Data: "Processed by worker " + fmt.Sprint(id)}
	}
}

func client(i int, numWorkers int, keys [][]byte, operationsPerWorker int, requests chan<- Request, wg *sync.WaitGroup) {
	defer wg.Done()
	keyIndex := rand.Intn(len(keys))

	for j := 0; j < operationsPerWorker; j++ {

		responseCh := make(chan Response)
		requests <- Request{key: keys[keyIndex], value: uint64(i*numWorkers + j), ResponseCh: responseCh}

		// TODO: handle errors and ensure output matches non-concurrent version
		<-responseCh

		keyIndex = (keyIndex + 1) % len(keys)
		//fmt.Println("Client", i, "received:", response.Data)
	}
}

// Per-thread bucket write to reduce lock contention of WVT-40 (multiple threads writing to bucket concurrently)
func TestMemtableConcurrent(t *testing.T) {
	// keep same constants as TestMemtableLockContention to make comparison easier
	const numWorkers = 10000
	const numKeys = 1000000
	const operationsPerWorker = 1000

	// sane default? Can be potentially tuned
	numClients := runtime.NumCPU()

	var wg sync.WaitGroup
	requests := make(chan Request, numClients)

	// Not sure if this is the best way of keeping the buckets around, but it works
	// This is being done at Bucket level instead of Memtable level for comparison reasons, but the same could be done at Memtable level, as the RoaringSetAddOne and other RoaringTable calls are basically a passthrough to the Memtable
	buckets := make([]*Bucket, numWorkers)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		ctx := context.Background()
		dirName := t.TempDir()

		b, err := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
			cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			WithStrategy(StrategyRoaringSet))
		require.Nil(t, err)
		buckets[i] = b

		go worker(i, b, requests)
	}

	// Start client goroutines
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go client(i, numWorkers, keys, operationsPerWorker, requests, &wg)
	}

	wg.Wait()       // Wait for all clients to finish
	close(requests) // Close the requests channel

	for _, b := range buckets {
		b.Shutdown(context.Background())
	}

	// TODO: merge the buckets and compare to the non-concurrent version
}
