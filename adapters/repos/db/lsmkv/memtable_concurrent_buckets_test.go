package lsmkv

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type Request struct {
	key        []byte
	value      uint64
	ResponseCh chan Response
}

// TODO: ensure errors are handled and output is the same as the non-concurrent version
type Response struct {
	Error error
}

// Worker goroutine: adds to the RoaringSet
// TODO: wrap this code to support an interface similar to the non-concurrent version on Bucket, instead of this worker model
func worker(id int, dirName string, requests <-chan Request, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	b, _ := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet))

	for req := range requests {
		// Do the work
		err := b.RoaringSetAddOne(req.key, req.value)
		// Send a response back
		req.ResponseCh <- Response{Error: err}

	}

	// Perform any cleanup here
	b.Shutdown(context.Background())
	fmt.Println("Worker", id, "size:", b.active.size)
}

func client(i int, numWorkers int, keys [][]byte, operationsPerWorker int, requests chan<- Request, wg *sync.WaitGroup) {
	defer wg.Done()
	keyIndex := rand.Intn(len(keys))

	for j := 0; j < operationsPerWorker; j++ {

		responseCh := make(chan Response)
		requests <- Request{key: keys[keyIndex], value: uint64(i*numWorkers + j), ResponseCh: responseCh}

		// TODO: handle errors and ensure output matches non-concurrent version
		err := <-responseCh

		if err.Error != nil {
			fmt.Printf("err: %v", err)
		}

		keyIndex = (keyIndex + 1) % len(keys)
		//fmt.Println("Client", i, "received:", response.Data)
	}
}

// Per-thread bucket write to reduce lock contention of WVT-40 (multiple threads writing to bucket concurrently)
func TestMemtableConcurrent(t *testing.T) {
	// keep same constants as TestMemtableLockContention to make comparison easier
	const numKeys = 1000000
	const operationsPerWorker = 1000
	const numClients = 10000

	// sane default? Can be potentially tuned
	numWorkers := runtime.NumCPU()

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup
	requests := make(chan Request, numClients)

	// Not sure if this is the best way of keeping the buckets around, but it works
	// This is being done at Bucket level instead of Memtable level for comparison reasons, but the same could be done at Memtable level, as the RoaringSetAddOne and other RoaringTable calls are basically a passthrough to the Memtable
	//buckets := make([]*Bucket, numWorkers)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}

	// Start worker goroutines
	wgWorkers.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		dirName := t.TempDir()
		go worker(i, dirName, requests, &wgWorkers)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go client(i, numWorkers, keys, operationsPerWorker, requests, &wgClients)
	}

	wgClients.Wait() // Wait for all clients to finish
	close(requests)  // Close the requests channel

	wgWorkers.Wait() // Wait for all workers to finish

	times.Insert = int(time.Since(startTime).Milliseconds())
	fmt.Println("Setup:", times.Setup)
	fmt.Println("Insert:", times.Insert)
	fmt.Println("Bucket shutdown:", times.BucketShutdown)

	// TODO: merge the buckets and compare to the non-concurrent version
}
