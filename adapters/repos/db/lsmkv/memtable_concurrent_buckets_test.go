package lsmkv

import (
	"context"
	"fmt"
	"hash/fnv"
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
	responseCh := make(chan Response)

	for j := 0; j < operationsPerWorker; j++ {

		requests <- Request{key: keys[keyIndex], value: uint64(i*numWorkers + j), ResponseCh: responseCh}

		err := <-responseCh

		if err.Error != nil {
			fmt.Printf("err: %v", err)
		}

		keyIndex = (keyIndex + 1) % len(keys)
		//fmt.Println("Client", i, "received:", response.Data)
	}
}

func clientRandom(i int, numWorkers int, keys [][]byte, operationsPerWorker int, requests []chan Request, wg *sync.WaitGroup) {
	defer wg.Done()
	keyIndex := rand.Intn(len(keys))
	responseCh := make(chan Response)

	for j := 0; j < operationsPerWorker; j++ {

		workerID := rand.Intn(numWorkers)
		//workerID := 0 // TODO: remove this line to make it random again
		//workerID := i % numWorkers // TODO: remove this line to make it random again
		//workerID := j % numWorkers // TODO: remove this line to make it random again
		requests[workerID] <- Request{key: keys[keyIndex], value: uint64(i*numWorkers + j), ResponseCh: responseCh}

		// TODO: handle errors and ensure output matches non-concurrent version
		err := <-responseCh

		if err.Error != nil {
			fmt.Printf("err: %v", err)
		}

		keyIndex = (keyIndex + 1) % len(keys)
		//fmt.Println("Client", i, "received:", response.Data)
	}
}

func hashKey(key []byte, numWorkers int) int {
	// consider using different hash function like Murmur hash or other hash table friendly hash functions
	hasher := fnv.New32a()
	hasher.Write(key)
	return int(hasher.Sum32()) % numWorkers
}

func clientHash(i int, numWorkers int, keys [][]byte, operationsPerWorker int, requests []chan Request, wg *sync.WaitGroup) {
	defer wg.Done()
	keyIndex := rand.Intn(len(keys))
	responseCh := make(chan Response)

	for j := 0; j < operationsPerWorker; j++ {

		workerID := hashKey(keys[keyIndex], numWorkers)
		requests[workerID] <- Request{key: keys[keyIndex], value: uint64(i*numWorkers + j), ResponseCh: responseCh}

		err := <-responseCh

		if err.Error != nil {
			fmt.Printf("err: %v", err)
		}

		keyIndex = (keyIndex + 1) % len(keys)
		//fmt.Println("Client", i, "received:", response.Data)
	}
}

const numKeys = 1000000
const operationsPerWorker = 1000
const numClients = 10000

// Per-thread bucket write to reduce lock contention of WVT-40 (multiple threads writing to bucket concurrently)
func TestMemtableConcurrentSingle(t *testing.T) {
	// keep same constants as TestMemtableLockContention to make comparison easier

	// sane default? Can be potentially tuned
	numWorkers := runtime.NumCPU()

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup
	requests := make(chan Request, numWorkers)

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

// Per-thread bucket write to reduce lock contention of WVT-40 (multiple threads writing to bucket concurrently)
func TestMemtableConcurrentHash(t *testing.T) {
	// keep same constants as TestMemtableLockContention to make comparison easier

	// sane default? Can be potentially tuned
	numWorkers := runtime.NumCPU()

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup
	requestsChannels := make([]chan Request, numWorkers)

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
		requestsChannels[i] = make(chan Request)
		dirName := t.TempDir()
		go worker(i, dirName, requestsChannels[i], &wgWorkers)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go clientHash(i, numWorkers, keys, operationsPerWorker, requestsChannels, &wgClients)
	}

	wgClients.Wait() // Wait for all clients to finish
	for _, ch := range requestsChannels {
		close(ch)
	}

	wgWorkers.Wait() // Wait for all workers to finish

	times.Insert = int(time.Since(startTime).Milliseconds())
	fmt.Println("Setup:", times.Setup)
	fmt.Println("Insert:", times.Insert)
	fmt.Println("Bucket shutdown:", times.BucketShutdown)

	// TODO: merge the buckets and compare to the non-concurrent version
}

func TestMemtableConcurrentRandom(t *testing.T) {

	// sane default? Can be potentially tuned
	numWorkers := runtime.NumCPU()

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup
	requestsChannels := make([]chan Request, numWorkers)

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
		requestsChannels[i] = make(chan Request)
		dirName := t.TempDir()
		go worker(i, dirName, requestsChannels[i], &wgWorkers)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go clientRandom(i, numWorkers, keys, operationsPerWorker, requestsChannels, &wgClients)
	}

	wgClients.Wait() // Wait for all clients to finish
	for _, ch := range requestsChannels {
		close(ch)
	}

	wgWorkers.Wait() // Wait for all workers to finish

	times.Insert = int(time.Since(startTime).Milliseconds())
	fmt.Println("Setup:", times.Setup)
	fmt.Println("Insert:", times.Insert)
	fmt.Println("Bucket shutdown:", times.BucketShutdown)

	// TODO: merge the buckets and compare to the non-concurrent version
}
