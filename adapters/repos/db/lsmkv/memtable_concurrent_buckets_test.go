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

func hashKey(key []byte, numWorkers int) int {
	// consider using different hash function like Murmur hash or other hash table friendly hash functions
	hasher := fnv.New32a()
	hasher.Write(key)
	return int(hasher.Sum32()) % numWorkers
}

func client(i int, numWorkers int, keys [][]byte, operationsPerWorker int, requests []chan Request, wg *sync.WaitGroup, workerAssignment string) {
	defer wg.Done()
	keyIndex := rand.Intn(len(keys))
	responseCh := make(chan Response)

	for j := 0; j < operationsPerWorker; j++ {

		workerID := 0 // TODO: remove this line to make it random again
		if workerAssignment == "single-channel" {
			workerID = 0
		} else if workerAssignment == "random" {
			workerID = rand.Intn(numWorkers)
		} else if workerAssignment == "round-robin" {
			workerID = i % numWorkers
		} else if workerAssignment == "hash" {
			workerID = hashKey(keys[keyIndex], numWorkers)
		} else {
			panic("invalid worker assignment")
		}

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

func RunExperiment(t *testing.T, numKeys int, operationsPerWorker int, numClients int, numWorkers int, workerAssignment string) {

	numChannels := numWorkers
	if workerAssignment == "single-channel" {
		numChannels = 1
	}

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup

	requestsChannels := make([]chan Request, numChannels)

	keys := make([][]byte, numKeys)
	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}

	for i := 0; i < numChannels; i++ {
		requestsChannels[i] = make(chan Request)
	}

	// Start worker goroutines
	wgWorkers.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		dirName := t.TempDir()
		go worker(i, dirName, requestsChannels[i%numChannels], &wgWorkers)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go client(i, numWorkers, keys, operationsPerWorker, requestsChannels, &wgClients, workerAssignment)
	}

	wgClients.Wait() // Wait for all clients to finish
	for _, ch := range requestsChannels {
		close(ch)
	}

	wgWorkers.Wait() // Wait for all workers to finish

	times.Insert = int(time.Since(startTime).Milliseconds())
	fmt.Println("Setup:", times.Setup)
	fmt.Println("Insert:", times.Insert)
	fmt.Println("Bucket shutdown:", times.Shutdown)
}

func TestMemtableConcurrent(t *testing.T) {
	const numKeys = 1000000
	const operationsPerWorker = 1000
	const numClients = 10000
	numWorkers := runtime.NumCPU()

	t.Run("single-channel", func(t *testing.T) {
		RunExperiment(t, numKeys, operationsPerWorker, numClients, numWorkers, "single-channel")
	})

	t.Run("random", func(t *testing.T) {
		RunExperiment(t, numKeys, operationsPerWorker, numClients, numWorkers, "random")
	})

	t.Run("round-robin", func(t *testing.T) {
		RunExperiment(t, numKeys, operationsPerWorker, numClients, numWorkers, "round-robin")
	})

	t.Run("hash", func(t *testing.T) {
		RunExperiment(t, numKeys, operationsPerWorker, numClients, numWorkers, "hash")
	})
}
