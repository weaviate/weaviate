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
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type bucketFunc func(*Bucket, []byte, uint64) error

type Request struct {
	key        []byte
	value      uint64
	operation  bucketFunc
	ResponseCh chan Response
}

type Response struct {
	Error error
}

func TestMemtableConcurrentInsert(t *testing.T) {
	const numKeys = 1000000
	const operationsPerClient = 1000
	const numClients = 10000
	numWorkers := runtime.NumCPU()

	operations := generateOperations(numKeys, operationsPerClient, numClients)

	t.Run("single-channel", func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, "single-channel", operations)
	})

	t.Run("random", func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, "random", operations)
	})

	t.Run("round-robin", func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, "round-robin", operations)
	})

	t.Run("hash", func(t *testing.T) {
		RunExperiment(t, numClients, numWorkers, "hash", operations)
	})
}

func RunExperiment(t *testing.T, numClients int, numWorkers int, workerAssignment string, operations [][]*Request) ([][]*roaringset.BinarySearchNode, Times) {

	numChannels := numWorkers
	if workerAssignment == "single-channel" {
		numChannels = 1
	}

	times := Times{}
	startTime := time.Now()

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup

	requestsChannels := make([]chan Request, numChannels)
	responseChannels := make([]chan []*roaringset.BinarySearchNode, numWorkers)

	for i := 0; i < numChannels; i++ {
		requestsChannels[i] = make(chan Request)
	}

	// Start worker goroutines
	wgWorkers.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		dirName := t.TempDir()
		responseChannels[i] = make(chan []*roaringset.BinarySearchNode)
		go worker(i, dirName, requestsChannels[i%numChannels], responseChannels[i], &wgWorkers)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go client(i, numWorkers, operations[i], requestsChannels, &wgClients, workerAssignment)
	}

	wgClients.Wait() // Wait for all clients to finish
	for _, ch := range requestsChannels {
		close(ch)
	}

	buckets := make([][]*roaringset.BinarySearchNode, numWorkers)
	for i := 0; i < numWorkers; i++ {
		bucket := <-responseChannels[i]
		buckets[i] = bucket
	}

	wgWorkers.Wait() // Wait for all workers to finish

	times.Insert = int(time.Since(startTime).Milliseconds())
	fmt.Println("Setup:", times.Setup)
	fmt.Println("Insert:", times.Insert)

	return buckets, times
}

func generateOperations(numKeys int, operationsPerClient int, numClients int) [][]*Request {
	keys := make([][]byte, numKeys)
	operations := make([][]*Request, numClients)
	operationsPerKey := make(map[string]int)

	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	for i := 0; i < numClients; i++ {
		operations[i] = make([]*Request, operationsPerClient)
		keyIndex := rand.Intn(len(keys))
		for j := 0; j < operationsPerClient; j++ {
			var funct bucketFunc

			if rand.Intn(2) == 0 {
				funct = func(b *Bucket, key []byte, value uint64) error { return b.RoaringSetRemoveOne(key, value) }
			} else {
				funct = func(b *Bucket, key []byte, value uint64) error { return b.RoaringSetRemoveOne(key, value) }
			}
			operations[i][j] = &Request{key: keys[keyIndex], value: uint64(i*numClients + j), operation: funct}
			operationsPerKey[string(keys[keyIndex])]++
			keyIndex = (keyIndex + 1) % len(keys)
		}
	}
	fmt.Println("Operations:", numClients*operationsPerClient, "Unique key count:", len(operationsPerKey))
	return operations
}

// Worker goroutine: adds to the RoaringSet
// TODO: wrap this code to support an interface similar to the non-concurrent version on Bucket, instead of this worker model
func worker(id int, dirName string, requests <-chan Request, response chan<- []*roaringset.BinarySearchNode, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	// One bucket per worker, initialization is done in the worker thread
	b, _ := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet))

	for req := range requests {
		err := req.operation(b, req.key, req.value)
		req.ResponseCh <- Response{Error: err}

	}
	// Grab the nodes and send them back for further merging
	nodes := b.active.roaringSet.FlattenInOrder()
	response <- nodes
	close(response)
	fmt.Println("Worker", id, "size:", len(nodes))

}

func hashKey(key []byte, numWorkers int) int {
	// consider using different hash function like Murmur hash or other hash table friendly hash functions
	hasher := fnv.New32a()
	hasher.Write(key)
	return int(hasher.Sum32()) % numWorkers
}

func client(i int, numWorkers int, threadOperations []*Request, requests []chan Request, wg *sync.WaitGroup, workerAssignment string) {
	defer wg.Done()

	responseCh := make(chan Response)

	for j := 0; j < len(threadOperations); j++ {

		workerID := 0
		if workerAssignment == "single-channel" {
			workerID = 0
		} else if workerAssignment == "random" {
			workerID = rand.Intn(numWorkers)
		} else if workerAssignment == "round-robin" {
			workerID = i % numWorkers
		} else if workerAssignment == "hash" {
			workerID = hashKey(threadOperations[j].key, numWorkers)
		} else {
			panic("invalid worker assignment")
		}

		operationWithChannel := Request{key: threadOperations[j].key, value: threadOperations[j].value, operation: threadOperations[j].operation, ResponseCh: responseCh}
		requests[workerID] <- operationWithChannel

		err := <-responseCh

		if err.Error != nil {
			fmt.Printf("err: %v", err)
		}
	}
}
