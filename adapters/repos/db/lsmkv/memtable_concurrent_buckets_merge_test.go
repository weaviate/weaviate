package lsmkv

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
	"github.com/weaviate/weaviate/entities/cyclemanager"
)

func TestMemtableConcurrentMerge(t *testing.T) {
	const numKeys = 1000000
	const operationsPerClient = 1000
	const numClients = 10000
	numWorkers := runtime.NumCPU()

	t.Run("single-channel", func(t *testing.T) {
		RunMergeExperiment(t, numKeys, operationsPerClient, numClients, numWorkers, "single-channel")
	})

	t.Run("random", func(t *testing.T) {
		RunMergeExperiment(t, numKeys, operationsPerClient, numClients, numWorkers, "random")
	})

	t.Run("round-robin", func(t *testing.T) {
		RunMergeExperiment(t, numKeys, operationsPerClient, numClients, numWorkers, "round-robin")
	})

	t.Run("hash", func(t *testing.T) {
		RunMergeExperiment(t, numKeys, operationsPerClient, numClients, numWorkers, "hash")
	})
}

func RunMergeExperiment(t *testing.T, numKeys int, operationsPerClient int, numClients int, numWorkers int, workerAssignment string) [][]*roaringset.BinarySearchNode {

	var wgClients sync.WaitGroup
	var wgWorkers sync.WaitGroup
	requestsChannels := make([]chan Request, numWorkers)
	responseChannels := make([]chan []*roaringset.BinarySearchNode, numWorkers)

	operations := generateOperations(numKeys, operationsPerClient, numClients)

	correctOrder, err := createSimpleBucket(operations, t)
	require.Nil(t, err)

	times := Times{}
	startTime := time.Now()

	// Start worker goroutines
	wgWorkers.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		dirName := t.TempDir()
		requestsChannels[i] = make(chan Request)
		responseChannels[i] = make(chan []*roaringset.BinarySearchNode)
		go worker(i, dirName, requestsChannels[i], responseChannels[i], &wgWorkers)
	}

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// Start client goroutines
	wgClients.Add(numClients)
	for i := 0; i < numClients; i++ {
		go client(i, numWorkers, nil, operationsPerClient, requestsChannels, &wgClients, workerAssignment, operations[i])
	}

	wgClients.Wait() // Wait for all clients to finish
	for _, ch := range requestsChannels {
		close(ch)
	}

	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	buckets := make([][]*roaringset.BinarySearchNode, numWorkers)
	for i := 0; i < numWorkers; i++ {
		bucket := <-responseChannels[i]
		buckets[i] = bucket
	}

	wgWorkers.Wait() // Wait for all workers to finish

	times.Copy = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// TODO: merge the buckets and compare to the non-concurrent version

	//dirName := "multi_thread/segment"
	dirName := t.TempDir()
	nodes, err := mergeRoaringSets(buckets, t)

	times.Merge = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	require.Nil(t, err)
	require.Nil(t, writeBucket(nodes, dirName, numKeys))

	times.Shutdown = int(time.Since(startTime).Milliseconds())

	fmt.Println()
	fmt.Println("Concurrent buckets:")
	fmt.Println("\tSetup:", times.Setup)
	fmt.Println("\tInsert:", times.Insert)
	fmt.Println("\tCopy:", times.Copy)
	fmt.Println("\tMerge:", times.Merge)
	fmt.Println("\tBucket shutdown:", times.Shutdown)

	compareTime := time.Now()

	require.True(t, compareBuckets(correctOrder, nodes))

	fmt.Println("\tCompare buckets:", int(time.Since(compareTime).Milliseconds()))

	return buckets
}

func generateOperations(numKeys int, operationsPerClient int, numClients int) [][]*Request {
	keys := make([][]byte, numKeys)
	operations := make([][]*Request, numClients)

	for i := range keys {
		keys[i] = []byte(fmt.Sprintf("key-%04d", i))
	}
	for i := 0; i < numClients; i++ {
		operations[i] = make([]*Request, operationsPerClient)
		keyIndex := rand.Intn(len(keys))
		for j := 0; j < operationsPerClient; j++ {
			operations[i][j] = &Request{key: keys[keyIndex], value: uint64(i*numClients + j)}
			keyIndex = (keyIndex + 1) % len(keys)
		}
	}
	return operations
}

func createSimpleBucket(operations [][]*Request, t *testing.T) ([]*roaringset.BinarySearchNode, error) {

	times := Times{}
	startTime := time.Now()

	ctx := context.Background()
	//dirName := "single_thread"
	dirName := t.TempDir()
	b, _ := NewBucket(ctx, dirName, dirName, logrus.New(), nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		WithStrategy(StrategyRoaringSet))

	times.Setup = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	// transverse operations in column major order
	for i := 0; i < len(operations[0]); i++ {
		for j := 0; j < len(operations); j++ {
			if err := b.RoaringSetAddOne(operations[j][i].key, operations[j][i].value); err != nil {
				return nil, err
			}
		}
	}

	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	nodes := b.active.roaringSet.FlattenInOrder()

	times.Flatten = int(time.Since(startTime).Milliseconds())

	fmt.Println("Single bucket:")
	fmt.Println("\tSetup:", times.Setup)
	fmt.Println("\tInsert:", times.Insert)
	fmt.Println("\tFlatten:", times.Flatten)

	err := b.Shutdown(ctx)
	if err != nil {
		return nil, err
	}

	return nodes, nil
}

func compareBuckets(b1 []*roaringset.BinarySearchNode, b2 []*roaringset.BinarySearchNode) bool {
	if len(b1) != len(b2) {
		return false
	}
	for i := range b1 {
		if !bytes.Equal(b1[i].Key, b2[i].Key) {
			return false
		}
		oldCardinality := b1[i].Value.Additions.GetCardinality()
		b1[i].Value.Additions.And(b2[i].Value.Additions)
		if b1[i].Value.Additions.GetCardinality() != oldCardinality {
			return false
		}

		oldCardinality = b1[i].Value.Deletions.GetCardinality()
		b1[i].Value.Deletions.And(b2[i].Value.Deletions)

		if b1[i].Value.Deletions.GetCardinality() != oldCardinality {
			return false
		}

	}
	return true
}

func mergeRoaringSets(metaNodes [][]*roaringset.BinarySearchNode, t *testing.T) ([]*roaringset.BinarySearchNode, error) {
	numBuckets := len(metaNodes)
	indices := make([]int, numBuckets)
	totalSize := 0
	for i := 0; i < numBuckets; i++ {
		totalSize += len(metaNodes[i])
	}

	flat := make([]*roaringset.BinarySearchNode, totalSize)
	mergedNodesIndex := 0

	for {
		var smallestNode *roaringset.BinarySearchNode
		var smallestNodeIndex int
		for i := 0; i < numBuckets; i++ {
			index := indices[i]
			if index < len(metaNodes[i]) {
				if smallestNode == nil || bytes.Compare(metaNodes[i][index].Key, smallestNode.Key) < 0 {
					smallestNode = metaNodes[i][index]
					smallestNodeIndex = i
				}
			}
		}
		if smallestNode == nil {
			break
		}
		flat[mergedNodesIndex] = smallestNode
		mergedNodesIndex++
		indices[smallestNodeIndex]++
	}

	fmt.Printf("Merged %d nodes into %d nodes", totalSize, mergedNodesIndex)
	return flat, nil
}

func writeBucket(flat []*roaringset.BinarySearchNode, path string, totalSize int) error {
	totalDataLength := totalPayloadSizeRoaringSet(flat)
	header := segmentindex.Header{
		IndexStart:       uint64(totalDataLength + segmentindex.HeaderSize),
		Level:            0,
		Version:          0,
		SecondaryIndices: 0,
		Strategy:         segmentindex.StrategyRoaringSet,
	}

	file, err := os.Create(path + ".db")
	if err != nil {
		return err
	}
	f := bufio.NewWriterSize(file, int(float64(totalSize)*1.3))

	n, err := header.WriteTo(f)
	if err != nil {
		return err
	}
	headerSize := int(n)
	keys := make([]segmentindex.Key, len(flat))

	totalWritten := headerSize
	for i, node := range flat {
		sn, err := roaringset.NewSegmentNode(node.Key, node.Value.Additions,
			node.Value.Deletions)
		if err != nil {
			return fmt.Errorf("create segment node: %w", err)
		}

		ki, err := sn.KeyIndexAndWriteTo(f, totalWritten)
		if err != nil {
			return fmt.Errorf("write node %d: %w", i, err)
		}

		keys[i] = ki
		totalWritten = ki.ValueEnd
	}
	return nil
}
