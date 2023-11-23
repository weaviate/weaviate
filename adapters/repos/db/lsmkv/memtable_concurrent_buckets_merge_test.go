package lsmkv

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"os"
	"runtime"
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

	operations := generateOperations(numKeys, operationsPerClient, numClients)
	correctOrder, err := createSimpleBucket(operations, t)
	require.Nil(t, err)

	t.Run("single-channel", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "single-channel", operations, correctOrder)
	})

	t.Run("random", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "random", operations, correctOrder)
	})

	t.Run("round-robin", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "round-robin", operations, correctOrder)
	})

	t.Run("hash", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "hash", operations, correctOrder)
	})
}

func RunMergeExperiment(t *testing.T, numClients int, numWorkers int, workerAssignment string, operations [][]*Request, correctOrder []*roaringset.BinarySearchNode) [][]*roaringset.BinarySearchNode {

	buckets, times := RunExperiment(t, numClients, numWorkers, workerAssignment, operations)

	// TODO: merge the buckets and compare to the non-concurrent version

	//dirName := "multi_thread/segment"
	dirName := t.TempDir()
	startTime := time.Now()
	nodes, err := mergeRoaringSets(buckets, t)

	times.Merge = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	require.Nil(t, err)
	require.Nil(t, writeBucket(nodes, dirName))

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

	maxOperations := 0
	for _, op := range operations {
		if len(op) > maxOperations {
			maxOperations = len(op)
		}
	}
	// transverse operations in column major order
	for i := 0; i < maxOperations; i++ {
		for j := 0; j < len(operations); j++ {
			if i >= len(operations[j]) {
				continue
			}
			err := operations[j][i].operation(b, operations[j][i].key, operations[j][i].value)
			require.Nil(t, err)
		}
	}

	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	nodes := b.active.RoaringSet().FlattenInOrder()

	times.Flatten = int(time.Since(startTime).Milliseconds())

	fmt.Println("Single bucket with node count:", len(nodes))
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
		fmt.Println("Length not equal:", len(b1), len(b2))
		return false
	}
	for i := range b1 {
		if !bytes.Equal(b1[i].Key, b2[i].Key) {
			fmt.Println("Keys not equal:", string(b1[i].Key), string(b2[i].Key))
			return false
		}
		oldCardinality := b1[i].Value.Additions.GetCardinality()
		b1[i].Value.Additions.And(b2[i].Value.Additions)
		if b1[i].Value.Additions.GetCardinality() != oldCardinality {
			fmt.Println("Addition not equal:", string(b1[i].Key), oldCardinality, b2[i].Value.Additions.GetCardinality(), b1[i].Value.Additions.GetCardinality())
			return false
		}

		oldCardinality = b1[i].Value.Deletions.GetCardinality()
		b1[i].Value.Deletions.And(b2[i].Value.Deletions)
		if b1[i].Value.Deletions.GetCardinality() != oldCardinality {
			fmt.Println("Deletions not equal:", string(b1[i].Key), oldCardinality, b2[i].Value.Deletions.GetCardinality(), b1[i].Value.Deletions.GetCardinality())
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
				} else if smallestNode != nil && bytes.Equal(metaNodes[i][index].Key, smallestNode.Key) {
					smallestNode.Value.Additions.Or(metaNodes[i][index].Value.Additions)
					smallestNode.Value.Deletions.Or(metaNodes[i][index].Value.Deletions)
					indices[i]++
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
	return flat[:mergedNodesIndex], nil
}

func writeBucket(flat []*roaringset.BinarySearchNode, path string) error {
	totalSize := len(flat)
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
