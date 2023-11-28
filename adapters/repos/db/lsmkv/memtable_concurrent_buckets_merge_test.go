//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package lsmkv

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/roaringset"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv/segmentindex"
)

func TestMemtableConcurrentMergeLoad(t *testing.T) {
	const numKeys = 1000000
	const operationsPerClient = 1000
	const numClients = 10000
	numWorkers := runtime.NumCPU()

	operations := generateOperations(numKeys, operationsPerClient, numClients)
	correctOrder, err := createSimpleBucket(operations, t)
	require.Nil(t, err)

	t.Run("baseline", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "baseline", operations, correctOrder)
	})

	t.Run("single-channel", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "single-channel", operations, correctOrder)
	})

	t.Run("random", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "random", operations, correctOrder)
	})

	//t.Run("round-robin", func(t *testing.T) {
	//	RunMergeExperiment(t, numClients, numWorkers, "round-robin", operations, correctOrder)
	//})

	t.Run("hash", func(t *testing.T) {
		RunMergeExperiment(t, numClients, numWorkers, "hash", operations, correctOrder)
	})
}

func RunMergeExperiment(t *testing.T, numClients int, numWorkers int, workerAssignment string, operations [][]*Request, correctOrder []*roaringset.BinarySearchNode) []*roaringset.BinarySearchNode {
	nodes, times := RunExperiment(t, numClients, numWorkers, workerAssignment, operations)

	// TODO: merge the buckets and compare to the non-concurrent version

	// dirName := "multi_thread/segment"
	dirName := t.TempDir()
	startTime := time.Now()

	startTime = time.Now()

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

	return nodes
}

func createSimpleBucket(operations [][]*Request, t *testing.T) ([]*roaringset.BinarySearchNode, error) {
	times := Times{}
	startTime := time.Now()

	// dirName := "single_thread"
	dirName := t.TempDir()
	m, _ := newMemtable(dirName, StrategyRoaringSet, 0, nil)

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
			if operations[j][i].operation == "ThreadedRoaringSetAddOne" {
				err := m.roaringSetAddOne(operations[j][i].key, operations[j][i].value)
				require.Nil(t, err)
			} else if operations[j][i].operation == "ThreadedRoaringSetRemoveOne" {
				err := m.roaringSetRemoveOne(operations[j][i].key, operations[j][i].value)
				require.Nil(t, err)
			}
		}
	}

	times.Insert = int(time.Since(startTime).Milliseconds())
	startTime = time.Now()

	nodes := m.RoaringSet().FlattenInOrder()

	times.Flatten = int(time.Since(startTime).Milliseconds())

	fmt.Println("Single bucket with node count:", len(nodes))
	fmt.Println("\tSetup:", times.Setup)
	fmt.Println("\tInsert:", times.Insert)
	fmt.Println("\tFlatten:", times.Flatten)

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
