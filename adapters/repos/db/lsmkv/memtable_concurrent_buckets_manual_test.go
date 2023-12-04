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
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// WARNING: this test will sometimes fail  due to the unknown/random order the operations are sent to the MemTableThreaded.
// Must be disabled before sending to prod.
func TestMemtableConcurrentMergeManual(t *testing.T) {
	numWorkers := runtime.NumCPU()

	operations := [][]*Request{
		{{key: []byte("a"), value: 1, operation: "RoaringSetRemoveOne"}, {key: []byte("a"), value: 1, operation: "RoaringSetRemoveOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "RoaringSetRemoveOne"}},
	}
	numClients := len(operations)

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
