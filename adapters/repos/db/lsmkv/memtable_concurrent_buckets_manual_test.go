package lsmkv

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemtableConcurrentMergeManual(t *testing.T) {

	numWorkers := runtime.NumCPU()

	operations := [][]*Request{
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetRemoveOne"}, {key: []byte("a"), value: 1, operation: "ThreadedRoaringSetRemoveOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetAddOne"}},
		{{key: []byte("a"), value: 1, operation: "ThreadedRoaringSetRemoveOne"}},
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
