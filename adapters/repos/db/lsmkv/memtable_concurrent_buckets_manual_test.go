package lsmkv

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemtableConcurrentMergeManual(t *testing.T) {

	numWorkers := runtime.NumCPU()

	operations := [][]*Request{
		{{key: []byte("a"), value: 1, isDeletion: false}, {key: []byte("a"), value: 1, isDeletion: false}},
		{{key: []byte("a"), value: 1, isDeletion: true}},
		{{key: []byte("a"), value: 1, isDeletion: true}},
		{{key: []byte("a"), value: 1, isDeletion: true}},
		{{key: []byte("a"), value: 1, isDeletion: true}},
		{{key: []byte("a"), value: 1, isDeletion: true}},
		{{key: []byte("a"), value: 1, isDeletion: true}},
		{{key: []byte("a"), value: 1, isDeletion: false}},
	}
	numClients := len(operations)

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
