package lsmkv

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMemtableConcurrentMergeManual(t *testing.T) {

	numWorkers := runtime.NumCPU()

	BRoaringSetRemoveOne := func(b *Bucket, key []byte, value uint64) error { return b.RoaringSetRemoveOne(key, value) }
	BRoaringSetAddOne := func(b *Bucket, key []byte, value uint64) error { return b.RoaringSetAddOne(key, value) }

	operations := [][]*Request{
		{{key: []byte("a"), value: 1, operation: BRoaringSetRemoveOne}, {key: []byte("a"), value: 1, operation: BRoaringSetRemoveOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetAddOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetAddOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetAddOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetAddOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetAddOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetAddOne}},
		{{key: []byte("a"), value: 1, operation: BRoaringSetRemoveOne}},
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
