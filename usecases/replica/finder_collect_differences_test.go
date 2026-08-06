//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package replica_test

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/replica"
	replicaerrors "github.com/weaviate/weaviate/usecases/replica/errors"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

func serveHashTreeLevel(ht hashtree.AggregatedHashTree) func(context.Context, string, string, string, int, *hashtree.Bitset) ([]hashtree.Digest, error) {
	return func(_ context.Context, _, _, _ string, level int, discriminant *hashtree.Bitset) ([]hashtree.Digest, error) {
		digests := make([]hashtree.Digest, discriminant.SetCount())
		n, err := ht.Level(level, discriminant, digests)
		if err != nil {
			return nil, err
		}
		return digests[:n], nil
	}
}

func newDivergentTrees(t *testing.T, height int, diffLeaves []uint64) (local, peer *hashtree.HashTree) {
	t.Helper()
	local, err := hashtree.NewHashTree(height)
	require.NoError(t, err)
	peer, err = hashtree.NewHashTree(height)
	require.NoError(t, err)
	for i := uint64(0); i < uint64(hashtree.LeavesCount(height)); i++ {
		require.NoError(t, local.AggregateLeafWith(i, []byte{byte(i)}))
		require.NoError(t, peer.AggregateLeafWith(i, []byte{byte(i)}))
	}
	for _, l := range diffLeaves {
		require.NoError(t, peer.AggregateLeafWith(l, []byte("extra")))
	}
	return local, peer
}

func collectRangeLeaves(t *testing.T, rr hashtree.AggregatedHashTreeRangeReader) []uint64 {
	t.Helper()
	var leaves []uint64
	for {
		start, end, err := rr.Next()
		if errors.Is(err, hashtree.ErrNoMoreRanges) {
			return leaves
		}
		require.NoError(t, err)
		for l := start; l <= end; l++ {
			leaves = append(leaves, l)
		}
	}
}

func TestCollectShardDifferencesConverged(t *testing.T) {
	const (
		class = "C1"
		shard = "SH1"
	)
	ctx := context.Background()

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	ht, err := hashtree.NewHashTree(16)
	require.NoError(t, err)
	for i := uint64(0); i < 1000; i++ {
		require.NoError(t, ht.AggregateLeafWith(i*61, []byte{byte(i), byte(i >> 8)}))
	}

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, mock.Anything, class, shard, mock.Anything, mock.Anything).
		RunAndReturn(serveHashTreeLevel(ht))

	dr, err := finder.CollectShardDifferences(ctx, shard, ht, time.Second, nil)
	require.ErrorIs(t, err, replicaerrors.ErrNoDiffFound)
	require.NotNil(t, dr)

	const iterations = 50
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	for i := 0; i < iterations; i++ {
		_, err := finder.CollectShardDifferences(ctx, shard, ht, time.Second, nil)
		require.ErrorIs(t, err, replicaerrors.ErrNoDiffFound)
	}
	runtime.ReadMemStats(&m2)

	perCall := (m2.TotalAlloc - m1.TotalAlloc) / iterations
	assert.Less(t, perCall, uint64(256*1024))
}

func TestCollectShardDifferencesDivergent(t *testing.T) {
	const (
		class  = "C1"
		shard  = "SH1"
		height = 8
	)
	ctx := context.Background()
	diffLeaves := []uint64{5, 100, 200}

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	local, peer := newDivergentTrees(t, height, diffLeaves)

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, mock.Anything, class, shard, mock.Anything, mock.Anything).
		RunAndReturn(serveHashTreeLevel(peer))

	dr, err := finder.CollectShardDifferences(ctx, shard, local, time.Second, nil)
	require.NoError(t, err)
	require.NotNil(t, dr.RangeReader)
	assert.Contains(t, []string{"B", "C"}, dr.TargetNodeName)
	assert.Equal(t, diffLeaves, collectRangeLeaves(t, dr.RangeReader))
}

func TestCollectShardDifferencesMixedTargets(t *testing.T) {
	const (
		class  = "C1"
		shard  = "SH1"
		height = 8
	)
	ctx := context.Background()
	diffLeaves := []uint64{5, 100, 200}

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	local, peer := newDivergentTrees(t, height, diffLeaves)

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "B", class, shard, mock.Anything, mock.Anything).
		RunAndReturn(serveHashTreeLevel(local)).
		Maybe()
	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "C", class, shard, mock.Anything, mock.Anything).
		RunAndReturn(serveHashTreeLevel(peer))

	dr, err := finder.CollectShardDifferences(ctx, shard, local, time.Second, nil)
	require.NoError(t, err)
	assert.Equal(t, "C", dr.TargetNodeName)
	require.NotNil(t, dr.RangeReader)
	assert.Equal(t, diffLeaves, collectRangeLeaves(t, dr.RangeReader))
}

func TestCollectShardDifferencesSkipsNotReadyTarget(t *testing.T) {
	const (
		class  = "C1"
		shard  = "SH1"
		height = 8
	)
	ctx := context.Background()
	diffLeaves := []uint64{5, 100}

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	local, peer := newDivergentTrees(t, height, diffLeaves)

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "B", class, shard, mock.Anything, mock.Anything).
		Return(nil, replica.ErrAsyncReplicationNotActive).
		Maybe()
	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "C", class, shard, mock.Anything, mock.Anything).
		RunAndReturn(serveHashTreeLevel(peer))

	dr, err := finder.CollectShardDifferences(ctx, shard, local, time.Second, nil)
	require.NoError(t, err)
	assert.Equal(t, "C", dr.TargetNodeName)
	require.NotNil(t, dr.RangeReader)
	assert.Equal(t, diffLeaves, collectRangeLeaves(t, dr.RangeReader))
}

func TestCollectShardDifferencesAllTargetsNotReady(t *testing.T) {
	const (
		class  = "C1"
		shard  = "SH1"
		height = 8
	)
	ctx := context.Background()

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	local, err := hashtree.NewHashTree(height)
	require.NoError(t, err)

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, mock.Anything, class, shard, mock.Anything, mock.Anything).
		Return(nil, replica.ErrAsyncReplicationNotActive)

	dr, err := finder.CollectShardDifferences(ctx, shard, local, time.Second, nil)
	require.ErrorIs(t, err, replica.ErrAsyncReplicationNotActive,
		"all targets not ready must surface the retry-later sentinel")
	require.NotErrorIs(t, err, replicaerrors.ErrNoDiffFound,
		"an unverified cycle must not read as convergence")
	require.Nil(t, dr)
}

func TestCollectShardDifferencesNotReadyPlusConverged(t *testing.T) {
	const (
		class  = "C1"
		shard  = "SH1"
		height = 8
	)
	ctx := context.Background()

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	local, _ := newDivergentTrees(t, height, nil)

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "B", class, shard, mock.Anything, mock.Anything).
		Return(nil, replica.ErrAsyncReplicationNotActive)
	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "C", class, shard, mock.Anything, mock.Anything).
		RunAndReturn(serveHashTreeLevel(local))

	_, err := finder.CollectShardDifferences(ctx, shard, local, time.Second, nil)
	require.ErrorIs(t, err, replicaerrors.ErrNoDiffFound,
		"one converged target is a verified cycle even when another is not ready")
}

func TestCollectShardDifferencesNotReadyPlusFailure(t *testing.T) {
	const (
		class  = "C1"
		shard  = "SH1"
		height = 8
	)
	ctx := context.Background()

	f := newFakeFactory(t, class, shard, []string{"A", "B", "C"}, false)
	finder := f.newFinder("A")

	local, err := hashtree.NewHashTree(height)
	require.NoError(t, err)

	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "B", class, shard, mock.Anything, mock.Anything).
		Return(nil, replica.ErrAsyncReplicationNotActive)
	f.RClient.EXPECT().
		HashTreeLevel(mock.Anything, "C", class, shard, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection refused"))

	_, err = finder.CollectShardDifferences(ctx, shard, local, time.Second, nil)
	require.Error(t, err)
	require.NotErrorIs(t, err, replica.ErrAsyncReplicationNotActive,
		"a hard failure must not be classified as retry-later")
	require.NotErrorIs(t, err, replicaerrors.ErrNoDiffFound)
}

func TestCollectShardDifferencesRejectsMalformedLevelResponse(t *testing.T) {
	testCases := []struct {
		name     string
		response []hashtree.Digest
	}{
		{"empty response", []hashtree.Digest{}},
		{"overlong response", make([]hashtree.Digest, 3)},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newFakeFactory(t, "C1", "SH1", []string{"A", "B", "C"}, false)
			finder := f.newFinder("A")

			ht, err := hashtree.NewHashTree(4)
			require.NoError(t, err)
			require.NoError(t, ht.AggregateLeafWith(0, []byte("x")))

			f.RClient.EXPECT().
				HashTreeLevel(mock.Anything, mock.Anything, "C1", "SH1", mock.Anything, mock.Anything).
				Return(tc.response, nil)

			_, err = finder.CollectShardDifferences(ctx, "SH1", ht, time.Second, nil)
			require.Error(t, err)
			require.NotErrorIs(t, err, replicaerrors.ErrNoDiffFound)
			require.ErrorContains(t, err, "digests")
		})
	}
}
