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

package db

import (
	"context"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	routertypes "github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/replica/hashtree"
)

// TestResolveObjectConflict covers all branches of resolveObjectConflict that
// do not require live storage (i.e., those that do not call s.DeleteObject).
// The delete branches (DeleteOnConflict and TimeBasedResolution when the remote
// is newer) are exercised by the integration tests in
// shard_async_replication_test.go (TestPropagateObjects).
//
// This test specifically validates the fix that changed the !r.Deleted branch
// from returning (false, true, nil) — incorrectly counting every propagated
// live object as "unresolved" — to the correct (false, false, nil).
func TestResolveObjectConflict(t *testing.T) {
	const (
		targetNode = "node-B"
		objID      = "00000000-0000-0000-0000-000000000001"
	)

	makeOverridesNoDeletion := func() additional.AsyncReplicationTargetNodeOverrides {
		return additional.AsyncReplicationTargetNodeOverrides{
			{TargetNode: targetNode, NoDeletionResolution: true},
		}
	}

	tests := []struct {
		name             string
		r                routertypes.RepairResponse
		deletionStrategy string
		targetOverrides  additional.AsyncReplicationTargetNodeOverrides
		localUpdateTime  int64
		wantDeleted      bool
		wantNotResolved  bool
	}{
		{
			// THE CRITICAL FIX: !r.Deleted must return (false, false, nil).
			// Before the fix this returned (false, true, nil), causing every
			// successfully propagated live object to be counted as "unresolved"
			// and ObjectsPropagated to be reported as 0.
			name:             "live remote object — propagation succeeded, no conflict",
			r:                routertypes.RepairResponse{ID: objID, Deleted: false},
			deletionStrategy: models.ReplicationConfigDeletionStrategyDeleteOnConflict,
			wantDeleted:      false,
			wantNotResolved:  false,
		},
		{
			name:             "deleted remote + NoAutomatedResolution strategy",
			r:                routertypes.RepairResponse{ID: objID, Deleted: true},
			deletionStrategy: models.ReplicationConfigDeletionStrategyNoAutomatedResolution,
			wantDeleted:      false,
			wantNotResolved:  true,
		},
		{
			name:             "deleted remote + target-node override forces NoAutomatedResolution",
			r:                routertypes.RepairResponse{ID: objID, Deleted: true, UpdateTime: 200},
			deletionStrategy: models.ReplicationConfigDeletionStrategyDeleteOnConflict,
			targetOverrides:  makeOverridesNoDeletion(),
			localUpdateTime:  100,
			wantDeleted:      false,
			wantNotResolved:  true,
		},
		{
			name:             "deleted remote + TimeBasedResolution + local strictly newer",
			r:                routertypes.RepairResponse{ID: objID, Deleted: true, UpdateTime: 100},
			deletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			localUpdateTime:  200, // local is newer; local wins, no delete
			wantDeleted:      false,
			wantNotResolved:  false,
		},
		{
			name:             "deleted remote + TimeBasedResolution + equal timestamps; local wins",
			r:                routertypes.RepairResponse{ID: objID, Deleted: true, UpdateTime: 100},
			deletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
			localUpdateTime:  100, // r.UpdateTime > local is false → local wins, no delete
			wantDeleted:      false,
			wantNotResolved:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := &Shard{}
			localTimes := map[strfmt.UUID]int64{
				strfmt.UUID(tc.r.ID): tc.localUpdateTime,
			}
			deleted, notResolved, err := s.resolveObjectConflict(
				context.Background(),
				tc.r,
				tc.deletionStrategy,
				targetNode,
				tc.targetOverrides,
				localTimes,
			)
			require.NoError(t, err)
			assert.Equal(t, tc.wantDeleted, deleted, "deleted")
			assert.Equal(t, tc.wantNotResolved, notResolved, "notResolved")
		})
	}
}

func TestHashTreeRoot(t *testing.T) {
	t.Run("not initialized", func(t *testing.T) {
		s := &Shard{}
		_, ok := s.HashTreeRoot()
		assert.False(t, ok)
	})

	t.Run("initialized matches Root and Level(0)", func(t *testing.T) {
		ht, err := hashtree.NewCompactHashTree(1024, 4)
		require.NoError(t, err)
		require.NoError(t, ht.AggregateLeafWith(0, []byte("payload")))

		s := &Shard{hashtree: ht, hashtreeFullyInitialized: true}

		root, ok := s.HashTreeRoot()
		require.True(t, ok)
		assert.Equal(t, ht.Root(), root)

		disc := hashtree.NewBitset(1)
		disc.Set(0)
		level0 := make([]hashtree.Digest, 1)
		n, err := ht.Level(0, disc, level0)
		require.NoError(t, err)
		require.Equal(t, 1, n)
		assert.Equal(t, level0[0], root)
	})
}

// TestGetAsyncReplicationStats pins that a shard reports its replication targets
// in a fixed order, so repeating a node status scan does not reshuffle them.
func TestGetAsyncReplicationStats(t *testing.T) {
	tests := []struct {
		name        string
		targetNodes []string
		wantTargets []string
	}{
		{
			name: "no target", wantTargets: []string{},
		},
		{
			name: "one target", targetNodes: []string{"node-B"},
			wantTargets: []string{"node-B"},
		},
		{
			name:        "many targets",
			targetNodes: []string{"node-D", "node-A", "node-C", "node-B", "node-E"},
			wantTargets: []string{"node-A", "node-B", "node-C", "node-D", "node-E"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := make(map[string]*hashBeatHostStats, len(tt.targetNodes))
			for _, node := range tt.targetNodes {
				stats[node] = &hashBeatHostStats{targetNodeName: node}
			}
			s := &Shard{asyncReplicationStatsByTargetNode: stats}

			reported := make([]string, 0, len(tt.wantTargets))
			for _, status := range s.getAsyncReplicationStats(context.Background()) {
				reported = append(reported, status.TargetNode)
			}

			assert.Equal(t, tt.wantTargets, reported, "order of the reported targets")
		})
	}
}

func TestShardHashTreeLevel(t *testing.T) {
	ctx := context.Background()
	height := 6
	ht, err := hashtree.NewHashTree(height)
	require.NoError(t, err)
	for i := uint64(0); i < uint64(hashtree.LeavesCount(height)); i++ {
		require.NoError(t, ht.AggregateLeafWith(i, []byte{byte(i)}))
	}
	s := &Shard{index: &Index{}, hashtree: ht, hashtreeFullyInitialized: true}

	t.Run("matches full-width reference on every level", func(t *testing.T) {
		for level := 0; level <= height; level++ {
			width := hashtree.LeavesCount(level)
			sparse := hashtree.NewBitset(width)
			for i := 0; i < width; i += 3 {
				sparse.Set(i)
			}
			for _, disc := range []*hashtree.Bitset{
				hashtree.NewBitset(width).Set(0),
				sparse,
				hashtree.NewBitset(width).SetAll(),
				hashtree.NewBitset(width),
			} {
				want := make([]hashtree.Digest, width)
				n, err := ht.Level(level, disc, want)
				require.NoError(t, err)

				got, err := s.HashTreeLevel(ctx, level, disc)
				require.NoError(t, err)
				require.Equal(t, want[:n], got)
				require.Equal(t, disc.SetCount(), cap(got))
			}
		}
	})

	t.Run("rejections", func(t *testing.T) {
		testCases := []struct {
			name  string
			shard *Shard
			level int
			disc  *hashtree.Bitset
		}{
			{"negative level", s, -1, hashtree.NewBitset(1).Set(0)},
			{"level above maximum height", s, maxHashtreeHeight + 1, hashtree.NewBitset(1).Set(0)},
			{"level above tree height", s, height + 1, hashtree.NewBitset(1).Set(0)},
			{"nil discriminant", s, 0, nil},
			{"wrong discriminant size", s, 2, hashtree.NewBitset(1).Set(0)},
			{"hashtree not initialized", &Shard{index: &Index{}}, 0, hashtree.NewBitset(1).Set(0)},
		}
		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := tc.shard.HashTreeLevel(ctx, tc.level, tc.disc)
				require.Error(t, err)
			})
		}
	})
}

func TestLazyLoadShardHashTreeLevelUnloaded(t *testing.T) {
	l := &LazyLoadShard{}
	digests, err := l.HashTreeLevel(context.Background(), 0, hashtree.NewBitset(1).Set(0))
	require.ErrorIs(t, err, errAsyncReplicationNotActive)
	assert.Nil(t, digests)
}
