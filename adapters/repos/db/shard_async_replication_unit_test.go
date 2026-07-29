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
