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
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
	"github.com/weaviate/weaviate/entities/models"
)

// coldTransferPaths are the two cold-shard transfer loops. Which one runs in
// production depends on whether the filesystem supports hardlinks, so both
// carry the same gate and both are pinned here.
var coldTransferPaths = []struct {
	name string
	run  func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error
}{
	{
		name: "hardlinks",
		run: func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error {
			return idx.descriptorWithHardlinks(ctx, "transfer-gate-backup", desc, nil)
		},
	},
	{
		name: "without hardlinks",
		run: func(ctx context.Context, idx *Index, desc *backup.ClassDescriptor) error {
			return idx.descriptorWithoutHardlinks(ctx, "transfer-gate-backup", desc, nil)
		},
	},
}

func transferGateShardName(i int) string {
	return fmt.Sprintf("cold-tenant-%d", i)
}

// newTransferGateTestIndex builds an Index whose cold-transfer loop walks
// shards inactive shards, and counts how often each backup-gate lookup is
// built.
//
// withShardDirs=false is the fixture shape that never reaches the gate:
// backupInactiveShardWith[out]Hardlinks stat the shard dir and return
// errShardNoLocalData first, and the loop swallows that error. A build count
// taken on such a fixture says nothing about the gate, so the tests below
// assert on it explicitly rather than letting it pass as a hoist.
func newTransferGateTestIndex(t *testing.T, shards int, withShardDirs, liveReindex bool) (*Index, *atomic.Int64, *atomic.Int64) {
	t.Helper()

	rootDir := t.TempDir()
	className := "TransferGateClass"

	// Replication factor equal to the tenant count puts every shard on every
	// node, so readSchema reports all of them as local to node1.
	builder := NewMultiTenantShardingStateBuilder().WithReplicationFactor(int64(shards))
	for s := 0; s < shards; s++ {
		builder.AddTenant(transferGateShardName(s), models.TenantActivityStatusCOLD)
	}
	idx := newDescriptorTestIndex(t, rootDir, className, builder.Build())

	if withShardDirs {
		for s := 0; s < shards; s++ {
			createColdShardFiles(t, rootDir, className, transferGateShardName(s))
		}
	}

	activityBuilds, cleanupBuilds := &atomic.Int64{}, &atomic.Int64{}
	db := &DB{}
	db.SetShardReindexActivityLookup(func() ShardReindexActivityLookup {
		activityBuilds.Add(1)
		return func(string, string) bool { return liveReindex }
	})
	db.SetReindexCleanupInProgressLookup(func() CleanupInProgressLookup {
		cleanupBuilds.Add(1)
		return func(string, string) bool { return false }
	})
	idx.db = db

	return idx, activityBuilds, cleanupBuilds
}

// TestColdTransfer_BuildsReindexLookupOncePerShardSet pins that one cold
// transfer builds each lookup exactly once, regardless of shard count.
//
// Each build is one cluster-wide ListDistributedTasks RAFT query, so the
// build count is assertable without scale: fifty shards pin the same
// invariant as fifty thousand.
func TestColdTransfer_BuildsReindexLookupOncePerShardSet(t *testing.T) {
	tests := []struct {
		name          string
		shards        int
		withShardDirs bool
		wantBuilds    int64
	}{
		{name: "single shard", shards: 1, withShardDirs: true, wantBuilds: 1},
		{name: "three shards, still one build", shards: 3, withShardDirs: true, wantBuilds: 1},
		{name: "twelve shards, still one build", shards: 12, withShardDirs: true, wantBuilds: 1},
		{name: "fifty shards, still one build", shards: 50, withShardDirs: true, wantBuilds: 1},
		{name: "no shard dirs, gate never reached", shards: 12, withShardDirs: false, wantBuilds: 0},
	}

	for _, tc := range tests {
		for _, path := range coldTransferPaths {
			t.Run(tc.name+"/"+path.name, func(t *testing.T) {
				idx, activityBuilds, cleanupBuilds := newTransferGateTestIndex(t, tc.shards, tc.withShardDirs, false)

				var desc backup.ClassDescriptor
				require.NoError(t, path.run(context.Background(), idx, &desc))

				wantDescribed := 0
				if tc.withShardDirs {
					wantDescribed = tc.shards
				}
				require.Lenf(t, desc.Shards, wantDescribed,
					"the loop must have walked %d shards to their descriptors; a shorter run would make the build count below meaningless",
					wantDescribed)

				require.Equalf(t, tc.wantBuilds, activityBuilds.Load(),
					"expected %d ListDistributedTasks lookup build(s) for %d shards, got %d",
					tc.wantBuilds, tc.shards, activityBuilds.Load())
				require.Equalf(t, tc.wantBuilds, cleanupBuilds.Load(),
					"expected %d cleanup lookup build(s) for %d shards, got %d",
					tc.wantBuilds, tc.shards, cleanupBuilds.Load())
			})
		}
	}
}

// TestColdTransfer_PopulatedShardsReachTheGate separates "the gate was
// resolved once" from "the gate was never consulted", which produce the same
// build count on a fixture whose shards have no directory on disk.
//
// A live reindex must refuse a shard that holds data, and must leave a shard
// with no local data alone: only the first of those proves the loop reaches
// the gate at all.
func TestColdTransfer_PopulatedShardsReachTheGate(t *testing.T) {
	for _, path := range coldTransferPaths {
		t.Run(path.name, func(t *testing.T) {
			t.Run("shards holding data are refused", func(t *testing.T) {
				idx, activityBuilds, _ := newTransferGateTestIndex(t, 4, true, true)

				var desc backup.ClassDescriptor
				err := path.run(context.Background(), idx, &desc)
				require.Error(t, err)
				require.True(t, errors.Is(err, backup.ErrBackupBlockedByInFlightReindex),
					"refusal must wrap the sentinel so the coordinator can classify it, got %v", err)
				require.Equal(t, int64(1), activityBuilds.Load(),
					"one build must serve the whole shard set, on the refusing path too")
			})

			t.Run("shards with no local data short-circuit before the gate", func(t *testing.T) {
				idx, activityBuilds, _ := newTransferGateTestIndex(t, 4, false, true)

				var desc backup.ClassDescriptor
				require.NoError(t, path.run(context.Background(), idx, &desc),
					"a missing shard dir returns errShardNoLocalData before the gate, so a live reindex cannot refuse it")
				require.Zero(t, activityBuilds.Load(),
					"a loop that reaches no shard must issue no query")
			})
		})
	}
}
