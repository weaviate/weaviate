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
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/backup"
)

// TestBackupable_OneGateStillJudgesEachShardByItsOwnName pins that sharing one
// gate across a shard set still judges each shard by its own name, not a
// shared verdict — a wrong name would pass every build-count assertion.
func TestBackupable_OneGateStillJudgesEachShardByItsOwnName(t *testing.T) {
	const shards = 6

	for _, liveIdx := range []int{0, 3, shards - 1} {
		t.Run(fmt.Sprintf("live on shard %d of %d", liveIdx, shards), func(t *testing.T) {
			db, classes := newPrecheckGateTestDB(t, 1, shards)
			liveShard := fmt.Sprintf("%s-shard%d", classes[0], liveIdx)

			var builds atomic.Int64
			probe := &gateProbe{}
			db.SetShardReindexActivityLookup(func() (ShardReindexActivityLookup, error) {
				builds.Add(1)
				return func(_, shardName string) bool {
					probe.record(shardName)
					return shardName == liveShard
				}, nil
			})

			err := db.Backupable(context.Background(), classes)
			require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)

			require.Equal(t, 1, strings.Count(err.Error(), "active runtime-reindex task in DTM"),
				"exactly one shard is being reindexed, so exactly one must be refused")
			require.Contains(t, err.Error(), liveShard, "the refusal must name the reindexing shard")

			require.Len(t, probe.probed(), shards,
				"the gate must be asked about every shard by its own name")
			require.Equal(t, int64(1), builds.Load(),
				"discriminating per shard must not cost a query per shard")
		})
	}
}

// TestColdTransfer_OneGateStillJudgesEachShardByItsOwnName pins the same
// property on the cold descriptor loops (backupInactiveShardWith[out]Hardlinks),
// which the precheck tests above don't exercise.
func TestColdTransfer_OneGateStillJudgesEachShardByItsOwnName(t *testing.T) {
	const shards = 6

	for _, path := range coldTransferPaths {
		for _, liveIdx := range []int{0, 3, shards - 1} {
			t.Run(fmt.Sprintf("%s/live on shard %d", path.name, liveIdx), func(t *testing.T) {
				liveShard := transferGateShardName(liveIdx)
				idx, builds, _, probe := newTransferGateTestIndex(t, shards, true,
					func(shardName string) bool { return shardName == liveShard })
				t.Cleanup(func() {
					require.NoError(t, idx.ReleaseBackup(context.Background(), "discrimination-backup"))
				})

				var desc backup.ClassDescriptor
				err := path.run(context.Background(), idx, &desc)
				require.ErrorIs(t, err, backup.ErrBackupBlockedByInFlightReindex)
				require.Contains(t, err.Error(), liveShard,
					"the refusal must name the reindexing shard, not whichever shard the loop happened to hold")

				for _, name := range transferGateShardNames(shards) {
					if name == liveShard {
						continue
					}
					require.NotContains(t, err.Error(), name,
						"only the reindexing shard may be refused")
				}
				require.Contains(t, probe.probed(), liveShard,
					"the gate must have been asked about the reindexing shard by name")
				require.Equal(t, int64(1), builds.Load(),
					"discriminating per shard must not cost a query per shard")
			})
		}
	}
}
