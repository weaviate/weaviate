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
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// RunSwapOnShard's IsTidied branch acks the shard as migrated, and for
// enable-rangeable that ack is what flips IndexRangeFilters cluster-wide. It
// must not do that for a generation whose data never reached the canonical
// bucket name — the residue a failed startup promotion leaves behind.
//
// This is the entry path a still-running task takes: the same task instance
// calls RunSwapOnShard directly, with no rehydrate in between, so the
// rehydrate-only guard in OnAfterLsmInitAsync never runs.
func TestRunSwapOnShard_TidiedWithoutPromotion_DoesNotAck(t *testing.T) {
	propName := filterableToRangeablePropName
	mainName := helpers.BucketRangeableFromPropNameLSM(propName)

	tests := []struct {
		name string
		// breakPromotion turns the healthy post-swap state into the one a
		// failed promotion leaves. It runs after the migration reached
		// tidied.mig and before RunSwapOnShard is called a second time.
		breakPromotion func(t *testing.T, ctx context.Context, shard *Shard)
		wantErr        bool
	}{
		{
			// Control: the mainline post-swap state. The ingest dir is still
			// where the data lives — the rename to the canonical name is
			// deferred to the next shard init — and the in-memory swap is
			// what serves it. Acking that is correct.
			name: "deferred rename with the in-memory swap live",
		},
		{
			// enable-rangeable: the schema flag is still false, so a shard
			// reload after a failed promotion opens no bucket at the
			// canonical name at all.
			name: "no bucket at the canonical name",
			breakPromotion: func(t *testing.T, ctx context.Context, shard *Shard) {
				require.NoError(t, shard.store.ShutdownBucket(ctx, mainName))
			},
			wantErr: true,
		},
		{
			// repair-rangeable: the schema flag is already true, so the
			// reload creates an EMPTY canonical bucket over the dir the
			// promotion failed to fill. A check that only asks the store
			// whether a bucket exists passes here.
			name: "empty canonical bucket over an unpromoted dir",
			breakPromotion: func(t *testing.T, ctx context.Context, shard *Shard) {
				require.NoError(t, shard.store.ShutdownBucket(ctx, mainName))
				require.NoError(t, shard.store.CreateOrLoadBucket(ctx, mainName,
					lsmkv.WithStrategy(lsmkv.StrategyRoaringSetRange)))
				require.NotNil(t, shard.store.Bucket(mainName))
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "SwapUnpromotedAck_" + uuid.NewString()[:8]

			shd, idx := testShardWithSettings(t, ctx, newFilterableToRangeableTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeFilterableToRangeableTestObjects(t, 25, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			require.NoError(t, task.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, task.RunPrepareOnShard(ctx, shard))
			require.NoError(t, task.RunSwapOnShard(ctx, shard))

			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			require.True(t, rt.IsTidied(), "the setup must land on the IsTidied dispatch branch")
			require.DirExists(t, filepath.Join(shard.pathLSM(), task.ingestBucketName(propName)),
				"the ingest dir is the only copy of the migrated data until the promotion runs")

			if tc.breakPromotion != nil {
				tc.breakPromotion(t, ctx, shard)
			}

			err = task.RunSwapOnShard(ctx, shard)
			if !tc.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err,
				"a tidied generation whose data never reached the canonical name must not ack success")
			require.False(t, errors.Is(err, context.Canceled),
				"the ack must record a permanent failure, not a shutdown the scheduler would retry in place")
			require.NoFileExists(t, migrationFinalizedMarkerPath(shard.pathLSM(), task.MigrationDirName()),
				"nothing may claim this generation was promoted")
			_, statErr := os.Stat(filepath.Join(shard.pathLSM(), ".migrations", task.MigrationDirName()))
			require.NoError(t, statErr, "the tracker must survive so the next shard init retries")
		})
	}
}
