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
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func segmentsOnDisk(t *testing.T, bucketDir string) int {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(bucketDir, "*.db"))
	require.NoError(t, err)
	return len(segments)
}

func TestCheckpointNeverOutrunsThePostingsItVouchesFor(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name        string
		buffered    bool
		poisonStore bool
		wantDurable bool
	}{
		{
			name:        "a posting still in the buffer reaches disk before the checkpoint does",
			buffered:    true,
			wantDurable: true,
		},
		{
			name: "a checkpoint with nothing buffered is still recorded",
		},
		{
			name:        "the postings are on disk even when recording the checkpoint fails",
			buffered:    true,
			poisonStore: true,
			wantDurable: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CheckpointDurability_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newFilterableToRangeableTestClass(className),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())

			task, _ := newFilterableToRangeableTask(t, idx, className, propName)
			require.NoError(t, task.OnAfterLsmInit(ctx, shard))

			rec, ok := task.migrationRecord(shard)
			require.True(t, ok, "the load hook records the migration as iterating")
			subject := rec.Subject()

			bucket := shard.Store().Bucket(task.reindexBucketName(propName))
			require.NotNil(t, bucket, "the load hook opens the reindex bucket")

			if tt.buffered {
				require.NoError(t, bucket.RoaringSetRangeAdd(42, 7))
				require.Zero(t, segmentsOnDisk(t, bucket.GetDir()),
					"the posting has to start out buffered, or this proves nothing")
			}

			if tt.poisonStore {
				foreign := subject
				foreign.Key.UnitID = "shard-9__node-9"
				require.NoError(t, shard.migrationRecords.Put(
					NewMigrationRecordIterating(foreign, MigrationCheckpoint{})))
				require.NoError(t, shard.migrationRecords.Load())
			}

			key := task.keyParser.FromBytes([]byte("the-last-processed-key"))
			err := task.recordCheckpoint(shard, subject, key)

			if tt.wantDurable {
				require.NotZero(t, segmentsOnDisk(t, bucket.GetDir()),
					"a checkpoint must never be more durable than the postings it vouches for")
			}
			if tt.poisonStore {
				require.Error(t, err, "a frozen store has to refuse the checkpoint")
				return
			}
			require.NoError(t, err)

			stored, ok := task.migrationRecord(shard)
			require.True(t, ok)
			iterating, ok := stored.(MigrationRecordIterating)
			require.True(t, ok)
			require.Equal(t, key.Bytes(), iterating.Checkpoint().LastProcessedKey)
		})
	}
}
