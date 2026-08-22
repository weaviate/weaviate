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

// segmentsOnDisk counts the reindex bucket's durable segments. A posting that
// is only in the write-ahead log's userspace buffer is in none of them, which
// is what a SIGKILL discards.
func segmentsOnDisk(t *testing.T, bucketDir string) int {
	t.Helper()
	segments, err := filepath.Glob(filepath.Join(bucketDir, "*.db"))
	require.NoError(t, err)
	return len(segments)
}

// TestCheckpointNeverOutrunsThePostingsItVouchesFor pins the durability
// ordering the resume depends on.
//
// The checkpoint is fsynced; the postings behind it sit in a buffered
// write-ahead log. A crash between the two drops the postings and keeps the
// checkpoint, and the resume seeks strictly past the checkpoint key — so
// nothing ever rebuilds them and the flip promotes a bucket permanently
// missing a posting. TestMultiNode_MajorityCrashDuringReindex lost exactly one
// object this way.
func TestCheckpointNeverOutrunsThePostingsItVouchesFor(t *testing.T) {
	const propName = filterableToRangeablePropName

	tests := []struct {
		name        string
		buffered    bool
		wantDurable bool
	}{
		{
			name:        "a posting still in the buffer reaches disk before the checkpoint does",
			buffered:    true,
			wantDurable: true,
		},
		{
			// The barrier must not turn an empty slice into an error: a
			// checkpoint with nothing behind it is the ordinary resume case.
			name: "a checkpoint with nothing buffered is still recorded",
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

			key := task.keyParser.FromBytes([]byte("the-last-processed-key"))
			require.NoError(t, task.recordCheckpoint(shard, subject, key, 1, 1))

			if tt.wantDurable {
				require.NotZero(t, segmentsOnDisk(t, bucket.GetDir()),
					"a checkpoint must never be more durable than the postings it vouches for")
			}

			stored, ok := task.migrationRecord(shard)
			require.True(t, ok)
			iterating, ok := stored.(MigrationRecordIterating)
			require.True(t, ok)
			require.Equal(t, key.Bytes(), iterating.Checkpoint().LastProcessedKey)
		})
	}
}
