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
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/cluster/distributedtask"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

type interruptingRetokenizeStrategy struct {
	FilterableRetokenizeStrategy
	writes      int
	cancelAfter int
	cancel      context.CancelFunc
}

func (s *interruptingRetokenizeStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	if err := s.FilterableRetokenizeStrategy.WriteToReindexBucket(shard, bucket, docID, prop); err != nil {
		return err
	}
	s.writes++
	if s.writes == s.cancelAfter {
		s.cancel()
	}
	return nil
}

// A shard that needs more than one slice resumes from the checkpoint, so
// without one it restarts every slice from the same key and never finishes.
func TestTheIterationLoopRecordsWhereItStopped(t *testing.T) {
	const (
		propName     = "title"
		checkpointAt = 20
	)
	tests := []struct {
		name               string
		processingDuration time.Duration
		checkEvery         int
		cancelAfter        int
	}{
		{
			name:               "the run stops on an error",
			processingDuration: 10 * time.Minute,
			checkEvery:         1000,
			cancelAfter:        checkpointAt,
		},
		{
			name:       "the run's time slice ends",
			checkEvery: checkpointAt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			className := "IterationCheckpoint_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx, newTestClassWithProps(className, []string{propName}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(context.Background())
			for _, obj := range makeConvergenceTestObjects(t, 50, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			strategy := &interruptingRetokenizeStrategy{
				FilterableRetokenizeStrategy: FilterableRetokenizeStrategy{
					propName: propName, targetTokenization: models.PropertyTokenizationField,
					className: className, generation: 1,
				},
				cancelAfter: tt.cancelAfter,
				cancel:      cancel,
			}
			task := NewShardReindexTaskGeneric("FilterableRetokenize", idx.logger, strategy,
				reindexTaskConfig{
					concurrency:                   2,
					memtableOptFactor:             4,
					pauseDuration:                 time.Second,
					processingDuration:            tt.processingDuration,
					checkProcessingEveryNoObjects: tt.checkEvery,
				},
				&UuidKeyParser{}, uuidObjectsIteratorAsync, defaultIndexClosingGuard)
			task.setMigrationIdentity(distributedtask.TaskDescriptor{ID: "retokenize", Version: 1},
				shard.migrationUnit(),
				&ReindexTaskPayload{MigrationType: ReindexTypeChangeTokenizationFilterable, Collection: className})

			require.NoError(t, startOnShard(ctx, task, shard))
			_, err := task.OnAfterLsmInitAsync(runCtx, shard)
			require.Equal(t, tt.cancelAfter > 0, err != nil, "unexpected run outcome: %v", err)
			require.Equal(t, checkpointAt, strategy.writes, "fixture: the loop has to stop after %d objects", checkpointAt)

			rec, ok := task.migrationRecord(shard)
			require.True(t, ok)
			iterating, ok := rec.(MigrationRecordIterating)
			require.True(t, ok, "the run stopped before the end, so the record is still %s", rec.State())
			require.NotEmpty(t, iterating.Checkpoint().LastProcessedKey)
		})
	}
}

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

			task, _ := newFilterableToRangeableTask(t, idx, className, propName, shard.migrationUnit())
			require.NoError(t, startOnShard(ctx, task, shard))

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
				// An unreadable record freezes the store; a foreign-unit one would
				// not, since the loader sets those aside.
				plantUnreadableRecord(t, shard.migrationRecords.Dir())
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
