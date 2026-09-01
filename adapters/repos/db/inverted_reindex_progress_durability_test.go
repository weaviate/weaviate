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
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/models"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

const (
	durabilityProp         = "title"
	durabilityCheckpointAt = 20
)

// interruptingRetokenizeStrategy delegates to FilterableRetokenizeStrategy and
// cancels the reindex run's context after a fixed number of writes.
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

// TestReindexProgressCheckpointDurability covers both markProgress call sites in
// OnAfterLsmInitAsync: a checkpoint may only certify postings that are already
// durable. Each case drives one site, snapshots the reindex bucket the way a
// SIGKILL would (bytes the kernel already holds are copied, the commit log's
// user-space buffer is not) and replays that snapshot the way a restart does.
func TestReindexProgressCheckpointDurability(t *testing.T) {
	tests := []struct {
		name               string
		processingDuration time.Duration
		checkEvery         int
		cancelAfter        int
	}{
		{
			name:               "error path checkpoints while unwinding",
			processingDuration: 10 * time.Minute,
			checkEvery:         1000,
			cancelAfter:        durabilityCheckpointAt,
		},
		{
			name:       "time slice path checkpoints on normal return",
			checkEvery: durabilityCheckpointAt,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := testCtx()
			runCtx, cancel := context.WithCancel(ctx)
			defer cancel()

			className := "ReindexCheckpointDurability_" + uuid.NewString()[:8]
			shd, idx := testShardWithSettings(t, ctx,
				newTestClassWithProps(className, []string{durabilityProp}),
				enthnsw.UserConfig{Skip: true}, false, false, false)
			shard := shd.(*Shard)
			defer shard.Shutdown(ctx)

			for _, obj := range makeConvergenceTestObjects(t, 50, className) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			strategy := &interruptingRetokenizeStrategy{
				FilterableRetokenizeStrategy: FilterableRetokenizeStrategy{
					propName:           durabilityProp,
					targetTokenization: models.PropertyTokenizationField,
					className:          className,
					generation:         1,
				},
				cancelAfter: tt.cancelAfter,
				cancel:      cancel,
			}
			task := NewShardReindexTaskGeneric("FilterableRetokenize", idx.logger, strategy,
				reindexTaskConfig{
					concurrency:                   2,
					memtableOptFactor:             4,
					backupMemtableOptFactor:       1,
					pauseDuration:                 time.Second,
					processingDuration:            tt.processingDuration,
					checkProcessingEveryNoObjects: tt.checkEvery,
				},
				&UuidKeyParser{}, uuidObjectsIteratorAsync)

			require.NoError(t, task.OnAfterLsmInit(ctx, shard))
			_, _, err := task.OnAfterLsmInitAsync(runCtx, shard)
			require.Equal(t, tt.cancelAfter > 0, err != nil, "unexpected run outcome: %v", err)
			require.Equal(t, durabilityCheckpointAt, strategy.writes,
				"the site under test must be reached after exactly %d objects", durabilityCheckpointAt)

			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			lastKey, _, err := rt.GetProgress()
			require.NoError(t, err)
			require.NotEmpty(t, lastKey.Bytes(), "the site under test must have written a checkpoint")

			snapshot := copyBucketDir(t, filepath.Join(shard.pathLSM(),
				task.reindexBucketName(durabilityProp)))
			logger, _ := test.NewNullLogger()
			recovered, err := lsmkv.NewBucketCreator().NewBucket(ctx, snapshot, "", logger, nil,
				cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
				lsmkv.WithStrategy(lsmkv.StrategyRoaringSet))
			require.NoError(t, err)
			defer recovered.Shutdown(ctx)

			docIDs := map[uint64]struct{}{}
			for _, ids := range fingerprintRoaringSetBucket(t, recovered) {
				for _, id := range ids {
					docIDs[id] = struct{}{}
				}
			}
			require.Len(t, docIDs, durabilityCheckpointAt,
				"the checkpoint certifies %d objects, so the recovered bucket must hold all of them",
				durabilityCheckpointAt)
		})
	}
}

// copyBucketDir snapshots a live bucket directory into a fresh temp dir. The
// copy sees every byte already handed to the kernel and nothing still held in
// the process, which is what a SIGKILL leaves on disk.
func copyBucketDir(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(src, entry.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, entry.Name()), data, 0o600))
	}
	return dst
}
