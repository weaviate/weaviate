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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storobj"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// A resume checkpoint claims that every object up to a key has been indexed,
// so anything the checkpoint outlives is never scanned again. These tests kill
// the process at each of the two points where a checkpoint is written and
// assert the finished index still holds a posting for every object.

// checkpointDurabilityObjects gives each object its own token, so a posting
// lost to the crash shows up as a missing term rather than a shorter posting
// list that a second object could mask.
func checkpointDurabilityObjects(className string, n int) []*storobj.Object {
	out := make([]*storobj.Object, n)
	for i := 0; i < n; i++ {
		out[i] = createTestObjectWithText(className, checkpointDurabilityToken(i))
	}
	return out
}

func checkpointDurabilityToken(i int) string {
	return fmt.Sprintf("tok%03d", i)
}

// crashingStrategy cancels the iteration from inside a bucket write, which is
// the only way to reach the deferred checkpoint: it is written when the
// iteration ends with an error, and a shutdown cancelling the context is how
// that happens in production.
type crashingStrategy struct {
	EnableFilterableStrategy
	writes          int
	cancelAfter     int
	cancelIteration context.CancelFunc
}

func (s *crashingStrategy) WriteToReindexBucket(shard ShardLike, bucket *lsmkv.Bucket,
	docID uint64, prop inverted.Property,
) error {
	if err := s.EnableFilterableStrategy.WriteToReindexBucket(shard, bucket, docID, prop); err != nil {
		return err
	}
	s.writes++
	if s.cancelAfter > 0 && s.writes == s.cancelAfter {
		s.cancelIteration()
	}
	return nil
}

func newCheckpointDurabilityTask(idx *Index, className, propName string,
	strategy MigrationStrategy, processingDuration time.Duration, checkEvery int,
) *ShardReindexTaskGeneric {
	return NewShardReindexTaskGeneric(
		"EnableFilterable", idx.logger, strategy,
		reindexTaskConfig{
			concurrency:                   2,
			memtableOptFactor:             4,
			backupMemtableOptFactor:       1,
			processingDuration:            processingDuration,
			pauseDuration:                 time.Millisecond,
			checkProcessingEveryNoObjects: checkEvery,

			selectionEnabled: true,
			selectedPropsByCollection: map[string]map[string]struct{}{
				className: {propName: {}},
			},
			selectedShardsByCollection: map[string]map[string]struct{}{
				className: nil, // nil = all shards
			},
		},
		&UuidKeyParser{}, uuidObjectsIteratorAsync,
	)
}

// copyDirTree mirrors src into dst, which must not exist yet.
func copyDirTree(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if !d.Type().IsRegular() {
			return nil
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.Create(target)
		if err != nil {
			return err
		}
		defer out.Close()
		_, err = io.Copy(out, in)
		return err
	}))
}

// crashShard models a SIGKILL: it captures the shard directory as the
// operating system currently holds it, which is every byte the process has
// handed over and nothing it is still buffering, then puts that capture back
// after closing the shard the ordinary way. Whatever a clean close would have
// written is discarded, so the reopened shard sees exactly what a killed one
// would have left behind.
func crashShard(t *testing.T, ctx context.Context, idx *Index, shard *Shard,
	class *models.Class,
) *Shard {
	t.Helper()
	shardName := shard.Name()
	shardRoot := shard.path()

	snapshot := filepath.Join(t.TempDir(), "crash-state")
	copyDirTree(t, shardRoot, snapshot)

	require.NoError(t, shard.Shutdown(ctx))
	simulateProcessRestartBucketCleanup(t, shard.pathLSM())

	require.NoError(t, os.RemoveAll(shardRoot))
	copyDirTree(t, snapshot, shardRoot)

	restarted, err := idx.initShard(ctx, shardName, class, nil, true, true)
	require.NoError(t, err, "shard re-init after crash")
	idx.shards.Store(shardName, restarted)
	return restarted.(*Shard)
}

// TestReindexCheckpointDurability_ResumeAfterCrash pins the ordering between a
// resume checkpoint and the postings it vouches for. The checkpoint is a plain
// file write that reaches the page cache at once and so survives a kill; the
// postings sit in the reindex bucket's buffered write-ahead log and do not. If
// the checkpoint is written first, the resumed scan starts past a range whose
// postings the kill discarded, and no later pass ever revisits it.
//
// The fixture is deliberately small: the postings written before the crash are
// only a few kilobytes, well inside the write-ahead log's buffer, so without a
// flush ahead of the checkpoint none of them have reached the operating system
// and the kill takes all of them.
func TestReindexCheckpointDurability_ResumeAfterCrash(t *testing.T) {
	const (
		propName   = "title"
		numObjects = 25
		crashAfter = 10
	)

	cases := []struct {
		name string
		// A checkpoint is written at two points, and only one of them is
		// reached per run: the end of a processing time slice, and the
		// deferred write that runs when the iteration ends with an error.
		processingDuration time.Duration
		checkEvery         int
		// cancelAfter > 0 ends the iteration with an error after that many
		// bucket writes, which is what reaches the deferred checkpoint.
		cancelAfter int
	}{
		{
			name:               "time slice boundary",
			processingDuration: 0,
			checkEvery:         crashAfter,
		},
		{
			name:               "iteration cancelled",
			processingDuration: 10 * time.Minute,
			checkEvery:         1000,
			cancelAfter:        crashAfter,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := testCtx()
			className := "CheckpointDurability_" + uuid.NewString()[:8]
			class := newEnableFilterableTestClass(className, propName)

			shd, idx := testShardWithSettings(t, ctx, class, enthnsw.UserConfig{Skip: true},
				false, false, false)
			shard := shd.(*Shard)

			for _, obj := range checkpointDurabilityObjects(className, numObjects) {
				require.NoError(t, shard.PutObject(ctx, obj))
			}

			iterationCtx, cancelIteration := context.WithCancel(ctx)
			defer cancelIteration()
			strategy := &crashingStrategy{
				EnableFilterableStrategy: EnableFilterableStrategy{
					propNames:  []string{propName},
					generation: 1,
				},
				cancelAfter:     tc.cancelAfter,
				cancelIteration: cancelIteration,
			}
			task := newCheckpointDurabilityTask(idx, className, propName, strategy,
				tc.processingDuration, tc.checkEvery)
			task.skipSwapOnFinish.Store(true)

			require.NoError(t, task.OnAfterLsmInit(iterationCtx, shard))
			rerunAt, _, err := task.OnAfterLsmInitAsync(iterationCtx, shard)
			if tc.cancelAfter > 0 {
				require.Error(t, err, "iteration should have been cancelled")
			} else {
				require.NoError(t, err)
				require.Falsef(t, rerunAt.IsZero(),
					"iteration finished in one slice; nothing was checkpointed, so the crash is untested")
			}

			// Non-vacuity guard. Without a checkpoint on disk the restarted
			// shard rescans from the beginning and would pass whatever the
			// crash destroyed.
			rt, err := task.newReindexTracker(shard.pathLSM())
			require.NoError(t, err)
			checkpointKey, _, err := rt.GetProgress()
			require.NoError(t, err)
			require.NotEmptyf(t, checkpointKey.Bytes(),
				"no resume checkpoint was written; the crash would be rescanned from scratch")
			require.Falsef(t, rt.IsReindexed(),
				"reindex already complete; the crash lands after the resume path, not inside it")

			shard = crashShard(t, ctx, idx, shard, class)
			defer shard.Shutdown(ctx)

			// Resume: a fresh task over the restarted shard, run to completion.
			resumeTask := newCheckpointDurabilityTask(idx, className, propName,
				&EnableFilterableStrategy{propNames: []string{propName}, generation: 1},
				10*time.Minute, 1000)
			require.NoError(t, resumeTask.RunReindexOnlyOnShard(ctx, shard))
			require.NoError(t, resumeTask.RunPrepareOnShard(ctx, shard))
			require.NoError(t, resumeTask.RunSwapOnShard(ctx, shard))

			bucket := shard.store.Bucket(helpers.BucketFromPropNameLSM(propName))
			require.NotNil(t, bucket, "filterable bucket must exist after the migration")
			fingerprint := fingerprintRoaringSetBucket(t, bucket)

			var missing []string
			for i := 0; i < numObjects; i++ {
				if docIDs := fingerprint[checkpointDurabilityToken(i)]; len(docIDs) == 0 {
					missing = append(missing, checkpointDurabilityToken(i))
				}
			}
			require.Emptyf(t, missing,
				"the resumed reindex skipped %d of %d objects: their postings were lost with the "+
					"crash while the checkpoint that vouches for them survived it",
				len(missing), numObjects)
		})
	}
}
