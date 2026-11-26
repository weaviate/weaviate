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
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/cluster/schema/local"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
	esync "github.com/weaviate/weaviate/entities/sync"
	dynamicent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	"github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/monitoring"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// Pins the shared-halt lost-write bug: when a second transfer consumer halts a
// shard that is already halted (haltForTransferCount>1), HaltForTransfer used to
// early-return without re-sealing. A write that landed after the first
// consumer's flush lived only in the active memtable/WAL — excluded from
// ListBackupFiles — so the second consumer's snapshot silently dropped it.
//
// The two overlapping ops prove the re-seal repeats, not just fires once.
// CreateReplicaSnapshot resumes the shard after collecting files, so obj2 and
// obj3 each take the shared halt count from 1 to 2 on their own.
func TestReplicaSnapshotSharedHaltSealsLateWrites(t *testing.T) {
	const (
		obj1 = strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b")
		obj2 = strfmt.UUID("5a7b9c1d-0000-4000-8000-000000000002")
		obj3 = strfmt.UUID("5a7b9c1d-0000-4000-8000-000000000003")
		opB  = "00000000-0000-0000-0000-0000000000b0"
		opC  = "00000000-0000-0000-0000-0000000000c0"
	)

	for _, tc := range []struct {
		name string
	}{
		{name: "hardlink mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			index, shard := newSharedHaltTestShard(t)
			ctx := context.Background()

			// object1 is flushed to a segment by op A's initial halt below.
			putSharedHaltObject(t, index, obj1, 0)

			// op A: first halt, never resumed — count stays 1, holds the shard.
			require.NoError(t, shard.HaltForTransfer(ctx, false))
			defer func() { _ = shard.resumeMaintenanceCycles(ctx) }()

			var openOps []string
			defer func() {
				for _, opID := range openOps {
					_ = index.IncomingReleaseReplicaSnapshot(ctx, opID)
				}
			}()

			overlaps := []struct {
				opID  string
				objID strfmt.UUID
				docID uint64
			}{
				{opID: opB, objID: obj2, docID: 1},
				{opID: opC, objID: obj3, docID: 2},
			}
			for _, ov := range overlaps {
				// Lands in the fresh active memtable/WAL; a halt never trips the
				// bucket read-only guard, so the write is accepted but unsealed.
				putSharedHaltObject(t, index, ov.objID, ov.docID)

				files, err := index.IncomingCreateReplicaSnapshot(ctx, "shard1", ov.opID)
				require.NoError(t, err)
				require.NotEmpty(t, files)
				openOps = append(openOps, ov.opID)

				// Reconstruct the snapshot's objects bucket from EXACTLY the returned
				// file list — this is what the target receives over the wire.
				sourceRoot := replicaStagingDir(index.Config.RootPath, ov.opID,
					schema.ClassName(index.Config.ClassName))
				require.Truef(t, snapshotHasObject(t, sourceRoot, files, ov.objID),
					"write %s applied before op %s's halt is missing from its snapshot — "+
						"the shared halt skipped the re-seal", ov.objID, ov.opID)
			}
		})
	}
}

// A prep failure on the second (count>1) halt must roll back 2->1 via the
// error defer, never unhalting the shard the first op still holds.
func TestHaltForTransferSharedHaltPrepErrorKeepsShardHalted(t *testing.T) {
	_, shard := newSharedHaltTestShard(t)
	ctx := context.Background()

	// op A holds the shard.
	require.NoError(t, shard.HaltForTransfer(ctx, false))

	// op B's second halt seals with an already-cancelled context; at count>1 the
	// pause steps are gated out, so only the seal steps run and FlushMemtables
	// (cyclemanager Deactivate) returns ctx.Err() deterministically.
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, shard.HaltForTransfer(cancelledCtx, false))

	shard.haltForTransferMux.Lock()
	require.EqualValues(t, 1, shard.haltForTransferCount.Load(),
		"failed count>1 halt must roll back 2->1, leaving op A's hold intact")
	shard.haltForTransferMux.Unlock()

	// op A's halt is intact and resumes cleanly to fully unhalted.
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	shard.haltForTransferMux.Lock()
	require.EqualValues(t, 0, shard.haltForTransferCount.Load())
	shard.haltForTransferMux.Unlock()
}

// A resume drops the halt count to 0 before it restarts the cycles. If the
// restart fails, the count stays 0 and the cycles stay paused. resumeIncomplete
// lets a later resume at count 0 retry the restart.
func TestResumeMaintenanceCyclesRetry(t *testing.T) {
	ctx := context.Background()

	t.Run("a failed resume is retried by a later resume at count 0", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		require.NoError(t, shard.HaltForTransfer(ctx, false))

		// Unregistering makes the resume's Activate fail with ErrorCallbackNotFound.
		cc := shard.cycleCallbacks
		require.NoError(t, cc.vectorCombinedCallbacksCtrl.Unregister(ctx))
		require.Error(t, shard.resumeMaintenanceCycles(ctx))
		require.Zero(t, shard.haltForTransferCount.Load())
		require.True(t, shard.resumeIncomplete.Load())

		reregisterVectorCallbacks(shard)

		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		require.Zero(t, shard.haltForTransferCount.Load())
		require.False(t, shard.resumeIncomplete.Load())
		require.True(t, cc.vectorCombinedCallbacksCtrl.IsActive(), "the retry re-ran the resume work")
	})

	t.Run("a resume at count 0 after a clean cycle does nothing", func(t *testing.T) {
		_, shard := newSharedHaltTestShard(t)
		require.NoError(t, shard.HaltForTransfer(ctx, false))
		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		require.False(t, shard.resumeIncomplete.Load())

		ctrl := shard.cycleCallbacks.vectorCombinedCallbacksCtrl
		require.NoError(t, ctrl.Deactivate(ctx))
		require.NoError(t, shard.resumeMaintenanceCycles(ctx))
		require.False(t, ctrl.IsActive(), "a count-0 resume with no failed resume behind it must not run resume work")
		require.NoError(t, ctrl.Activate())
	})
}

// prepFailingIndex runs onPrepare and then fails PrepareForBackup. onPrepare
// runs while the shard is halted and haltForTransferMux is held.
type prepFailingIndex struct {
	VectorIndex
	onPrepare func()
}

func (i *prepFailingIndex) PrepareForBackup(context.Context) error {
	i.onPrepare()
	return errors.New("forced prepare failure")
}

func TestHaltForTransferRollback(t *testing.T) {
	ctx := context.Background()

	// newRollbackShard returns a shard whose vector index preparation adds a
	// target-node override (an async-replication enable) and then fails.
	newRollbackShard := func(t *testing.T, alsoInPrepare func(s *Shard)) (s *Shard, overrideErr *error, skipped *bool) {
		index, shard := newSharedHaltTestShard(t)
		// A single replica at factor 1: the override alone enables async replication.
		index.schemaReader.(*local.MockSchemaReader).EXPECT().ShardReplicas(mock.Anything, mock.Anything).
			Return([]string{"node1"}, nil).Maybe()
		index.asyncReplicationScheduler = newSchedulerForUnitTest(t)
		index.replicationConfigLock.Lock()
		index.Config.AsyncReplicationConfig = minAsyncReplicationConfig()
		index.replicationConfigLock.Unlock()

		overrideErr, skipped = new(error), new(bool)
		var names []string
		var inner VectorIndex
		require.NoError(t, shard.ForEachVectorIndex(func(name string, vi VectorIndex) error {
			names, inner = append(names, name), vi
			return nil
		}))
		require.Len(t, names, 1)
		shard.setVectorIndex(names[0], &prepFailingIndex{VectorIndex: inner, onPrepare: func() {
			*overrideErr = shard.addTargetNodeOverride(ctx, additional.AsyncReplicationTargetNodeOverride{
				CollectionID:   index.Config.ClassName.String(),
				ShardID:        shard.name,
				SourceNode:     "nodeA",
				TargetNode:     "nodeB",
				UpperTimeBound: time.Now().Add(time.Hour).UnixMilli(),
			})
			*skipped = !asyncReplicationInstalled(shard)
			if alsoInPrepare != nil {
				alsoInPrepare(shard)
			}
		}})
		return shard, overrideErr, skipped
	}

	t.Run("a rollback that fully resumes re-applies the skipped enable", func(t *testing.T) {
		s, overrideErr, skipped := newRollbackShard(t, nil)

		require.Error(t, s.HaltForTransfer(ctx, false))
		require.NoError(t, *overrideErr)
		require.True(t, *skipped, "the enable must be skipped while halted mid-preparation")
		require.Zero(t, s.haltForTransferCount.Load())
		require.False(t, s.resumeIncomplete.Load())
		require.True(t, asyncReplicationInstalled(s), "the rollback must re-apply the skipped enable")
	})

	t.Run("a rollback whose resume fails defers the re-apply to the completing retry", func(t *testing.T) {
		// The pause step has already deactivated the callbacks. Unregistering
		// them now makes the rollback's Activate fail with ErrorCallbackNotFound.
		s, overrideErr, skipped := newRollbackShard(t, func(s *Shard) {
			require.NoError(t, s.cycleCallbacks.vectorCombinedCallbacksCtrl.Unregister(ctx))
		})

		require.Error(t, s.HaltForTransfer(ctx, false))
		require.NoError(t, *overrideErr)
		require.True(t, *skipped, "the enable must be skipped while halted mid-preparation")
		require.Zero(t, s.haltForTransferCount.Load())
		require.True(t, s.resumeIncomplete.Load())
		require.False(t, asyncReplicationInstalled(s), "a rollback whose resume failed must not re-apply")

		reregisterVectorCallbacks(s)

		require.NoError(t, s.resumeMaintenanceCycles(ctx))
		require.False(t, s.resumeIncomplete.Load())
		require.True(t, asyncReplicationInstalled(s), "the completing retry must re-apply the skipped enable")
	})
}

func asyncReplicationInstalled(s *Shard) bool {
	s.asyncReplicationRWMux.RLock()
	defer s.asyncReplicationRWMux.RUnlock()
	return s.hashtree != nil
}

// snapshotHasObject reconstructs the snapshot's objects bucket from the wire
// file list (copying every listed lsm/objects/* file out of sourceRoot into a
// throwaway dir), opens it read-only, and reports whether id is retrievable —
// i.e. whether the write survives in a sealed, listable segment.
func snapshotHasObject(t *testing.T, sourceRoot string, files []string, id strfmt.UUID) bool {
	t.Helper()
	ctx := context.Background()

	const objectsPrefix = "lsm/objects/" // shard-relative segment path
	recon := t.TempDir()
	copied := false
	for _, rel := range files {
		if !strings.HasPrefix(rel, objectsPrefix) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(sourceRoot, rel))
		require.NoError(t, err)
		dst := filepath.Join(recon, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(dst), 0o755))
		require.NoError(t, os.WriteFile(dst, data, 0o644))
		copied = true
	}
	require.True(t, copied, "no objects-bucket segment was listed for the snapshot")

	logger := logrus.New()
	bucket, err := lsmkv.NewBucketCreator().NewBucket(ctx, filepath.Join(recon, "lsm", "objects"), "",
		logger, nil, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyReplace), lsmkv.WithSecondaryIndices(1))
	require.NoError(t, err)
	defer bucket.Shutdown(ctx)

	key, err := uuid.MustParse(id.String()).MarshalBinary()
	require.NoError(t, err)
	val, err := bucket.Get(key)
	require.NoError(t, err)
	return val != nil
}

func putSharedHaltObject(t *testing.T, index *Index, id strfmt.UUID, docID uint64) {
	t.Helper()
	require.NoError(t, index.IncomingPutObject(context.Background(), "shard1", &storobj.Object{
		MarshallerVersion: 1,
		DocID:             docID,
		Object: models.Object{
			ID:    id,
			Class: "TestClass",
		},
	}, 0))
}

// newSharedHaltTestShard builds a real Index+Shard+LSM store (single HOT tenant),
// mirroring the harness in replica_snapshot_concurrent_test.go.
func newSharedHaltTestShard(t *testing.T) (*Index, *Shard) {
	t.Helper()

	mockSchemaGetter := schemaUC.NewMockSchemaGetter(t)

	class := &models.Class{
		Class:               "TestClass",
		InvertedIndexConfig: &models.InvertedIndexConfig{},
		MultiTenancyConfig:  &models.MultiTenancyConfig{Enabled: true},
	}
	mockSchemaGetter.On("ReadOnlyClass", "TestClass").Return(class).Maybe()

	logger := logrus.New()
	scheduler := queue.NewScheduler(queue.SchedulerOptions{Logger: logger, Workers: 1})

	ss := &sharding.State{
		Physical: map[string]sharding.Physical{
			"shard1": {
				Name:           "shard1",
				BelongsToNodes: []string{"node1"},
				Status:         models.TenantActivityStatusHOT,
			},
		},
		PartitioningEnabled: true,
	}

	mockSchemaReader := local.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, ss)
		}).Maybe()

	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)

	index, err := NewIndex(context.Background(), nil, IndexConfig{
		NodeName:          "node1",
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader,
		nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	shutdownIndexOnCleanup(t, index)
	index.db = stubDBWithNoLiveReindex()

	shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop(),
		monitoring.ShardRegistrationEager)
	require.NoError(t, err)
	index.shards.Store("shard1", shard)

	return index, shard
}

// A replica transfer delivers the files the snapshot lists, and index.db was
// not among them. The target of a moved legacy dynamic index
// that had upgraded to hnsw then reads no verdict, treats the index as flat,
// deletes the hnsw commit log it just received, and has no flat stage left
// to fall back to. Pins that the verdict, and the graph, travel with the
// replica.
func TestReplicaTransferCarriesTheDynamicUpgrade(t *testing.T) {
	for _, tc := range []struct {
		name string
	}{
		{name: "hardlink mode"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			const className = "TransferDynamicLegacy"
			duc := dynamicent.UserConfig{}
			duc.SetDefaults()
			duc.Threshold = 10 // the queue upgrades the index once this many are indexed
			shd, idx := testShardWithSettings(t, ctx, &models.Class{Class: className}, duc, false, true)
			shard := shd.(*Shard)
			t.Cleanup(func() { _ = idx.drop() })

			r := rand.New(rand.NewSource(1))
			for _, err := range shd.PutObjectBatch(ctx, createRandomObjects(r, className, 20, 4)) {
				require.NoError(t, err)
			}
			probe := createRandomObjects(rand.New(rand.NewSource(1)), className, 1, 4)[0].Vector
			commitLog := filepath.Join(shard.path(), "main.hnsw.commitlog.d")
			flatBucket := filepath.Join(shard.pathLSM(), "vectors")
			exists := func(path string) bool { _, err := os.Stat(path); return err == nil }
			// the upgrade writes its verdict to index.db and removes the flat stage
			require.Eventually(t, func() bool { return exists(commitLog) && !exists(flatBucket) }, 30*time.Second, 50*time.Millisecond,
				"the queue upgrades past the threshold")
			before, _, err := shard.ObjectVectorSearch(ctx, []models.Vector{probe}, []string{""}, 0, 5, nil, nil, nil, additional.Properties{}, nil, nil)
			require.NoError(t, err)
			require.Len(t, before, 5)

			// what the copier fetches: the snapshot's file list, file by file
			idx.replicaSnapshotOpLocks = esync.NewKeyRWLocker()
			const opID = "move"
			files, err := idx.IncomingCreateReplicaSnapshot(ctx, shard.name, opID)
			require.NoError(t, err)
			delivered := map[string][]byte{}
			for _, rel := range files {
				rc, err := idx.IncomingGetReplicaSnapshotFile(ctx, opID, rel)
				require.NoError(t, err)
				data, err := io.ReadAll(rc)
				require.NoError(t, err)
				require.NoError(t, rc.Close())
				delivered[rel] = data
			}
			require.NoError(t, idx.IncomingReleaseReplicaSnapshot(ctx, opID))

			// the target: an empty shard directory holding exactly the
			// delivered files, then the load the movement runs
			shardDir := shard.path()
			require.NoError(t, shard.Shutdown(ctx))
			require.NoError(t, os.RemoveAll(shardDir))
			for rel, data := range delivered {
				path := filepath.Join(shardDir, rel)
				require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
				require.NoError(t, os.WriteFile(path, data, 0o644))
			}
			restored, err := idx.initShard(ctx, shard.name, idx.getClass(), nil, true, true)
			require.NoError(t, err)
			idx.shards.Store(shard.name, restored)
			target := underlyingShard(t, restored)

			assert.True(t, exists(filepath.Join(target.path(), "main.hnsw.commitlog.d")), "the hnsw commit log survived the move")
			after, _, err := target.ObjectVectorSearch(ctx, []models.Vector{probe}, []string{""}, 0, 5, nil, nil, nil, additional.Properties{}, nil, nil)
			require.NoError(t, err)
			require.Len(t, after, 5, "the moved replica serves its vectors")
		})
	}
}
