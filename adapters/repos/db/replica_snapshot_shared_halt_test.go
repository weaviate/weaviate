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
	"strings"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/inverted"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	resolver "github.com/weaviate/weaviate/adapters/repos/db/sharding"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/loadlimiter"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/storobj"
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
// Both replica modes are covered: hardlink (segments staged) and fallback
// halt-for-duration (segments served from the live shard root). The two
// overlapping ops prove the re-seal is repeatable, not one-shot. In fallback
// mode each op's halt outlives its call, so obj2 halts at count 1->2 and obj3 at
// 2->3. In hardlink mode CreateReplicaSnapshot self-resumes after collecting
// files, so obj2 and obj3 each drive an independent 1->2 shared-halt transition.
func TestReplicaSnapshotSharedHaltSealsLateWrites(t *testing.T) {
	const (
		obj1 = strfmt.UUID("40d3be3e-2ecc-49c8-b37c-d8983164848b")
		obj2 = strfmt.UUID("5a7b9c1d-0000-4000-8000-000000000002")
		obj3 = strfmt.UUID("5a7b9c1d-0000-4000-8000-000000000003")
		opB  = "00000000-0000-0000-0000-0000000000b0"
		opC  = "00000000-0000-0000-0000-0000000000c0"
	)

	for _, tc := range []struct {
		name            string
		forceNoHardlink bool
	}{
		{name: "hardlink mode", forceNoHardlink: false},
		{name: "fallback halt-for-duration mode", forceNoHardlink: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.forceNoHardlink {
				t.Setenv("WEAVIATE_TEST_FORCE_NO_HARDLINK", "true")
			}

			index, shard := newSharedHaltTestShard(t)
			ctx := context.Background()

			// object1 is flushed to a segment by op A's initial halt below.
			putSharedHaltObject(t, index, obj1, 0)

			// op A: first halt, never resumed — count stays 1, holds the shard.
			require.NoError(t, shard.HaltForTransfer(ctx, false, 0))
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
				if tc.forceNoHardlink {
					sourceRoot = shard.path()
				}
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
	require.NoError(t, shard.HaltForTransfer(ctx, false, 0))

	// op B's second halt seals with an already-cancelled context; at count>1 the
	// pause steps are gated out, so only the seal steps run and FlushMemtables
	// (cyclemanager Deactivate) returns ctx.Err() deterministically.
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, shard.HaltForTransfer(cancelledCtx, false, 0))

	shard.haltForTransferMux.Lock()
	require.Equal(t, 1, shard.haltForTransferCount,
		"failed count>1 halt must roll back 2->1, leaving op A's hold intact")
	shard.haltForTransferMux.Unlock()

	// op A's halt is intact and resumes cleanly to fully unhalted.
	require.NoError(t, shard.resumeMaintenanceCycles(ctx))
	shard.haltForTransferMux.Lock()
	require.Equal(t, 0, shard.haltForTransferCount)
	shard.haltForTransferMux.Unlock()
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
	mockSchemaGetter.On("NodeName").Return("node1")

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

	mockSchemaReader := schemaUC.NewMockSchemaReader(t)
	mockSchemaReader.EXPECT().Read(mock.Anything, mock.Anything, mock.Anything).
		RunAndReturn(func(_ string, _ bool, readFunc func(*models.Class, *sharding.State) error) error {
			return readFunc(class, ss)
		}).Maybe()

	shardResolver := resolver.NewShardResolver(class.Class, class.MultiTenancyConfig.Enabled, mockSchemaGetter)

	index, err := NewIndex(context.Background(), IndexConfig{
		ClassName:         schema.ClassName("TestClass"),
		RootPath:          t.TempDir(),
		ReplicationFactor: 1,
		ShardLoadLimiter:  loadlimiter.NewLoadLimiter(monitoring.NoopRegisterer, "dummy", 1),
	}, inverted.ConfigFromModel(class.InvertedIndexConfig),
		hnsw.NewDefaultUserConfig(), nil, nil, shardResolver, mockSchemaGetter, mockSchemaReader,
		nil, logger, nil, nil, nil, nil, nil, class, nil, scheduler, nil, nil,
		NewShardReindexerV3Noop(), roaringset.NewBitmapBufPoolNoop(), false, nil)
	require.NoError(t, err)
	index.db = stubDBWithNoLiveReindex()

	shard, err := NewShard(context.Background(), nil, "shard1", index, class, nil, scheduler, nil,
		NewShardReindexerV3Noop(), false, roaringset.NewBitmapBufPoolNoop())
	require.NoError(t, err)
	index.shards.Store("shard1", shard)

	return index, shard
}
