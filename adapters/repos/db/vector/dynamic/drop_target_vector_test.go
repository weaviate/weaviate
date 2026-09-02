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

package dynamic

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/shardmeta"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// newDynamicForDrop builds one dynamic index over a SHARED metadata DB,
// mirroring the shard: getOrInitMetadataDB opens index.db once and every
// dynamic vector on that shard is a key inside it.
func newDynamicForDrop(t *testing.T, meta *shardmeta.DB, rootPath, targetVector string) *dynamic {
	t.Helper()
	dist := distancer.NewL2SquaredProvider()
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()

	idx, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		TargetVector:          targetVector,
		RootPath:              rootPath,
		ID:                    "vectors_" + targetVector,
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      dist,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			return []float32{0, 0}, nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk([][]float32{{0, 0}}),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		State:                        meta.Namespace(StateNamespace),
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}, ent.UserConfig{
		Threshold: 1_000_000,
		Distance:  dist.Type(),
		HnswUC:    hnswent.UserConfig{MaxConnections: 8, EFConstruction: 16, EF: 8, VectorCacheMaxObjects: 1000},
		FlatUC:    fuc,
	}, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	return idx
}

// TestDropTargetVector_LeavesTheSharedStateDBUsable pins the whole reason
// DropTargetVector exists.
//
// The metadata DB is opened ONCE PER SHARD and every dynamic vector is a key
// inside it. Removing a single named vector must leave the file and the
// shard's handle intact: every sibling's flat-to-hnsw upgrade, the shard
// backup and the shard's own shutdown all use the same handle.
func TestDropTargetVector_LeavesTheSharedStateDBUsable(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	dbPath := filepath.Join(rootPath, shardmeta.FileName)
	meta, err := shardmeta.Open(rootPath, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	dropped := newDynamicForDrop(t, meta, rootPath, "a")
	sibling := newDynamicForDrop(t, meta, rootPath, "b")

	require.NoError(t, dropped.DropTargetVector(ctx))

	// The file is still there and the handle still works: a sibling can read
	// and write its own state.
	_, statErr := os.Stat(dbPath)
	assert.NoError(t, statErr, "the shard-shared state DB must survive a per-vector drop")

	assert.NotPanics(t, func() {
		err := meta.Namespace(StateNamespace).Put(sibling.dbKey(), []byte("1"))
		assert.NoError(t, err, "the shared handle must still be open for siblings")
	})
}

// TestDropTargetVector_ClearsOnlyItsOwnKey pins both halves of the key
// handling: the dropped vector's upgrade verdict must go, or a re-created
// vector of the same name inherits "already upgraded" and boots straight into
// an empty hnsw, skipping its flat stage; and the sibling's verdict must stay,
// or the sibling silently restarts its own upgrade.
func TestDropTargetVector_ClearsOnlyItsOwnKey(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	meta, err := shardmeta.Open(rootPath, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	dropped := newDynamicForDrop(t, meta, rootPath, "a")
	sibling := newDynamicForDrop(t, meta, rootPath, "b")

	// Both have been upgraded, as the shard would have recorded.
	ns := meta.Namespace(StateNamespace)
	require.NoError(t, ns.Put(dropped.dbKey(), []byte("1")))
	require.NoError(t, ns.Put(sibling.dbKey(), []byte("1")))

	require.NoError(t, dropped.DropTargetVector(ctx))

	droppedState, err := ns.Get(dropped.dbKey())
	require.NoError(t, err)
	assert.Empty(t, droppedState,
		"the dropped vector's upgrade verdict must go, or a re-created name skips its flat stage")
	siblingState, err := ns.Get(sibling.dbKey())
	require.NoError(t, err)
	assert.Equal(t, []byte("1"), siblingState,
		"a sibling's upgrade verdict must survive")
}

// TestDrop_ToleratesClosedMetadataDB pins the shutdown-then-drop journey: the
// shard's shutdown closes the metadata DB before the drop runs, so the state
// key deletion in Drop(keepFiles=false) hits a closed handle. That must read
// as "nothing left to update" (the whole shard directory, key included, is
// removed right after), not fail the drop.
func TestDrop_ToleratesClosedMetadataDB(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	meta, err := shardmeta.Open(rootPath, time.Second)
	require.NoError(t, err)

	idx := newDynamicForDrop(t, meta, rootPath, "a")

	require.NoError(t, meta.Close())
	require.NoError(t, idx.Drop(ctx, false))
}

// TestDrop_LeavesTheSharedMetadataDBUsable pins the ownership boundary that
// DebugResetVectorIndex depends on: Drop(keepFiles=false) must neither close
// nor delete the shard-owned metadata DB. It once did both, which broke every
// sibling dynamic vector and made the reset's re-init fail on the shard's
// stale closed handle.
func TestDrop_LeavesTheSharedMetadataDBUsable(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	meta, err := shardmeta.Open(rootPath, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	dropped := newDynamicForDrop(t, meta, rootPath, "a")
	sibling := newDynamicForDrop(t, meta, rootPath, "b")
	ns := meta.Namespace(StateNamespace)

	require.NoError(t, dropped.Drop(ctx, false))

	// the file is still there and the handle still works: a sibling can read
	// and write its own state
	_, statErr := os.Stat(filepath.Join(rootPath, shardmeta.FileName))
	assert.NoError(t, statErr, "the shard-owned metadata DB must survive a vector index drop")
	require.NoError(t, ns.Put(sibling.dbKey(), []byte{1}))

	// the dropped vector's own verdict is gone, so a re-created name starts
	// from its flat stage
	v, err := ns.Get(dropped.dbKey())
	require.NoError(t, err)
	assert.Empty(t, v)
}

// TestDrop_KeepFilesKeepsTheStateKey pins the backup journey: a drop that
// keeps files (backup in flight) must also keep the upgrade verdict, or the
// backed-up shard restores into an empty hnsw with its flat data unread.
func TestDrop_KeepFilesKeepsTheStateKey(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	meta, err := shardmeta.Open(rootPath, time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { meta.Close() })

	idx := newDynamicForDrop(t, meta, rootPath, "a")
	ns := meta.Namespace(StateNamespace)
	require.NoError(t, ns.Put(idx.dbKey(), []byte{1}))

	require.NoError(t, idx.Drop(ctx, true))

	v, err := ns.Get(idx.dbKey())
	require.NoError(t, err)
	assert.Equal(t, []byte{1}, v, "keepFiles must keep the upgrade verdict")
}
