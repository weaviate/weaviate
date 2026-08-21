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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// newDynamicForDrop builds one dynamic index over a SHARED state DB, mirroring
// the shard: getOrInitDynamicVectorIndexDB opens index.db once and every
// dynamic vector on that shard is a key inside it.
func newDynamicForDrop(t *testing.T, db *bbolt.DB, rootPath, targetVector string) *dynamic {
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
		SharedDB:                     db,
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
// The state DB is opened ONCE PER SHARD and every dynamic vector is a key
// inside it. Drop(keepFiles=false) closes that handle and removes the file,
// which is right when the shard goes away and catastrophic when a single named
// vector does: every sibling's flat-to-hnsw upgrade, the shard backup and the
// shard's own shutdown all use the same handle.
func TestDropTargetVector_LeavesTheSharedStateDBUsable(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	dbPath := filepath.Join(rootPath, StateDBFileName)
	db, err := bbolt.Open(dbPath, 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	dropped := newDynamicForDrop(t, db, rootPath, "a")
	sibling := newDynamicForDrop(t, db, rootPath, "b")

	require.NoError(t, dropped.DropTargetVector(ctx))

	// The file is still there and the handle still works: a sibling can read
	// and write its own state.
	_, statErr := os.Stat(dbPath)
	assert.NoError(t, statErr, "the shard-shared state DB must survive a per-vector drop")

	assert.NotPanics(t, func() {
		err := db.Update(func(tx *bbolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists(dynamicBucket)
			if err != nil {
				return err
			}
			return b.Put(sibling.dbKey(), []byte("1"))
		})
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
	db, err := bbolt.Open(filepath.Join(rootPath, StateDBFileName), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	dropped := newDynamicForDrop(t, db, rootPath, "a")
	sibling := newDynamicForDrop(t, db, rootPath, "b")

	// Both have been upgraded, as the shard would have recorded.
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dynamicBucket)
		if err != nil {
			return err
		}
		if err := b.Put(dropped.dbKey(), []byte("1")); err != nil {
			return err
		}
		return b.Put(sibling.dbKey(), []byte("1"))
	}))

	require.NoError(t, dropped.DropTargetVector(ctx))

	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		require.NotNil(t, b)
		assert.Empty(t, b.Get(dropped.dbKey()),
			"the dropped vector's upgrade verdict must go, or a re-created name skips its flat stage")
		assert.Equal(t, []byte("1"), b.Get(sibling.dbKey()),
			"a sibling's upgrade verdict must survive")
		return nil
	}))
}
