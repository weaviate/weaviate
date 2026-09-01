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
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/dynamic"
	flatent "github.com/weaviate/weaviate/entities/vectorindex/flat"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
	"github.com/weaviate/weaviate/usecases/memwatch"
)

// newCopyWindowDynamic builds an async dynamic index seeded with enough vectors
// (2*batchSize) that copyToVectorIndex needs a second cursor batch, so the
// betweenCopyBatchesHook fires exactly once inside the upgrade's copy window,
// with ids 0..batchSize-1 already copied. Ids listed in omit get a vector but
// are not inserted, leaving room for a later Add behind the cursor.
func newCopyWindowDynamic(t *testing.T, omit ...uint64) (*dynamic, [][]float32) {
	t.Helper()
	ctx := context.Background()
	dimensions := 20
	vectorsSize := 2 * batchSize

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	vectors, _ := testinghelpers.RandomVecs(vectorsSize, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	hnswuc := hnswent.UserConfig{
		MaxConnections:        30,
		EFConstruction:        64,
		EF:                    32,
		VectorCacheMaxObjects: 1_000_000,
	}
	hnswuc.SetDefaults()

	uc := ent.UserConfig{
		Threshold: 100,
		Distance:  dist.Type(),
		HnswUC:    hnswuc,
		FlatUC:    fuc,
	}

	idx, err := New(Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              t.TempDir(),
		ID:                    "copy-window-test",
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		DistanceProvider:      dist,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true, // required: New() errors otherwise
	}, uc, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })

	omitted := make(map[uint64]bool, len(omit))
	for _, id := range omit {
		omitted[id] = true
	}
	for i := uint64(0); i < uint64(vectorsSize); i++ {
		if omitted[i] {
			continue
		}
		require.NoError(t, idx.Add(ctx, i, vectors[i]))
	}
	return idx, vectors
}

// runDuringCopyWindow upgrades idx to HNSW and runs op concurrently — from its
// own goroutine, as a live writer would, under the same RLock doUpgrade holds —
// between the first and second cursor batches of copyToVectorIndex. The copy is
// held until op returns, fixing op's position relative to the cursor, and the
// call returns only after both op and the whole upgrade completed.
func runDuringCopyWindow(t *testing.T, idx *dynamic, op func() error) {
	t.Helper()

	opErr := make(chan error, 1)
	var once sync.Once
	idx.betweenCopyBatchesHook = func() {
		once.Do(func() {
			ch := make(chan error, 1)
			go func() { ch <- op() }()
			opErr <- <-ch
		})
	}

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
	require.True(t, idx.status.IsUpgraded(), "upgrade must have completed successfully")

	select {
	case err := <-opErr:
		require.NoError(t, err, "the concurrent op must have been acknowledged")
	default:
		t.Fatal("betweenCopyBatchesHook never fired; the seed must span more than one copy batch")
	}
}

// TestUpgrade_CopyWindowLosesConcurrentWrites pins
// https://github.com/weaviate/weaviate/issues/12892.
//
// doUpgrade holds only the read lock while copyToVectorIndex cursors over the
// flat vectors bucket into the new HNSW, and Add/Delete also run under RLock,
// so both proceed concurrently with the copy:
//
//   - an Add whose key the cursor has already passed lands only in the flat
//     bucket, which is dropped when the upgrade swaps indexes — the write is
//     lost even though it was acknowledged and readable before the swap;
//   - a Delete of an already-copied key removes it from the flat bucket only,
//     while the new HNSW keeps its copy — the vector is resurrected.
//
// The betweenCopyBatchesHook seam makes the interleaving deterministic: the
// concurrent op runs after the first cursor batch (ids 0..batchSize-1 copied)
// while the copy still has a second batch to go.
func TestUpgrade_CopyWindowLosesConcurrentWrites(t *testing.T) {
	t.Skip("pins https://github.com/weaviate/weaviate/issues/12892 — flat→HNSW upgrade window loses concurrent writes; remove skip when fixing")

	t.Run("add behind the cursor is lost", func(t *testing.T) {
		const lateID = uint64(3) // sorts before every id the first batch copied
		idx, vectors := newCopyWindowDynamic(t, lateID)

		var visibleAfterAdd bool
		runDuringCopyWindow(t, idx, func() error {
			if err := idx.Add(context.Background(), lateID, vectors[lateID]); err != nil {
				return err
			}
			visibleAfterAdd = idx.ContainsDoc(lateID)
			return nil
		})

		require.True(t, visibleAfterAdd,
			"precondition: the flat index acknowledged and served the concurrent write")
		assert.Truef(t, idx.ContainsDoc(lateID),
			"vector %d was acknowledged during the upgrade but is gone after the flat→HNSW swap (lost write)",
			lateID)
	})

	t.Run("delete of an already-copied id is resurrected", func(t *testing.T) {
		const victimID = uint64(10) // copied into the new HNSW by the first batch
		idx, _ := newCopyWindowDynamic(t)

		var visibleAfterDelete bool
		runDuringCopyWindow(t, idx, func() error {
			if err := idx.Delete(victimID); err != nil {
				return err
			}
			visibleAfterDelete = idx.ContainsDoc(victimID)
			return nil
		})

		require.False(t, visibleAfterDelete,
			"precondition: the flat index acknowledged the concurrent delete")
		assert.Falsef(t, idx.ContainsDoc(victimID),
			"vector %d was deleted during the upgrade but reappears after the flat→HNSW swap (resurrected delete)",
			victimID)
	})
}
