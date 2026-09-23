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
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"

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

// TestUpgrade_InterruptedQuantizedUpgradeCorruptsCompressedBucket reproduces
// https://github.com/weaviate/0-weaviate-issues/issues/451.
//
// The dynamic index's flat stage and the in-progress HNSW share ONE on-disk
// compressed LSM bucket (both derive it from GetCompressedBucketName(targetVector)),
// keyed by docID. When the flat stage and the HNSW target use different-length
// quantizers that both compress on insert, doUpgrade -> copyToVectorIndex ->
// AddBatch overwrites the flat stage's codes with the HNSW's codes, batch by
// batch, in that shared bucket. The "upgraded" verdict is committed only AFTER
// the whole copy.
//
// If the process is interrupted mid-copy (simulated by panicking between copy
// batches, which skips doUpgrade's cleanup exactly as a crash would), the shared
// compressed bucket is left as a mix of the two code formats — of different byte
// lengths — with no upgrade verdict recorded. On restart the dynamic index rolls
// back to the flat stage and, on the buggy code, reads that mixed bucket: its
// quantizer's distancer rejects the wrong-length codes with "vector lengths
// don't match".
//
// The fix persists an UPGRADING marker before the copy and, on restart, rebuilds
// the flat compressed bucket from the intact raw vectors bucket, so searches
// succeed with the flat stage's own codes again. The restart here goes through a
// full store shutdown + reopen, so recovery runs against the on-disk buckets.
func TestUpgrade_InterruptedQuantizedUpgradeCorruptsCompressedBucket(t *testing.T) {
	rq8 := func() flatent.UserConfig {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		fuc.RQ.Enabled = true
		fuc.RQ.Bits = 8
		return fuc
	}
	bqFlat := func() flatent.UserConfig {
		fuc := flatent.UserConfig{}
		fuc.SetDefaults()
		fuc.BQ.Enabled = true
		return fuc
	}
	hnswWith := func(mutate func(*hnswent.UserConfig)) hnswent.UserConfig {
		hnswuc := hnswent.UserConfig{
			MaxConnections:        30,
			EFConstruction:        64,
			EF:                    32,
			VectorCacheMaxObjects: 1_000_000,
		}
		hnswuc.SetDefaults()
		mutate(&hnswuc)
		return hnswuc
	}

	tests := []struct {
		name string
		// targetVector "" is the legacy unnamed vector; a non-empty value is a
		// named target vector (its own buckets + state key), which also exercises
		// the commit-log-dir inference path in init that the UPGRADING marker
		// has to override.
		targetVector string
		flatUC       flatent.UserConfig
		hnswUC       hnswent.UserConfig
	}{
		{
			// flat RQ8 (80-byte codes) reads HNSW BQ codes (8 bytes) => "8 vs 80"
			name:   "flat RQ8 -> hnsw BQ",
			flatUC: rq8(),
			hnswUC: hnswWith(func(uc *hnswent.UserConfig) { uc.BQ.Enabled = true }),
		},
		{
			// the reverse pairing: flat BQ reads HNSW RQ8 codes
			name:   "flat BQ -> hnsw RQ8",
			flatUC: bqFlat(),
			hnswUC: hnswWith(func(uc *hnswent.UserConfig) { uc.RQ.Enabled = true; uc.RQ.Bits = 8 }),
		},
		{
			// named target vector: a crash must not be misread as upgraded by the
			// commit-log-dir inference; {2} must win.
			name:         "named target vector, flat RQ8 -> hnsw BQ",
			targetVector: "namedtarget",
			flatUC:       rq8(),
			hnswUC:       hnswWith(func(uc *hnswent.UserConfig) { uc.BQ.Enabled = true }),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runInterruptedQuantizedUpgrade(t, tc.targetVector, tc.flatUC, tc.hnswUC)
		})
	}
}

func runInterruptedQuantizedUpgrade(t *testing.T, targetVector string, flatUC flatent.UserConfig, hnswUC hnswent.UserConfig) {
	t.Helper()
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dimensions := 32
	vectorsSize := 2 * batchSize // > batchSize so the copy spans >1 batch
	queriesSize := 20
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	dist := distancer.NewL2SquaredProvider()

	dirName := t.TempDir()
	indexID := "interrupted-quantized-upgrade"
	storeDir := filepath.Join(t.TempDir(), "store")
	require.NoError(t, os.MkdirAll(storeDir, 0o777))

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	noopCallback := cyclemanager.NewCallbackGroupNoop()

	config := Config{
		AllocChecker:          memwatch.NewDummyMonitor(),
		RootPath:              dirName,
		ID:                    indexID,
		TargetVector:          targetVector,
		Logger:                logger,
		DistanceProvider:      dist,
		MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			vec := vectors[int(id)]
			if vec == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return vec, nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           noopCallback,
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}

	uc := ent.UserConfig{
		Threshold: uint64(vectorsSize),
		Distance:  dist.Type(),
		HnswUC:    hnswUC,
		FlatUC:    flatUC,
	}

	store := testinghelpers.NewDummyStoreFromFolder(storeDir, t)

	idx, err := New(config, uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)

	for i := 0; i < vectorsSize; i++ {
		require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
	}
	require.True(t, idx.Compressed(), "flat stage must be compressed before upgrade")

	// Baseline: the flat stage's own answers before any upgrade. The repair
	// re-encodes from the raw bucket, so it must reproduce these exactly —
	// a quantizer-agnostic proof that recovery restored the real codes (not
	// just silenced the length error or returned garbage).
	baseline := make([][]uint64, queriesSize)
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoError(t, err)
		baseline[i] = ids
	}

	// Interrupt the upgrade mid-copy: panic between copy batches, after the
	// first batch has already written HNSW codes over the flat codes for
	// ids 0..batchSize-1. GoWrapper recovers the panic, so doUpgrade's cleanup
	// never runs — exactly the on-disk residue a hard crash leaves.
	var once sync.Once
	idx.betweenCopyBatchesHook = func() {
		once.Do(func() { panic("simulated crash during quantized dynamic upgrade") })
	}

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
	require.False(t, idx.IsUpgraded(), "upgrade must have been interrupted, not completed")

	// The upgrade-start marker must have been persisted as UPGRADING ({2}) and
	// must not read as "upgraded" — for a named vector this is exactly what the
	// commit-log-dir inference would otherwise get wrong.
	require.Equal(t, verdictUpgrading, readVerdict(t, db, targetVector),
		"doUpgrade must persist the UPGRADING marker before the copy")

	// Restart through a full store shutdown + reopen from the same directory, so
	// recovery runs against the on-disk buckets, not an in-memory store.
	require.NoError(t, store.Shutdown(ctx))
	store = testinghelpers.NewDummyStoreFromFolder(storeDir, t)
	reopened := store

	idx, err = New(config, uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)
	t.Cleanup(func() {
		idx.Shutdown(context.Background())
		reopened.Shutdown(context.Background())
	})

	require.False(t, idx.IsUpgraded(),
		"an interrupted upgrade must roll back to the flat stage on restart")

	// The core regression: every search must succeed. On the buggy code the flat
	// stage reads wrong-length codes for ids 0..batchSize-1 and fails with
	// "vector lengths don't match". With the fix, results must match the
	// pre-upgrade baseline exactly.
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoErrorf(t, err,
			"search after an interrupted quantized upgrade must not fail on a corrupt compressed bucket (query %d)", i)
		assert.ElementsMatchf(t, baseline[i], ids,
			"repaired flat stage must return its original results for query %d", i)
	}
}

// TestUpgrade_InterruptedUpgradeRepairIsDurableAcrossCrash pins the durability
// half of the #451 fix: the recovery must persist the rebuilt codes to disk
// BEFORE it clears the UPGRADING marker. The marker lives in the fsync'd shard
// metadata DB; the codes go to the compressed bucket's bufio-buffered WAL. If
// recovery clears the marker without flushing, a crash that loses the WAL buffer
// leaves "flat, all good" on disk over a still-corrupt compressed bucket, and
// the next start never re-repairs.
//
// The interrupted residue is built directly (small enough that the rebuilt codes
// stay inside the WAL buffer and never auto-flush), recovery is run, and a crash
// is simulated by snapshotting only the durably-on-disk state (a directory copy,
// which excludes in-memory WAL buffers). Opening from that snapshot must still
// search correctly — which only holds if recovery flushed the bucket first.
func TestUpgrade_InterruptedUpgradeRepairIsDurableAcrossCrash(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dimensions := 32
	vectorsSize := 8 // small: rebuilt codes stay in the WAL buffer, so a crash
	queriesSize := 10
	k := 5
	indexID := "interrupted-upgrade-durability"

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	dist := distancer.NewL2SquaredProvider()

	root := t.TempDir()
	storeDir := filepath.Join(root, "store")
	rootPath := filepath.Join(root, "index")
	dbPath := filepath.Join(root, "index.db")
	for _, d := range []string{storeDir, rootPath} {
		require.NoError(t, os.MkdirAll(d, 0o777))
	}

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.RQ.Enabled = true
	fuc.RQ.Bits = 8

	hnswuc := hnswent.UserConfig{
		MaxConnections: 30, EFConstruction: 64, EF: 32, VectorCacheMaxObjects: 1_000_000,
	}
	hnswuc.SetDefaults()
	hnswuc.BQ.Enabled = true

	uc := ent.UserConfig{Threshold: uint64(vectorsSize), Distance: dist.Type(), HnswUC: hnswuc, FlatUC: fuc}

	newConfig := func(db *bbolt.DB, rp string) Config {
		return Config{
			AllocChecker:          memwatch.NewDummyMonitor(),
			RootPath:              rp,
			ID:                    indexID,
			Logger:                logger,
			DistanceProvider:      dist,
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			GetViewThunk:                 GetViewThunk,
			TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
			TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
			SharedDB:                     db,
			MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
			AsyncIndexingEnabled:         true,
		}
	}

	// Phase 1: build the flat RQ8 stage, then fabricate the on-disk residue an
	// interrupted quantized upgrade leaves: one compressed entry overwritten with
	// a shorter (BQ-length) code, and the UPGRADING marker set.
	db, err := bbolt.Open(dbPath, 0o666, nil)
	require.NoError(t, err)
	store := testinghelpers.NewDummyStoreFromFolder(storeDir, t)
	idx, err := New(newConfig(db, rootPath), uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)
	for i := 0; i < vectorsSize; i++ {
		require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
	}
	require.True(t, idx.Compressed())

	baseline := make([][]uint64, queriesSize)
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoError(t, err)
		baseline[i] = ids
	}

	// pollute one code with a short (BQ-length, 8-byte) value
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, 0)
	require.NoError(t, store.Bucket(idx.getCompressedBucketName()).Put(key, make([]byte, 8)))
	// mark the upgrade as in progress, as doUpgrade's start marker would
	writeVerdict(t, db, "", verdictUpgrading)
	require.NoError(t, store.Shutdown(ctx))

	// Phase 2: reopen and let recovery run against the on-disk residue.
	store = testinghelpers.NewDummyStoreFromFolder(storeDir, t)
	idx, err = New(newConfig(db, rootPath), uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)
	require.False(t, idx.IsUpgraded())
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoError(t, err)
		assert.ElementsMatch(t, baseline[i], ids)
	}

	// Phase 3: simulate a crash right after recovery by snapshotting only the
	// durably-on-disk state (a directory copy — in-memory WAL buffers are not
	// captured). Do NOT shut the store/db down first, or their flush would mask
	// the gap.
	snap := t.TempDir()
	require.NoError(t, snapshotTree(root, snap))
	t.Cleanup(func() {
		idx.Shutdown(context.Background())
		store.Shutdown(context.Background())
		db.Close()
	})

	snapDB, err := bbolt.Open(filepath.Join(snap, "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { snapDB.Close() })
	snapStore := testinghelpers.NewDummyStoreFromFolder(filepath.Join(snap, "store"), t)
	idx2, err := New(newConfig(snapDB, filepath.Join(snap, "index")), uc, snapStore)
	require.NoError(t, err)
	idx2.PostStartup(ctx)
	t.Cleanup(func() {
		idx2.Shutdown(context.Background())
		snapStore.Shutdown(context.Background())
	})

	// If the marker was cleared before the rebuilt codes were durable, the
	// snapshot has the cleared marker over the still-corrupt bucket and this
	// search fails with "vector lengths don't match".
	for i := range queries {
		ids, _, err := idx2.SearchByVector(ctx, queries[i], k, nil)
		require.NoErrorf(t, err, "post-crash search must succeed (query %d): repair was not durable", i)
		assert.ElementsMatch(t, baseline[i], ids)
	}
}

func readVerdict(t *testing.T, db *bbolt.DB, targetVector string) byte {
	t.Helper()
	var v byte
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		require.NotNil(t, b)
		raw := b.Get(dbKey(targetVector))
		require.NotEmpty(t, raw)
		v = raw[0]
		return nil
	}))
	return v
}

func writeVerdict(t *testing.T, db *bbolt.DB, targetVector string, verdict byte) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dynamicBucket)
		if err != nil {
			return err
		}
		return b.Put(dbKey(targetVector), []byte{verdict})
	}))
}

// snapshotTree recursively copies src into dst, capturing exactly what is on disk —
// the crash snapshot for the durability test.
func snapshotTree(src, dst string) error {
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst, 0o777); err != nil {
		return err
	}
	for _, e := range entries {
		s := filepath.Join(src, e.Name())
		d := filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := snapshotTree(s, d); err != nil {
				return err
			}
			continue
		}
		if err := snapshotFile(s, d); err != nil {
			return err
		}
	}
	return nil
}

func snapshotFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
