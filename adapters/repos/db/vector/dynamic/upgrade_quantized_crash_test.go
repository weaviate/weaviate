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
	"runtime"
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

// interruptingIndex wraps the copy-target HNSW during an upgrade and runs a hook
// before each AddBatch, so a test can interrupt the flat→HNSW copy at a
// deterministic point — end the goroutine (mid-copy crash, via runtime.Goexit),
// return an error (ordinary abort), or mutate the dynamic index (delete
// mid-copy). It embeds VectorIndex so every
// other method passes straight through to the real index. Tests inject it via
// upgradeFn → upgradeUsing; there is no production seam.
type interruptingIndex struct {
	VectorIndex
	calls      int
	onAddBatch func(call int, ids []uint64) error
}

func (i *interruptingIndex) AddBatch(ctx context.Context, ids []uint64, vectors [][]float32) error {
	i.calls++
	if i.onAddBatch != nil {
		if err := i.onAddBatch(i.calls, ids); err != nil {
			return err
		}
	}
	return i.VectorIndex.AddBatch(ctx, ids, vectors)
}

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
// If the process is interrupted mid-copy (simulated by ending the upgrade
// goroutine between copy batches, which skips doUpgrade's cleanup exactly as a
// crash would), the shared
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
			// commit-log-dir inference; the in-progress marker must win.
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
	// Pin the crash simulation to the integration environment (recovery disabled):
	// with runtime.Goexit this is a no-op, but if the interrupt is ever changed
	// back to a panic, GoWrapper will not recover it here and this test crashes —
	// locally, not only in the integration CI.
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "true")
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

	// Interrupt the upgrade mid-copy, before the second copy batch, after the first
	// has already written HNSW codes over the flat codes for ids 0..batchSize-1.
	// runtime.Goexit ends the upgrade goroutine without a panic — none of
	// doUpgrade's error/cleanup paths run, leaving exactly the on-disk residue a
	// hard crash leaves. (A panic would depend on GoWrapper's recover, which is
	// off under DISABLE_RECOVERY_ON_PANIC in integration runs; Goexit does not.)
	idx.upgradeFn = func() error {
		return idx.upgradeUsing(func(target VectorIndex) VectorIndex {
			return &interruptingIndex{VectorIndex: target, onAddBatch: func(call int, ids []uint64) error {
				if call >= 2 {
					runtime.Goexit()
				}
				return nil
			}}
		})
	}

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
	require.False(t, idx.IsUpgraded(), "upgrade must have been interrupted, not completed")

	// The in-progress-upgrade marker (a sibling of the verdict key) must be set,
	// the verdict must stay flat, and the offline reader must report not-upgraded
	// — even for a named vector, whose commit-log dir would otherwise be inferred
	// as upgraded.
	present, err := idx.upgradingMarkerPresent()
	require.NoError(t, err)
	assert.True(t, present, "doUpgrade must persist the in-progress-upgrade marker before the copy")
	assert.NotEqual(t, verdictUpgraded, readVerdict(t, db, targetVector),
		"the verdict itself must stay flat while an upgrade is in progress")

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
	// mark the upgrade as in progress, as doUpgrade's start marker would (the
	// verdict itself stays flat/absent)
	markUpgradingInDB(t, db, "")
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

// readVerdict returns the recorded verdict byte, or verdictFlat when no verdict
// key is stored (which is the state while an upgrade is only marked in progress).
func readVerdict(t *testing.T, db *bbolt.DB, targetVector string) byte {
	t.Helper()
	v := verdictFlat
	require.NoError(t, db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(dynamicBucket)
		if b == nil {
			return nil
		}
		if raw := b.Get(dbKey(targetVector)); len(raw) > 0 {
			v = raw[0]
		}
		return nil
	}))
	return v
}

// markUpgradingInDB sets the in-progress-upgrade marker key directly, as
// doUpgrade's start marker would.
func markUpgradingInDB(t *testing.T, db *bbolt.DB, targetVector string) {
	t.Helper()
	require.NoError(t, db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(dynamicBucket)
		if err != nil {
			return err
		}
		return b.Put(upgradingKey(targetVector), []byte{1})
	}))
}

// writeVerdict sets a target vector's verdict byte directly.
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

// TestUpgrade_MarkerKeyDisjointFromVerdictKey pins that the in-progress marker
// lives in a namespace disjoint from any verdict key. Deriving it by suffixing
// the verdict key ("upgraded_foo" + "_upgrading") would collide with the verdict
// key of a target vector literally named "foo_upgrading", so upgrading "foo"
// would read, write, and delete "foo_upgrading"'s verdict.
func TestUpgrade_MarkerKeyDisjointFromVerdictKey(t *testing.T) {
	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	// "foo_upgrading" is a committed (upgraded) vector.
	writeVerdict(t, db, "foo_upgrading", verdictUpgraded)

	// these three methods are all that markUpgrading/clear/present touch.
	foo := &dynamic{db: db, targetVector: "foo"}

	require.NoError(t, foo.markUpgrading())
	present, err := foo.upgradingMarkerPresent()
	require.NoError(t, err)
	assert.True(t, present, "foo's own marker must be set")
	assert.Equal(t, verdictUpgraded, readVerdict(t, db, "foo_upgrading"),
		"marking foo upgrading must not touch foo_upgrading's verdict")

	require.NoError(t, foo.clearUpgradingMarker())
	assert.Equal(t, verdictUpgraded, readVerdict(t, db, "foo_upgrading"),
		"clearing foo's marker must not delete foo_upgrading's verdict")

	present, err = foo.upgradingMarkerPresent()
	require.NoError(t, err)
	assert.False(t, present, "foo's marker must be cleared")
}

// TestUpgrade_OrdinaryUpgradeFailureRecoversInline covers an upgrade that fails
// WITHOUT a crash — the copy errors after >=1 batch has already written
// HNSW-format codes into the shared compressed bucket. The flat stage stays
// live, so doUpgrade must recover it in place (rebuild from raw, durable flush,
// clear marker) before it resumes serving: searches must succeed immediately,
// with no restart, and the in-progress marker must be gone.
func TestUpgrade_OrdinaryUpgradeFailureRecoversInline(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dimensions := 32
	vectorsSize := 2 * batchSize
	queriesSize := 20
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	dist := distancer.NewL2SquaredProvider()

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.RQ.Enabled = true
	fuc.RQ.Bits = 8
	hnswuc := hnswent.UserConfig{MaxConnections: 30, EFConstruction: 64, EF: 32, VectorCacheMaxObjects: 1_000_000}
	hnswuc.SetDefaults()
	hnswuc.BQ.Enabled = true

	config := Config{
		AllocChecker:                 memwatch.NewDummyMonitor(),
		RootPath:                     t.TempDir(),
		ID:                           "ordinary-upgrade-failure",
		Logger:                       logger,
		DistanceProvider:             dist,
		MakeCommitLoggerThunk:        hnsw.MakeNoopCommitLogger,
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[int(id)], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}
	uc := ent.UserConfig{Threshold: uint64(vectorsSize), Distance: dist.Type(), HnswUC: hnswuc, FlatUC: fuc}

	idx, err := New(config, uc, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	idx.PostStartup(ctx)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })
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

	// abort the copy with an ordinary error before the second batch (ids
	// 0..batchSize-1 have BQ codes in the shared bucket by now)
	idx.upgradeFn = func() error {
		return idx.upgradeUsing(func(target VectorIndex) VectorIndex {
			return &interruptingIndex{VectorIndex: target, onAddBatch: func(call int, ids []uint64) error {
				if call >= 2 {
					return assert.AnError
				}
				return nil
			}}
		})
	}

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
	require.False(t, idx.IsUpgraded(), "the upgrade failed, so it must not be marked upgraded")

	// No restart: the live flat index must already serve correctly again, and
	// the in-progress marker must have been cleared by the inline recovery.
	present, err := idx.upgradingMarkerPresent()
	require.NoError(t, err)
	assert.False(t, present, "inline recovery must clear the in-progress marker on an ordinary abort")
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoErrorf(t, err, "the live flat index must serve correctly right after an aborted upgrade (query %d)", i)
		assert.ElementsMatch(t, baseline[i], ids)
	}
}

// TestUpgradedOnDisk_InProgressMarkerReadsNotUpgraded pins that the offline
// reader treats an in-progress-upgrade sibling key as not-upgraded, for both an
// unnamed vector and a named one whose HNSW commit-log dir exists (which the
// dir-existence fallback would otherwise read as upgraded).
func TestUpgradedOnDisk_InProgressMarkerReadsNotUpgraded(t *testing.T) {
	rootPath := t.TempDir()
	db, err := bbolt.Open(filepath.Join(rootPath, ent.StateDBFileName), 0o666, nil)
	require.NoError(t, err)

	const unnamedTV = "" // no dir fallback
	const namedTV = "namedtarget"
	const namedID = "vectors_namedtarget"
	const committedTV = "committed"
	const committedID = "vectors_committed"

	markUpgradingInDB(t, db, unnamedTV)
	markUpgradingInDB(t, db, namedTV)
	// give the named vector an HNSW commit-log dir, the exact state that without
	// the sibling-key override would be inferred as upgraded
	require.NoError(t, os.MkdirAll(hnswCommitLogDirectory(rootPath, namedID), 0o777))
	// a committed upgrade whose start marker was never cleared: both keys present.
	writeVerdict(t, db, committedTV, verdictUpgraded)
	markUpgradingInDB(t, db, committedTV)
	require.NoError(t, db.Close()) // UpgradedOnDisk opens the file read-only itself

	up, err := UpgradedOnDisk(rootPath, "main", unnamedTV)
	require.NoError(t, err)
	assert.False(t, up, "unnamed vector with in-progress marker must read as not upgraded")

	up, err = UpgradedOnDisk(rootPath, namedID, namedTV)
	require.NoError(t, err)
	assert.False(t, up, "named vector with in-progress marker must read as not upgraded despite the HNSW dir")

	up, err = UpgradedOnDisk(rootPath, committedID, committedTV)
	require.NoError(t, err)
	assert.True(t, up, "committed verdict must win over a stale in-progress marker, matching init()")
}

// TestUpgrade_FailedRebuildKeepsMarker pins that a partial rebuild does not clear
// the in-progress marker: if re-encoding a vector fails, recovery errors out and
// leaves the marker set so the next restart retries, rather than sealing a
// corrupt bucket behind a "flat, all good" state.
func TestUpgrade_FailedRebuildKeepsMarker(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dimensions := 32
	vectors, _ := testinghelpers.RandomVecs(8, 0, dimensions)
	dist := distancer.NewL2SquaredProvider()

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.RQ.Enabled = true
	fuc.RQ.Bits = 8
	hnswuc := hnswent.UserConfig{MaxConnections: 30, EFConstruction: 64, EF: 32, VectorCacheMaxObjects: 1_000_000}
	hnswuc.SetDefaults()
	hnswuc.BQ.Enabled = true

	config := Config{
		AllocChecker:                 memwatch.NewDummyMonitor(),
		RootPath:                     t.TempDir(),
		ID:                           "failed-rebuild-keeps-marker",
		Logger:                       logger,
		DistanceProvider:             dist,
		MakeCommitLoggerThunk:        hnsw.MakeNoopCommitLogger,
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[int(id)], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}
	uc := ent.UserConfig{Threshold: 8, Distance: dist.Type(), HnswUC: hnswuc, FlatUC: fuc}

	store := testinghelpers.NewDummyStore(t)
	idx, err := New(config, uc, store)
	require.NoError(t, err)
	idx.PostStartup(ctx)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })
	for i := range vectors {
		require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
	}
	require.True(t, idx.Compressed())

	// mark an upgrade in progress, then make the compressed bucket unavailable so
	// the re-encode's store fails partway.
	require.NoError(t, idx.markUpgrading())
	require.NoError(t, store.ShutdownBucket(ctx, idx.getCompressedBucketName()))

	err = idx.recoverInterruptedUpgrade()
	require.Error(t, err, "a failed re-encode must surface as an error, not be silently logged")

	present, perr := idx.upgradingMarkerPresent()
	require.NoError(t, perr)
	assert.True(t, present, "a failed rebuild must keep the in-progress marker for a retry")
}

// TestUpgrade_OrphanedCompressedCodeReconciledOnRecovery covers an id deleted
// mid-copy: the copy snapshots the id into a batch, the id is then deleted (raw +
// compressed gone), and the batch's AddBatch re-writes a compressed code for it —
// an orphan (present in compressed, absent from raw). Recovery must reconcile it
// away, or a scan of the compressed bucket resurrects the deleted id (and trips
// on its wrong-length code).
func TestUpgrade_OrphanedCompressedCodeReconciledOnRecovery(t *testing.T) {
	ctx := context.Background()
	logger, _ := test.NewNullLogger()

	dimensions := 32
	vectorsSize := 2 * batchSize
	queriesSize := 20
	k := 10

	vectors, queries := testinghelpers.RandomVecs(vectorsSize, queriesSize, dimensions)
	dist := distancer.NewL2SquaredProvider()

	db, err := bbolt.Open(filepath.Join(t.TempDir(), "index.db"), 0o666, nil)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.RQ.Enabled = true
	fuc.RQ.Bits = 8
	hnswuc := hnswent.UserConfig{MaxConnections: 30, EFConstruction: 64, EF: 32, VectorCacheMaxObjects: 1_000_000}
	hnswuc.SetDefaults()
	hnswuc.BQ.Enabled = true

	config := Config{
		AllocChecker:                 memwatch.NewDummyMonitor(),
		RootPath:                     t.TempDir(),
		ID:                           "orphan-reconcile",
		Logger:                       logger,
		DistanceProvider:             dist,
		MakeCommitLoggerThunk:        hnsw.MakeNoopCommitLogger,
		VectorForIDThunk:             func(ctx context.Context, id uint64) ([]float32, error) { return vectors[int(id)], nil },
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(vectors),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		AsyncIndexingEnabled:         true,
	}
	uc := ent.UserConfig{Threshold: uint64(vectorsSize), Distance: dist.Type(), HnswUC: hnswuc, FlatUC: fuc}

	idx, err := New(config, uc, testinghelpers.NewDummyStore(t))
	require.NoError(t, err)
	idx.PostStartup(ctx)
	t.Cleanup(func() { idx.Shutdown(context.Background()) })
	for i := 0; i < vectorsSize; i++ {
		require.NoError(t, idx.Add(ctx, uint64(i), vectors[i]))
	}
	require.True(t, idx.Compressed())

	// delete the first id of batch 1 during that batch (it was already snapshotted
	// into the batch), so the batch's AddBatch re-writes its compressed code as an
	// orphan; then abort before batch 2. Delete via the underlying flat index to
	// avoid re-entering the dynamic read lock held by the copy.
	var victim uint64
	var deleted bool
	idx.upgradeFn = func() error {
		return idx.upgradeUsing(func(target VectorIndex) VectorIndex {
			return &interruptingIndex{VectorIndex: target, onAddBatch: func(call int, ids []uint64) error {
				if call == 1 {
					victim = ids[0]
					require.NoError(t, idx.index.Delete(victim))
					deleted = true
				}
				if call >= 2 {
					return assert.AnError
				}
				return nil
			}}
		})
	}

	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
	require.True(t, deleted, "the mid-copy delete must have run")
	require.False(t, idx.IsUpgraded(), "the upgrade failed, so it must not be marked upgraded")

	// inline recovery reconciled the orphan: no length-mismatch on scan, and the
	// deleted id is gone.
	require.False(t, idx.ContainsDoc(victim), "the deleted id must not survive as an orphan")
	for i := range queries {
		ids, _, err := idx.SearchByVector(ctx, queries[i], k, nil)
		require.NoErrorf(t, err, "scan must not trip on an orphaned compressed code (query %d)", i)
		for _, id := range ids {
			assert.NotEqualf(t, victim, id, "deleted id %d resurrected in query %d results", victim, i)
		}
	}
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
