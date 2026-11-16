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
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	bbolt "go.etcd.io/bbolt"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
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

// upgradeTestEnv keeps a dynamic index's files under one root, so a test can
// restart it or reopen it from a crash snapshot.
type upgradeTestEnv struct {
	root    string
	vectors [][]float32
	uc      ent.UserConfig
	target  string
	// hnsw derives its compressed bucket name from the id
	id string

	db      *bbolt.DB
	store   *lsmkv.Store
	idx     *dynamic
	logHook *test.Hook
}

func newUpgradeTestEnv(t *testing.T, root, targetVector string, vectors [][]float32,
	flatUC flatent.UserConfig, hnswUC hnswent.UserConfig,
) *upgradeTestEnv {
	t.Helper()
	env := &upgradeTestEnv{
		root:    root,
		vectors: vectors,
		target:  targetVector,
		id:      helpers.VectorIndexIDForTarget(targetVector),
		uc: ent.UserConfig{
			Threshold: uint64(len(vectors)),
			Distance:  distancer.NewL2SquaredProvider().Type(),
			HnswUC:    hnswUC,
			FlatUC:    flatUC,
		},
	}
	env.open(t)
	return env
}

// open loads the index from whatever is on disk under root.
func (e *upgradeTestEnv) open(t *testing.T) {
	t.Helper()
	logger, logHook := test.NewNullLogger()
	storeDir := filepath.Join(e.root, "store")
	rootPath := filepath.Join(e.root, "index")
	for _, d := range []string{storeDir, rootPath} {
		require.NoError(t, os.MkdirAll(d, 0o777))
	}

	db, err := bbolt.Open(filepath.Join(e.root, "index.db"), 0o666, nil)
	require.NoError(t, err)
	store := testinghelpers.NewDummyStoreFromFolder(storeDir, t)

	idx, err := New(Config{
		AllocChecker:     memwatch.NewDummyMonitor(),
		RootPath:         rootPath,
		ID:               e.id,
		TargetVector:     e.target,
		Logger:           logger,
		DistanceProvider: distancer.NewL2SquaredProvider(),
		MakeCommitLoggerThunk: func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(rootPath, e.id, logger,
				cyclemanager.NewCallbackGroupNoop(), opts...)
		},
		VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
			if int(id) >= len(e.vectors) || e.vectors[id] == nil {
				return nil, storobj.NewErrNotFoundf(id, "nil vec")
			}
			return e.vectors[id], nil
		},
		GetViewThunk:                 GetViewThunk,
		TempVectorForIDWithViewThunk: TempVectorForIDWithViewThunk(e.vectors),
		TombstoneCallbacks:           cyclemanager.NewCallbackGroupNoop(),
		SharedDB:                     db,
		MakeBucketOptions:            lsmkv.MakeNoopBucketOptions,
		HNSWWaitForCachePrefill:      true,
		AsyncIndexingEnabled:         true,
	}, e.uc, store)
	require.NoError(t, err)
	idx.PostStartup(context.Background())

	e.db, e.store, e.idx, e.logHook = db, store, idx, logHook
	t.Cleanup(e.shutdown)
}

// shutdown is a clean stop: everything buffered reaches the disk.
func (e *upgradeTestEnv) shutdown() {
	if e.idx == nil {
		return
	}
	ctx := context.Background()
	e.idx.Flush()
	e.idx.Shutdown(ctx)
	e.store.Shutdown(ctx)
	e.db.Close()
	e.idx = nil
}

func (e *upgradeTestEnv) restart(t *testing.T) {
	t.Helper()
	e.shutdown()
	e.open(t)
}

// crash reopens the index from a copy of its files taken while still open, so
// whatever sits in a write buffer is lost.
func (e *upgradeTestEnv) crash(t *testing.T) *upgradeTestEnv {
	t.Helper()
	return e.crashWithConfig(t, e.uc)
}

func (e *upgradeTestEnv) crashWithConfig(t *testing.T, uc ent.UserConfig) *upgradeTestEnv {
	t.Helper()
	snap := t.TempDir()
	require.NoError(t, snapshotTree(e.root, snap))
	crashed := &upgradeTestEnv{root: snap, vectors: e.vectors, uc: uc, target: e.target, id: e.id}
	crashed.open(t)
	return crashed
}

func (e *upgradeTestEnv) addAll(t *testing.T) {
	t.Helper()
	for i, vec := range e.vectors {
		require.NoError(t, e.idx.Add(context.Background(), uint64(i), vec))
	}
}

func (e *upgradeTestEnv) search(t *testing.T, queries [][]float32, k int, msg string) [][]uint64 {
	t.Helper()
	res := make([][]uint64, len(queries))
	for i, q := range queries {
		ids, _, err := e.idx.SearchByVector(context.Background(), q, k, nil)
		require.NoErrorf(t, err, "%s: query %d", msg, i)
		res[i] = ids
	}
	return res
}

// upgrade runs the flat→HNSW upgrade to its end. onAddBatch, when non-nil, is
// called before each copy batch.
func (e *upgradeTestEnv) upgrade(t *testing.T, onAddBatch func(call int, ids []uint64) error) {
	t.Helper()
	idx := e.idx
	idx.upgradeFn = func() error {
		return idx.upgradeUsing(func(target VectorIndex) VectorIndex {
			return &interruptingIndex{VectorIndex: target, onAddBatch: onAddBatch}
		})
	}
	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done
}

// compressedCodeLengths returns the byte length of every value in the
// compressed bucket, or nil if the bucket is empty or does not exist.
func (e *upgradeTestEnv) compressedCodeLengths() []int {
	bucket := e.store.Bucket(e.idx.getCompressedBucketName())
	if bucket == nil {
		return nil
	}
	var lengths []int
	cursor := bucket.Cursor()
	defer cursor.Close()
	for k, v := cursor.First(); k != nil; k, v = cursor.Next() {
		lengths = append(lengths, len(v))
	}
	return lengths
}

func flatRQ8() flatent.UserConfig {
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.RQ.Enabled = true
	fuc.RQ.Bits = 8
	return fuc
}

func flatBQ() flatent.UserConfig {
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	fuc.BQ.Enabled = true
	return fuc
}

func flatUncompressed() flatent.UserConfig {
	fuc := flatent.UserConfig{}
	fuc.SetDefaults()
	return fuc
}

func hnswWith(mutate func(*hnswent.UserConfig)) hnswent.UserConfig {
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

func hnswBQ() hnswent.UserConfig {
	return hnswWith(func(uc *hnswent.UserConfig) { uc.BQ.Enabled = true })
}

func hnswRQ8() hnswent.UserConfig {
	return hnswWith(func(uc *hnswent.UserConfig) { uc.RQ.Enabled = true; uc.RQ.Bits = 8 })
}

type upgradeQuantizerCase struct {
	name         string
	targetVector string
	flatUC       flatent.UserConfig
	hnswUC       hnswent.UserConfig
}

func upgradeQuantizerCases() []upgradeQuantizerCase {
	return []upgradeQuantizerCase{
		// flat reads 80-byte codes, hnsw writes 8-byte ones: "8 vs 80"
		{name: "flat RQ8 -> hnsw BQ", flatUC: flatRQ8(), hnswUC: hnswBQ()},
		{name: "flat BQ -> hnsw RQ8", flatUC: flatBQ(), hnswUC: hnswRQ8()},
		{name: "named vector, flat RQ8 -> hnsw BQ", targetVector: "named", flatUC: flatRQ8(), hnswUC: hnswBQ()},
		// same code length, but each stage has its own rotation: a mix-up
		// returns wrong results without any error
		{name: "flat RQ8 -> hnsw RQ8", flatUC: flatRQ8(), hnswUC: hnswRQ8()},
		{name: "flat BQ -> hnsw BQ", flatUC: flatBQ(), hnswUC: hnswBQ()},
		{name: "flat uncompressed -> hnsw BQ", flatUC: flatUncompressed(), hnswUC: hnswBQ()},
		{name: "flat uncompressed -> hnsw RQ8", flatUC: flatUncompressed(), hnswUC: hnswRQ8()},
		{name: "flat RQ8 -> hnsw uncompressed", flatUC: flatRQ8(), hnswUC: hnswWith(func(*hnswent.UserConfig) {})},
		{
			// evicted vectors are read back during the copy
			name:   "flat RQ8 -> hnsw BQ, small hnsw cache",
			flatUC: flatRQ8(),
			hnswUC: hnswWith(func(uc *hnswent.UserConfig) { uc.BQ.Enabled = true; uc.VectorCacheMaxObjects = 100 }),
		},
	}
}

// The flat stage serves during the copy and shares the compressed bucket with
// the new HNSW, which must not write to it before the upgrade commits.
func TestUpgrade_SearchDuringQuantizedCopy(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			const k = 10
			vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)

			baseline := env.search(t, queries, k, "before the upgrade")
			flatCodes := env.compressedCodeLengths()

			// the first batch is in the HNSW, the flat stage still serves
			var during [][]uint64
			var searchErrs []error
			var codesDuring []int
			env.upgrade(t, func(call int, ids []uint64) error {
				if call != 2 {
					return nil
				}
				codesDuring = env.compressedCodeLengths()
				for _, q := range queries {
					ids, _, err := env.idx.SearchByVector(context.Background(), q, k, nil)
					searchErrs = append(searchErrs, err)
					during = append(during, ids)
				}
				return nil
			})
			require.True(t, env.idx.IsUpgraded(), "the upgrade must complete")

			require.Len(t, searchErrs, len(queries))
			for i, err := range searchErrs {
				require.NoErrorf(t, err, "search %d during the upgrade copy", i)
				assert.ElementsMatchf(t, baseline[i], during[i],
					"search %d during the upgrade copy must return what the flat stage returned before", i)
			}
			assert.Equal(t, flatCodes, codesDuring,
				"the copy must leave the flat stage's compressed bucket alone")

			// after the commit the bucket holds only the HNSW's vectors
			upgraded := env.search(t, queries, k, "after the upgrade committed")
			codes := env.compressedCodeLengths()
			if env.idx.Compressed() {
				if tc.hnswUC.VectorCacheMaxObjects >= int(env.uc.Threshold) {
					require.Len(t, codes, len(vectors), "every vector must be persisted compressed")
				}
				require.NotEmpty(t, codes)
				for _, l := range codes {
					require.Equal(t, codes[0], l, "compressed vectors of mixed length")
				}
			} else {
				require.Empty(t, codes, "the flat stage's compressed vectors must be gone")
			}

			// a restart fills the HNSW cache from the bucket
			env.restart(t)
			require.True(t, env.idx.IsUpgraded())
			restarted := env.search(t, queries, k, "after a restart")
			for i := range queries {
				assert.ElementsMatchf(t, upgraded[i], restarted[i], "query %d after a restart", i)
			}
		})
	}
}

// A crash right after the commit loses what was still buffered. The flat stage
// is gone, so the HNSW must come back complete: the graph from its commit log,
// missing compressed vectors re-encoded from the raw ones.
func TestUpgrade_CrashAfterCommit(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			// few enough vectors that nothing leaves a write buffer on its own
			vectors, queries := testinghelpers.RandomVecs(16, 5, 32)
			k := len(vectors)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)
			// flat compressed vectors are on disk; none may survive the crash
			env.restart(t)
			env.upgrade(t, nil)
			require.True(t, env.idx.IsUpgraded(), "the upgrade must complete")
			upgraded := env.search(t, queries, k, "after the upgrade committed")

			crashed := env.crash(t)
			require.True(t, crashed.idx.IsUpgraded(), "the committed upgrade must survive the crash")
			restarted := crashed.search(t, queries, k, "after the crash")
			for i := range queries {
				require.Lenf(t, restarted[i], len(vectors), "query %d must reach every vector after the crash", i)
				assert.Equalf(t, upgraded[i], restarted[i], "query %d after the crash", i)
			}
		})
	}
}

// A crash between the bucket removal and the verdict boots the flat stage,
// which must rebuild its compressed vectors from the raw ones.
func TestUpgrade_CrashBetweenBucketRemovalAndVerdict(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			const k = 10
			vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)
			baseline := env.search(t, queries, k, "before the upgrade")

			// the residue of that crash: marker set, bucket gone, no verdict
			require.NoError(t, env.idx.markUpgrading())
			bucketDir := ""
			if bucket := env.store.Bucket(env.idx.getCompressedBucketName()); bucket != nil {
				bucketDir = bucket.GetDir()
			}
			env.shutdown()
			if bucketDir != "" {
				require.NoError(t, os.RemoveAll(bucketDir))
			}

			env.open(t)
			require.False(t, env.idx.IsUpgraded(), "without a verdict the index must stay flat")
			present, err := env.idx.upgradingMarkerPresent()
			require.NoError(t, err)
			assert.False(t, present, "a finished repair must clear the marker")
			recovered := env.search(t, queries, k, "after the crash")
			for i := range queries {
				assert.ElementsMatchf(t, baseline[i], recovered[i], "query %d after the crash", i)
			}

			// and the retried upgrade goes through
			env.upgrade(t, nil)
			require.True(t, env.idx.IsUpgraded(), "the retried upgrade must complete")
			env.search(t, queries, k, "after the retried upgrade")
		})
	}
}

// A failed verdict write leaves the flat stage live without its compressed
// bucket; it must get it back right away, not at the next restart.
func TestUpgrade_FailedCommitRestoresFlatStage(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			const k = 10
			vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)
			baseline := env.search(t, queries, k, "before the upgrade")

			// closing the state DB during the copy fails the verdict write
			env.upgrade(t, func(call int, ids []uint64) error {
				if call == 2 {
					require.NoError(t, env.db.Close())
				}
				return nil
			})
			require.False(t, env.idx.IsUpgraded(), "the upgrade must not commit without its verdict")

			recovered := env.search(t, queries, k, "after the failed commit")
			for i := range queries {
				assert.ElementsMatchf(t, baseline[i], recovered[i], "query %d after the failed commit", i)
			}
			_, err := os.Stat(hnswCommitLogDirectory(filepath.Join(env.root, "index"), env.id))
			assert.True(t, os.IsNotExist(err), "the aborted hnsw must not leave a commit log behind")

			// the marker could not be cleared, so the next start repairs again
			env.restart(t)
			require.False(t, env.idx.IsUpgraded())
			restarted := env.search(t, queries, k, "after a restart")
			for i := range queries {
				assert.ElementsMatchf(t, baseline[i], restarted[i], "query %d after a restart", i)
			}
		})
	}
}

// A copy that panics is retried in the same process. The retry must start from
// nothing: an HNSW restored from the first attempt's commit log would bring a
// compressor that writes into the bucket the flat stage still reads.
func TestUpgrade_RetryAfterPanickedCopy(t *testing.T) {
	t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			const k = 10
			vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)
			baseline := env.search(t, queries, k, "before the upgrade")

			env.upgrade(t, func(call int, ids []uint64) error {
				if call == 2 {
					panic("copy failed")
				}
				return nil
			})
			testinghelpers.WaitForRecoveredPanic(t, env.logHook)
			require.False(t, env.idx.IsUpgraded(), "the panicked upgrade must not commit")
			_, err := os.Stat(hnswCommitLogDirectory(filepath.Join(env.root, "index"), env.id))
			assert.True(t, os.IsNotExist(err), "the panicked copy must not leave a commit log behind")

			var during [][]uint64
			env.upgrade(t, func(call int, ids []uint64) error {
				if call == 2 {
					during = env.search(t, queries, k, "during the retried copy")
				}
				return nil
			})
			require.True(t, env.idx.IsUpgraded(), "the retried upgrade must complete")
			require.Len(t, during, len(queries))
			for i := range queries {
				assert.ElementsMatchf(t, baseline[i], during[i], "query %d during the retried copy", i)
			}

			upgraded := env.search(t, queries, k, "after the retried upgrade")
			env.restart(t)
			restarted := env.search(t, queries, k, "after a restart")
			for i := range queries {
				assert.ElementsMatchf(t, upgraded[i], restarted[i], "query %d after a restart", i)
			}
		})
	}
}

// The HNSW is built uncompressed and compressed once it is live. A crash in
// between leaves a committed upgrade whose config asks for a quantizer that
// has written nothing yet.
func TestUpgrade_CrashBeforeCompression(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			vectors, queries := testinghelpers.RandomVecs(16, 5, 32)
			k := len(vectors)
			// what is on disk at that point is what an upgrade to an uncompressed
			// hnsw leaves behind
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC,
				uncompressedDuringCopy(tc.hnswUC))
			env.addAll(t)
			env.restart(t)
			env.upgrade(t, nil)
			require.True(t, env.idx.IsUpgraded(), "the upgrade must complete")

			uc := env.uc
			uc.HnswUC = tc.hnswUC
			crashed := env.crashWithConfig(t, uc)
			require.True(t, crashed.idx.IsUpgraded(), "the committed upgrade must survive the crash")
			for i, ids := range crashed.search(t, queries, k, "after the crash") {
				require.Lenf(t, ids, len(vectors), "query %d must reach every vector after the crash", i)
			}

			// the indexing queue triggers the compression that never ran
			if should, _ := crashed.idx.ShouldUpgrade(); should && !crashed.idx.Upgraded() {
				done := make(chan struct{})
				require.NoError(t, crashed.idx.Upgrade(func() { close(done) }))
				<-done
			}
			wantCompressed := tc.hnswUC.BQ.Enabled || tc.hnswUC.RQ.Enabled
			require.Equal(t, wantCompressed, crashed.idx.Compressed())
			for i, ids := range crashed.search(t, queries, k, "after compressing") {
				require.Lenf(t, ids, len(vectors), "query %d must reach every vector once compressed", i)
			}
		})
	}
}

// An earlier upgrade attempt can leave a compressed bucket on disk that an
// uncompressed flat stage never loads. The commit must remove it too, or the
// hnsw would later load it and read its rows as its own.
func TestUpgrade_RemovesUnloadedCompressedBucket(t *testing.T) {
	vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
	env := newUpgradeTestEnv(t, t.TempDir(), "", vectors, flatUncompressed(), hnswBQ())
	env.addAll(t)

	name := env.idx.getCompressedBucketName()
	require.NoError(t, env.store.CreateOrLoadBucket(context.Background(), name,
		lsmkv.MakeNoopBucketOptions(lsmkv.StrategyReplace)...))
	stale := make([]byte, 8)
	binary.BigEndian.PutUint64(stale, uint64(10*len(vectors)))
	require.NoError(t, env.store.Bucket(name).Put(stale, []byte{1, 2, 3}))
	require.NoError(t, env.store.ShutdownBucket(context.Background(), name))

	env.upgrade(t, nil)
	require.True(t, env.idx.IsUpgraded(), "the upgrade must complete")
	env.search(t, queries, 10, "after the upgrade committed")

	codes := env.compressedCodeLengths()
	require.Len(t, codes, len(vectors), "only the hnsw's compressed vectors may remain")
	for _, l := range codes {
		require.Equal(t, codes[0], l, "compressed vectors of mixed length")
	}
}

// A compression that did not happen right after the swap is picked up by the
// indexing queue, which asks ShouldUpgrade and then calls Upgrade. A failed
// attempt must leave the queue alone for a while, and must not be the last.
func TestUpgrade_QueueRetriesMissedCompression(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		if !tc.hnswUC.BQ.Enabled && !tc.hnswUC.RQ.Enabled {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)

			idx := env.idx
			idx.upgradeFn = func() error {
				index, err := idx.buildUpgraded(nil)
				if err != nil {
					return err
				}
				return idx.installUpgraded(index)
			}
			upgrade := func() {
				done := make(chan struct{})
				require.NoError(t, idx.Upgrade(func() { close(done) }))
				<-done
			}
			upgrade()
			require.True(t, idx.IsUpgraded())
			require.False(t, idx.Compressed())

			// a file in place of the compressed bucket's directory fails its creation
			blocker := filepath.Join(env.store.GetDir(), idx.getCompressedBucketName())
			require.NoError(t, os.WriteFile(blocker, nil, 0o600))
			should, at := idx.ShouldUpgrade()
			require.True(t, should, "the queue must be asked to compress")
			require.False(t, idx.Upgraded())
			require.Greater(t, idx.AlreadyIndexed(), uint64(at))
			upgrade()
			require.False(t, idx.Compressed())
			should, _ = idx.ShouldUpgrade()
			require.False(t, should, "a failed attempt must not be repeated on every queue round")
			env.search(t, queries, 10, "after the failed compression")

			require.NoError(t, os.Remove(blocker))
			idx.compressRetryAfter.Store(0)
			should, _ = idx.ShouldUpgrade()
			require.True(t, should, "the queue must be asked again after the cool-down")
			upgrade()
			require.True(t, idx.Compressed())
			env.search(t, queries, 10, "after the retried compression")
		})
	}
}

// The config applied after the compression must be the latest one, so updates
// have to be tracked after the upgrade as well.
func TestUpgrade_TracksConfigUpdatesAfterUpgrade(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecs(2*batchSize, 0, 32)
	env := newUpgradeTestEnv(t, t.TempDir(), "", vectors, flatRQ8(), hnswBQ())
	env.addAll(t)
	env.upgrade(t, nil)
	require.True(t, env.idx.IsUpgraded())

	updated := env.uc
	updated.HnswUC.VectorCacheMaxObjects = 77
	require.NoError(t, env.idx.UpdateUserConfig(updated, func() {}))
	require.Equal(t, 77, env.idx.uc.HnswUC.VectorCacheMaxObjects)
}

// A backup asks UpgradeInProgress. The answer must stay yes until the
// compression that follows the swap is done.
func TestUpgrade_InProgressUntilCompressed(t *testing.T) {
	vectors, _ := testinghelpers.RandomVecs(2*batchSize, 0, 32)
	env := newUpgradeTestEnv(t, t.TempDir(), "", vectors, flatRQ8(), hnswBQ())
	env.addAll(t)

	idx := env.idx
	var afterInstall, afterCompress bool
	idx.upgradeFn = func() error {
		index, err := idx.buildUpgraded(nil)
		if err != nil {
			return err
		}
		if err := idx.installUpgraded(index); err != nil {
			return err
		}
		afterInstall = idx.UpgradeInProgress()
		idx.compressUpgraded(index)
		afterCompress = idx.UpgradeInProgress()
		return nil
	}
	done := make(chan struct{})
	require.NoError(t, idx.Upgrade(func() { close(done) }))
	<-done

	require.True(t, afterInstall, "between the swap and the compression")
	require.True(t, afterCompress, "until the upgrade returns")
	require.False(t, idx.UpgradeInProgress(), "once the upgrade returned")
	require.True(t, idx.Compressed())
}
