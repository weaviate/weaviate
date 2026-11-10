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

// upgradeTestEnv is a dynamic index whose every on-disk part (LSM store, HNSW
// commit log, state DB) lives under one root, so a test can restart it from
// the same files or from a crash snapshot of them.
type upgradeTestEnv struct {
	root    string
	vectors [][]float32
	uc      ent.UserConfig
	target  string
	// the id a shard gives the index: the hnsw stage derives its target vector,
	// and with it the compressed bucket's name, from it
	id string

	db    *bbolt.DB
	store *lsmkv.Store
	idx   *dynamic
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
	logger, _ := test.NewNullLogger()
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

	e.db, e.store, e.idx = db, store, idx
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

// crash returns the index as a process started on the files a crash would
// leave: a copy of the on-disk state taken while everything is still open, so
// whatever sits in a write buffer is lost.
func (e *upgradeTestEnv) crash(t *testing.T) *upgradeTestEnv {
	t.Helper()
	snap := t.TempDir()
	require.NoError(t, snapshotTree(e.root, snap))
	crashed := &upgradeTestEnv{root: snap, vectors: e.vectors, uc: e.uc, target: e.target, id: e.id}
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
			// the hnsw cache cannot hold the whole index, so vectors evicted
			// during the copy are read back before the bucket is the hnsw's
			name:   "flat RQ8 -> hnsw BQ, small hnsw cache",
			flatUC: flatRQ8(),
			hnswUC: hnswWith(func(uc *hnswent.UserConfig) { uc.BQ.Enabled = true; uc.VectorCacheMaxObjects = 100 }),
		},
	}
}

// The flat stage keeps serving while the upgrade copies its vectors into the
// new HNSW. Both stages name the same compressed bucket, so the HNSW must not
// write to it before the upgrade commits: the flat stage would read vectors
// compressed by another quantizer and fail ("vector lengths don't match: 8 vs
// 80"), or with equally long codes silently return wrong results.
func TestUpgrade_SearchDuringQuantizedCopy(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			const k = 10
			vectors, queries := testinghelpers.RandomVecs(2*batchSize, 5, 32)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)

			baseline := env.search(t, queries, k, "before the upgrade")
			flatCodes := env.compressedCodeLengths()

			// Before the second copy batch the first batch is already in the HNSW
			// and the flat stage still serves.
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

			// After the commit the bucket holds the HNSW's compressed vectors and
			// nothing else.
			upgraded := env.search(t, queries, k, "after the upgrade committed")
			codes := env.compressedCodeLengths()
			if env.idx.Compressed() {
				require.Len(t, codes, len(vectors), "every vector must be persisted compressed")
				for _, l := range codes {
					require.Equal(t, codes[0], l, "compressed vectors of mixed length")
				}
			} else {
				require.Empty(t, codes, "the flat stage's compressed vectors must be gone")
			}

			// ... so a restart, which fills the HNSW cache from the bucket, serves
			// the same results.
			env.restart(t)
			require.True(t, env.idx.IsUpgraded())
			restarted := env.search(t, queries, k, "after a restart")
			for i := range queries {
				assert.ElementsMatchf(t, upgraded[i], restarted[i], "query %d after a restart", i)
			}
		})
	}
}

// A process that dies right after the upgrade committed loses whatever was
// still buffered: the HNSW's compressed vectors on their way into the bucket,
// and — unless the commit flushed it — the tail of the HNSW commit log. The
// flat stage is gone by then, so the HNSW must come back complete on its own:
// the whole graph from the commit log, and each compressed vector missing from
// the bucket compressed again from the raw vector.
func TestUpgrade_CrashAfterCommit(t *testing.T) {
	for _, tc := range upgradeQuantizerCases() {
		t.Run(tc.name, func(t *testing.T) {
			// few enough vectors that nothing leaves a write buffer on its own
			vectors, queries := testinghelpers.RandomVecs(16, 5, 32)
			k := len(vectors)
			env := newUpgradeTestEnv(t, t.TempDir(), tc.targetVector, vectors, tc.flatUC, tc.hnswUC)
			env.addAll(t)
			// The flat stage's compressed vectors are safely on disk, in the
			// bucket the HNSW is about to take over: none may survive the crash,
			// or the HNSW would read them as its own.
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

// The commit removes the flat stage's compressed bucket before it records the
// verdict. A crash in between boots the flat stage again, which must get its
// compressed vectors back from the raw ones.
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

// A commit that fails to record the verdict has already removed the flat
// stage's compressed bucket. The flat stage stays live, so it must get the
// bucket back right away, not at the next restart.
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

			// The marker could not be cleared with the state DB closed, so the
			// next start repeats the repair, and serves the same results.
			env.restart(t)
			require.False(t, env.idx.IsUpgraded())
			restarted := env.search(t, queries, k, "after a restart")
			for i := range queries {
				assert.ElementsMatchf(t, baseline[i], restarted[i], "query %d after a restart", i)
			}
		})
	}
}
