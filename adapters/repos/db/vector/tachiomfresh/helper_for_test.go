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

package tachiomfresh

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/queue"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hfresh"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/storobj"
	ent "github.com/weaviate/weaviate/entities/vectorindex/tachiomfresh"
	"github.com/weaviate/weaviate/usecases/memwatch"
	"github.com/weaviate/weaviate/usecases/monitoring"
)

// copied from hfresh.muveraTestStore, renamed: it backs MultiVectorForIDThunk.
// Guarded by a mutex, as hfresh's testVectorStore is:
// background splits, merges and reassigns read it while a test writes.
type multiVectorTestStore struct {
	mu           sync.RWMutex
	multiVectors map[uint64][][]float32
	// deleted holds the ids deleteMultiVector removed: those read as an
	// object the shard no longer holds, any other missing id as a test bug
	deleted map[uint64]struct{}
}

func newMultiVectorTestStore() *multiVectorTestStore {
	return &multiVectorTestStore{
		multiVectors: make(map[uint64][][]float32),
		deleted:      make(map[uint64]struct{}),
	}
}

func (s *multiVectorTestStore) storeMultiVector(id uint64, vecs [][]float32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.multiVectors[id] = vecs
	delete(s.deleted, id)
}

// deleteMultiVector drops the doc record, as the shard does on a delete.
func (s *multiVectorTestStore) deleteMultiVector(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.multiVectors, id)
	s.deleted[id] = struct{}{}
}

func (s *multiVectorTestStore) getMultiVector(id uint64) ([][]float32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vecs, ok := s.multiVectors[id]
	if !ok {
		if _, gone := s.deleted[id]; gone {
			// the error a shard returns for an object it no longer holds
			return nil, storobj.NewErrNotFoundf(id, "multi-vector not found for id %d", id)
		}
		return nil, fmt.Errorf("multi-vector %d not stored in this test", id)
	}
	return vecs, nil
}

// getMultiVectorInto is the buffered thunk of the unit tests: the
// document's tokens copied into the container's backing array, grown when
// it is short, as storobj.MultiVectorFromBinaryInto does for a shard.
func (s *multiVectorTestStore) getMultiVectorInto(id uint64, container *common.VectorSlice) ([][]float32, error) {
	vecs, err := s.getMultiVector(id)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, v := range vecs {
		total += len(v)
	}
	if cap(container.Slice) < total {
		container.Slice = make([]float32, total)
	}
	flat := container.Slice[:total]
	out := make([][]float32, len(vecs))
	off := 0
	for i, v := range vecs {
		copy(flat[off:off+len(v)], v)
		out[i] = flat[off : off+len(v) : off+len(v)]
		off += len(v)
	}
	container.Mem, container.Slice = flat, flat
	return out, nil
}

// getToken is the token thunk of the unit tests: token pos of
// document id, a copy, as a store hands out memory of its own.
func (s *multiVectorTestStore) getToken(id uint64, pos int) ([]float32, error) {
	vecs, err := s.getMultiVector(id)
	if err != nil {
		return nil, err
	}
	if pos < 0 || pos >= len(vecs) {
		return nil, errors.Errorf("doc %d has %d tokens but token %d is asked", id, len(vecs), pos)
	}
	return slices.Clone(vecs[pos]), nil
}

// copied from hfresh_test.go
type noopBucketView struct{}

func (n *noopBucketView) ReleaseView() {}

// copied from hfresh_test.go
func makeNoopCommitLogger(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
	return &hnsw.NoopCommitLogger{}, nil
}

// testTyped gives the documents of the tests several token types by
// position (testTokenType); without it every token has testPostingType, the type
// of the postings the tests build by hand, so the index has one group, which
// the hand-built postings join. TestTyped sets it to run the tests it lists a
// second time with several types. The tests do not run in
// parallel, so a package variable is enough.
var testTyped bool

// testPostingType is the type of the postings and one-token documents the
// tests build by hand.
const testPostingType uint32 = 7

// testTokenType is the token id the test store gives token pos of every
// document (see testTyped).
func testTokenType(pos int) uint32 {
	if testTyped {
		return uint32(pos % 3)
	}
	return testPostingType
}

// addMulti inserts through AddMulti, which reads the token ids from the
// test store, as a shard's index reads them from the stored object.
func (tf *testIndex) addMulti(ctx context.Context, docID uint64, vectors [][]float32) error {
	return tf.Index.AddMulti(ctx, docID, vectors)
}

type testIndex struct {
	Index   *TachiomFresh
	Logs    *test.Hook
	mvStore *multiVectorTestStore
	store   *lsmkv.Store
	// widenings counts how many times a filtered search doubled kCentroids.
	widenings *atomic.Uint64
	// centeredOn is the number of tokens the mean was computed from.
	centeredOn *atomic.Uint64
	// metricsClass is the class name the index's metrics are labelled with.
	metricsClass string
}

// cleanupCount reads the metric of the cleanups that rewrote a posting.
func (tf *testIndex) cleanupCount(t *testing.T) uint64 {
	t.Helper()
	gauge, err := monitoring.GetMetrics().VectorIndexBackgroundOperationsCount.GetMetricWith(prometheus.Labels{
		"class_name": tf.metricsClass, "shard_name": "", "operation": "cleanup",
	})
	require.NoError(t, err)
	return uint64(testutil.ToFloat64(gauge))
}

// metricsClasses numbers the test indexes: each gets a class name of its own,
// so its metrics start at zero.
var metricsClasses atomic.Uint64

// setCenterTokens changes the staging threshold of an index nothing has been
// inserted into. Production builds at defaultCenterTokens. 0 means no staging
// and no centering, the configuration the package tests exercise. A restored
// index keeps its phase.
func (t *TachiomFresh) setCenterTokens(n uint64) {
	t.initMu.Lock()
	defer t.initMu.Unlock()
	t.centerTokens = n
	if !t.initDone {
		t.built.Store(n == 0)
	}
}

// close shuts the index and its store down, so newTestIndexAt can reopen
// them from the same directory.
func (tf *testIndex) close(t *testing.T) {
	t.Helper()
	require.NoError(t, tf.Index.Shutdown(t.Context()))
	require.NoError(t, tf.store.Shutdown(t.Context()))
}

// waitForMaintenance blocks until all background queues are drained.
// Callers must stop issuing foreground writes before calling it.
// copied from hfresh's waitForMaintenance
func (t *TachiomFresh) waitForMaintenance(tb testing.TB) {
	tb.Helper()

	queues := []*queue.DiskQueue{
		t.taskQueue.splitQueue,
		t.taskQueue.mergeQueue,
		t.taskQueue.reassignQueue,
	}

	for {
		require.NoError(tb, t.taskQueue.Flush())
		t.scheduler.Schedule(tb.Context())

		for _, q := range queues {
			require.NoError(tb, q.Wait(tb.Context()))
		}
		if t.taskQueue.Size() == 0 {
			return
		}
	}
}

// pauseMaintenance stops the split, merge and reassign queues from running
// tasks until the test ends. Tasks can still be enqueued. A test that
// compares two searches needs it: a search enqueues a merge for a posting
// below the minimum size, and the scheduler may run it between the two
// searches, which changes the postings the second one reaches.
func (t *TachiomFresh) pauseMaintenance(tb testing.TB) {
	tb.Helper()

	for _, q := range []*queue.DiskQueue{
		t.taskQueue.splitQueue,
		t.taskQueue.mergeQueue,
		t.taskQueue.reassignQueue,
	} {
		require.NoError(tb, q.Pause(tb.Context()))
		tb.Cleanup(q.Resume)
	}
}

// testIndexOption mutates the index config before the index is created.
// copied from hfresh's helper_for_test.go
type testIndexOption func(*hfresh.Config)

// withoutTokenThunk leaves the index with the document thunk only: the
// read path the token reads must match (token_read_test.go).
func withoutTokenThunk() testIndexOption {
	return func(cfg *hfresh.Config) {
		cfg.TokenForIDThunk = nil
	}
}

// withoutTempThunk leaves stage 3 with the plain document thunk: the read
// path the buffered reads must match (search_test.go).
func withoutTempThunk() testIndexOption {
	return func(cfg *hfresh.Config) {
		cfg.TempMultiVectorForIDThunk = nil
	}
}

// withDistanceProvider sets the distance provider for both the index itself
// and its centroid HNSW, keeping the two consistent like production wiring
// does. copied from hfresh's helper_for_test.go
func withDistanceProvider(provider distancer.Provider) testIndexOption {
	return func(cfg *hfresh.Config) {
		cfg.DistanceProvider = provider
		cfg.Centroids.HNSWConfig.DistanceProvider = provider
	}
}

// withID sets the index id, which names the index's buckets. A shard passes
// the vectors bucket name of the target vector.
func withID(id string) testIndexOption {
	return func(cfg *hfresh.Config) {
		cfg.ID = id
	}
}

// hfreshEFConstruction is the centroid HNSW efConstruction hfresh uses.
const hfreshEFConstruction = 64

// newTestIndex wires an index on top of a dummy LSM store and a noop-bucket
// centroid HNSW, the way hfresh's newTestIndex does, with the multi-vector
// thunk served by the returned test store.
func newTestIndex(t *testing.T, maxPostingSize uint32, kCentroids, kDocsToScore, efConstruction int, opts ...testIndexOption) testIndex {
	t.Helper()
	return newTestIndexAt(t, "", maxPostingSize, kCentroids, kDocsToScore, efConstruction, opts...)
}

// newTestIndexAt is newTestIndex on a directory that outlives the test when
// dir is not empty: the LSM store in dir/store, the task queues and the
// centroid HNSW's commit log in dir/root, wired as hfresh's makeHFreshConfig
// does for its restart test. Calling it again on the same dir reopens the
// index (after close). With dir empty everything lives in temp dirs and the
// centroid HNSW logs nothing.
func newTestIndexAt(t *testing.T, dir string, maxPostingSize uint32, kCentroids, kDocsToScore, efConstruction int, opts ...testIndexOption) testIndex {
	t.Helper()

	cfg, uc, store, mvStore, hook := newTestConfigAt(t, dir, maxPostingSize, kCentroids, kDocsToScore, efConstruction, opts...)
	index, err := New(cfg, uc, store)
	require.NoError(t, err)
	widenings, centeredOn := &atomic.Uint64{}, &atomic.Uint64{}
	index.opts.onWiden = func() { widenings.Add(1) }
	index.opts.onCentered = centeredOn.Store
	if index.center != nil {
		// restored: the mean came from the threshold's tokens
		centeredOn.Store(index.centerTokens)
	}
	// the package tests exercise the built index, so nothing is staged
	// and nothing centered unless a test sets a threshold
	// (setCenterTokens). This runs on a reopened index too: one restored
	// in the staging phase keeps its phase but has its threshold set to 0,
	// so a test that reopens a staging index sets the threshold again.
	index.setCenterTokens(0)

	// A test that fails, or never calls close, leaves no index running into
	// the next test: queued tasks would keep calling the test's hooks. The
	// errors of an index or store the test closed already are ignored.
	t.Cleanup(func() {
		_ = index.Shutdown(context.Background())
		_ = store.Shutdown(context.Background())
	})

	return testIndex{
		Index:   index,
		Logs:    hook,
		mvStore: mvStore,
		store:   store,

		widenings:    widenings,
		centeredOn:   centeredOn,
		metricsClass: cfg.ClassName,
	}
}

// newTestConfigAt is what newTestIndexAt passes to New, for a test that calls
// New itself.
func newTestConfigAt(t *testing.T, dir string, maxPostingSize uint32, kCentroids, kDocsToScore, efConstruction int, opts ...testIndexOption) (*hfresh.Config, ent.UserConfig, *lsmkv.Store, *multiVectorTestStore, *test.Hook) {
	t.Helper()

	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	cfg := hfresh.DefaultConfig()
	cfg.RootPath = t.TempDir()
	cfg.ID = "tachiomfresh"
	cfg.DistanceProvider = distancer.NewDotProductProvider()
	cfg.Centroids.HNSWConfig = &hnsw.Config{
		RootPath:              t.TempDir(),
		ID:                    "tachiomfresh",
		MakeCommitLoggerThunk: makeNoopCommitLogger,
		DistanceProvider:      distancer.NewDotProductProvider(),
		MakeBucketOptions:     lsmkv.MakeNoopBucketOptions,
		AllocChecker:          memwatch.NewDummyMonitor(),
		GetViewThunk:          func() common.BucketView { return &noopBucketView{} },
	}
	cfg.TombstoneCallbacks = cyclemanager.NewCallbackGroupNoop()
	cfg.Logger = logger
	cfg.PrometheusMetrics = monitoring.GetMetrics()
	cfg.ClassName = fmt.Sprintf("testIndex%d", metricsClasses.Add(1))

	mvStore := newMultiVectorTestStore()
	cfg.MultiVectorForIDThunk = func(ctx context.Context, id uint64) ([][]float32, error) {
		return mvStore.getMultiVector(id)
	}
	// both thunks, so that the suite runs the token reads; a test
	// of the document path sets TokenForIDThunk to nil (withoutTokenThunk)
	cfg.TokenForIDThunk = func(ctx context.Context, id uint64, pos int, dims int) ([]float32, error) {
		return mvStore.getToken(id, pos)
	}
	cfg.TempMultiVectorForIDThunk = func(ctx context.Context, id uint64, container *common.VectorSlice) ([][]float32, error) {
		return mvStore.getMultiVectorInto(id, container)
	}
	// the token ids stored with the object: testTokenType of each position
	cfg.TokenIDsForIDThunk = func(ctx context.Context, id uint64) ([]uint32, error) {
		vecs, err := mvStore.getMultiVector(id)
		if err != nil {
			return nil, err
		}
		ids := make([]uint32, len(vecs))
		for pos := range ids {
			ids[pos] = testTokenType(pos)
		}
		return ids, nil
	}

	for _, opt := range opts {
		opt(cfg)
	}

	// the scheduler that runs split, merge and reassign, wired as hfresh's
	// newTestIndex does; after the options, which may replace the logger,
	// so that it logs where the index does
	scheduler := queue.NewScheduler(
		queue.SchedulerOptions{
			Logger: cfg.Logger,
		},
	)
	cfg.Scheduler = scheduler
	scheduler.Start()
	t.Cleanup(func() {
		scheduler.Close(t.Context())
	})

	var store *lsmkv.Store
	if dir == "" {
		store = testinghelpers.NewDummyStore(t)
	} else {
		root := filepath.Join(dir, "root")
		require.NoError(t, os.MkdirAll(root, 0o755))
		cfg.RootPath = root
		cfg.Centroids.HNSWConfig.RootPath = root
		cfg.Centroids.HNSWConfig.ID = "centroids"
		cfg.Centroids.HNSWConfig.MakeCommitLoggerThunk = func(opts ...hnsw.CommitlogOption) (hnsw.CommitLogger, error) {
			return hnsw.NewCommitLogger(root, "centroids", cfg.Logger, cyclemanager.NewCallbackGroupNoop(), opts...)
		}
		store = testinghelpers.NewDummyStoreFromFolder(filepath.Join(dir, "store"), t)
	}

	// the user config carries the four parameters; its distance is not
	// read by New (the provider above is), and validate is the schema's
	uc := ent.NewDefaultUserConfig()
	uc.MaxPostingSize = maxPostingSize
	uc.KCentroids = uint32(kCentroids)
	uc.KDocsToScore = uint32(kDocsToScore)
	uc.EFConstruction = efConstruction
	return cfg, uc, store, mvStore, hook
}

// addMultiVectorToIndex stores the document before indexing it: a split
// triggered by its own tokens reads it back (see AddMulti). copied from
// hfresh's addMultiVectorToIndex
func addMultiVectorToIndex(t *testing.T, tf *testIndex, docID uint64, vectors [][]float32) {
	t.Helper()
	tf.mvStore.storeMultiVector(docID, vectors)
	err := tf.addMulti(t.Context(), docID, vectors)
	require.NoError(t, err)
}

// randomMultiVector returns unit tokens, as a late-interaction model emits
// them: the dot product then ranks a document's own tokens first, whatever
// the norms of the others. Based on hfresh's search_test.go.
func randomMultiVector(rng *rand.Rand, tokens, dim int) [][]float32 {
	mv := make([][]float32, tokens)
	for i := range mv {
		v := make([]float32, dim)
		for j := range v {
			v[j] = rng.Float32()
		}
		mv[i] = distancer.Normalize(v)
	}
	return mv
}

// createPostingWithTokens stores every vector as a one-token document
// (doc ids from startID) and returns a fresh posting id with their entries.
// The posting is not registered: tests do that with registerPosting.
// The counterpart of hfresh's createPostingWithVectors.
func createPostingWithTokens(t *testing.T, tf *testIndex, vectors [][]float32, startID uint64) (uint64, []tokenRef) {
	t.Helper()

	if len(vectors) > 0 {
		require.NoError(t, tf.Index.initDimensions(vectors[0]))
	}

	postingID, err := tf.Index.IDs.Next()
	require.NoError(t, err)

	refs := make([]tokenRef, len(vectors))
	for i, vec := range vectors {
		docID := startID + uint64(i)
		tf.mvStore.storeMultiVector(docID, [][]float32{vec})
		refs[i] = tokenRef{DocID: uint32(docID), Pos: 0}
	}

	return postingID, refs
}

// registerPosting stores the posting and makes its centroid searchable, what
// hfresh's tests do with Centroids.Insert, PostingStore.Put and
// setPostingVectorIDs.
func registerPosting(t *testing.T, tf *testIndex, postingID uint64, refs []tokenRef, centroid []float32) {
	t.Helper()
	registerTypedPosting(t, tf, postingID, refs, centroid, testPostingType)
}

// registerTypedPosting is registerPosting with the posting's type, which a
// typed index records before the centroid is searchable and adds to the
// type's group after (see doSplit).
func registerTypedPosting(t *testing.T, tf *testIndex, postingID uint64, refs []tokenRef, centroid []float32, typ uint32) {
	t.Helper()
	require.NoError(t, tf.Index.initDimensions(centroid))
	require.NoError(t, tf.Index.types.register(postingID, typ))
	// a copy: the index owns and reorders the slice it is given
	require.NoError(t, tf.Index.putPosting(t.Context(), postingID, append([]tokenRef(nil), refs...)))
	require.NoError(t, tf.Index.Centroids.Insert(postingID, &hfresh.Centroid{Uncompressed: centroid}))
	tf.Index.types.join(postingID, typ)
}

// posting returns a copy of posting id's entries.
func (tf *testIndex) posting(id uint64) []tokenRef {
	tf.Index.postingLocks.RLock(id)
	defer tf.Index.postingLocks.RUnlock(id)
	return tf.Index.postings.get(id).decode()
}

// hasLog reports whether a log entry with this message was written.
func (tf *testIndex) hasLog(message string) bool {
	for _, entry := range tf.Logs.AllEntries() {
		if entry.Message == message {
			return true
		}
	}
	return false
}

// checkInvariants checks the posting table against the documents the test
// inserted (docs, by doc id) and deleted: every token of every live document
// is in exactly one posting, no entry belongs to an unknown document, a
// posting with entries has a centroid and a posting without a centroid has
// no entries; on a typed index also checkTypeInvariants, for documents
// inserted with testTokenType. Posting sizes are not checked: as in hfresh, a posting that
// grew past maxPostingSize before its split ran leaves halves over the limit
// until another append reaches them. With cleaned set, no
// entry of a deleted document may remain (every posting was garbage
// collected since the deletes). Call it with the queues drained.
func checkInvariants(t *testing.T, tf *testIndex, docs map[uint64][][]float32, deleted map[uint64]bool, cleaned bool) {
	t.Helper()

	seen := make(map[tokenRef]int)
	tf.Index.postings.mu.RLock()
	postings := len(tf.Index.postings.lists)
	tf.Index.postings.mu.RUnlock()
	for id := range uint64(postings) {
		refs := tf.posting(id)
		if len(refs) > 0 {
			require.True(t, tf.Index.Centroids.Exists(id), "posting %d has entries but no centroid", id)
		}
		for _, ref := range refs {
			// a stale entry, older than its document's re-insert, waits for
			// the next split, merge or cleanup of its posting
			if live, err := tf.Index.live(t.Context(), ref); err == nil && !live && !deleted[uint64(ref.DocID)] {
				continue
			}
			seen[newTokenRef(ref.DocID, ref.Pos, 0)]++
			tokens, known := docs[uint64(ref.DocID)]
			require.True(t, known, "posting %d holds token %+v of a document never inserted", id, ref)
			require.Less(t, int(ref.Pos), len(tokens), "posting %d holds token %+v past the document's end", id, ref)
			if cleaned {
				require.False(t, deleted[uint64(ref.DocID)], "posting %d still holds token %+v of a deleted document", id, ref)
			}
		}
	}
	for ref, n := range seen {
		require.Equal(t, 1, n, "token %+v is in %d postings", ref, n)
	}
	checkTypeInvariants(t, tf, postings)
	for docID, tokens := range docs {
		if deleted[docID] {
			continue
		}
		for pos := range tokens {
			require.Equal(t, 1, seen[newTokenRef(uint32(docID), uint16(pos), 0)],
				"token %d of live doc %d is not in exactly one posting", pos, docID)
		}
	}
}

// checkTypeInvariants checks the type invariants of an index whose
// documents were inserted with testTokenType: every posting holds tokens of one
// type, the type recorded for it, and every live centroid is in exactly one
// group, the group of its type, which holds no centroid that is gone; and
// every type counts the entries its postings hold.
func checkTypeInvariants(t *testing.T, tf *testIndex, postings int) {
	t.Helper()

	for id := range uint64(postings) {
		typ := tf.Index.types.typeOf(id)
		for _, ref := range tf.posting(id) {
			require.Equal(t, testTokenType(int(ref.Pos)), typ,
				"posting %d of type %d holds token %+v of another type", id, typ, ref)
		}
	}

	grouped := make(map[uint64]uint32)
	tf.Index.types.mu.RLock()
	defer tf.Index.types.mu.RUnlock()
	for typ, g := range tf.Index.types.groups {
		g.mu.RLock()
		var entries int
		for _, id := range g.ids {
			_, twice := grouped[id]
			require.False(t, twice, "centroid %d is in two groups", id)
			grouped[id] = typ
			require.True(t, tf.Index.Centroids.Exists(id), "group %d holds centroid %d, which is gone", typ, id)
			require.Equal(t, typ, tf.Index.types.types[id], "centroid %d is in the group of another type", id)
			entries += len(tf.posting(id))
		}
		// the size the type's posting limits follow is what its postings
		// hold
		require.Equal(t, int64(entries), g.entries.Load(), "type %d counts another number of entries than its postings hold", typ)
		g.mu.RUnlock()
	}
	for id := range uint64(postings) {
		if tf.Index.Centroids.Exists(id) {
			_, ok := grouped[id]
			require.True(t, ok, "live centroid %d is in no group", id)
		}
	}
}

// AddMultiWithTypes is AddMulti with the token ids given: types[i] is the
// token id of vectors[i]. The shard inserts through AddMulti, which reads
// the ids from the stored object.
func (t *TachiomFresh) AddMultiWithTypes(ctx context.Context, docID uint64, vectors [][]float32, types []uint32) error {
	if len(types) != len(vectors) {
		return errors.Errorf("multi-vector has %d tokens but %d token types", len(vectors), len(types))
	}
	return t.addMulti(ctx, docID, vectors, types)
}
