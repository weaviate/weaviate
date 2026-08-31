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

package inverted

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	"github.com/weaviate/weaviate/entities/concurrency"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	"github.com/weaviate/weaviate/entities/filters"
	entsInverted "github.com/weaviate/weaviate/entities/inverted"
)

// containsBatchFixture owns a real roaringset *lsmkv.Bucket and the
// read-tracking state shared by the readers it hands out: the keys read and
// how many of the returned release funcs get called, so tests can assert both
// which keys docBitmapContainsBatch actually read and that it never leaks a
// bitmap.
type containsBatchFixture struct {
	*lsmkv.Bucket
	// pool backs the bucket's row buffers, so Outstanding() sees a row the fold
	// read and failed to release
	pool *roaringset.BitmapBufPoolTrackingForTests
	// mu guards the counters below. A parallel fold drives one fixture from
	// every worker at once; reading the fields directly is safe only after the
	// fold has returned, which is where every assertion here happens.
	mu           sync.Mutex
	reads        []string
	releaseCalls int
	// onHold, when set, runs while the worker holds the row it just fetched,
	// which is the only point a test can observe what the fold holds at once.
	// onRead fires before the fetch and so cannot.
	onHold func()
	// onRead, when set, runs after each read is recorded — the cancellation
	// test uses it to cancel a context at a deterministic point in the fold.
	onRead func(numReads int)
}

// reader opens a real batch reader on the underlying bucket and wraps it so
// every read and every release func records into the fixture. The reader's view is
// released once, at test end — releasing is the caller's job, so the fold never
// does it.
func (s *containsBatchFixture) reader(t *testing.T, keys entsInverted.SortedKeys) *spyContainsBatchReader {
	t.Helper()
	view := s.GetConsistentView()
	t.Cleanup(view.ReleaseView)
	rdr, err := lsmkv.NewRoaringSetBatchReader(view.WithoutEmptyActiveMemtable(), keys)
	require.NoError(t, err)
	return &spyContainsBatchReader{reader: rdr, fixture: s, keys: keys}
}

// source hands the fold spied readers, one per share it asks for, so a parallel
// fold's reads land in the same fixture a sequential one's do.
//
// All of them come off ONE view, as production's does: a view is what pins the
// segments the readers walk, and taking one per reader would have each release
// its own while the others are still reading.
func (s *containsBatchFixture) source(t *testing.T) *spyContainsBatchReaderSource {
	t.Helper()
	view := s.GetConsistentView()
	t.Cleanup(view.ReleaseView)
	// narrowed once, up front, as production does
	return &spyContainsBatchReaderSource{t: t, fixture: s, view: view.WithoutEmptyActiveMemtable()}
}

type spyContainsBatchReaderSource struct {
	t       *testing.T
	fixture *containsBatchFixture
	view    lsmkv.NarrowedConsistentView
	// shares records the keys each reader was opened over, in the order the fold
	// asked for them, so a test can assert how a batch was split without the
	// planner's arithmetic standing in for the fold's own wiring.
	//
	// It needs no lock: readers are opened while the workers are being started,
	// from that one goroutine, and read once the fold has returned.
	shares [][]string
}

func (s *spyContainsBatchReaderSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	rdr, err := lsmkv.NewRoaringSetBatchReader(s.view, keys)
	if err != nil {
		return nil, err
	}
	share := make([]string, 0, keys.Len())
	for _, k := range keys.All() {
		share = append(share, string(k))
	}
	s.shares = append(s.shares, share)
	return &spyContainsBatchReader{reader: rdr, fixture: s.fixture, keys: keys}, nil
}

// spyContainsBatchReader is the reader the fixture hands the fold: it records
// every key read and wraps every release so the fixture's counters see the
// whole batch.
type spyContainsBatchReader struct {
	reader  *lsmkv.RoaringSetBatchReader
	fixture *containsBatchFixture
	keys    entsInverted.SortedKeys
	// pos tracks the walk so the spy can name the key each read is for, since Next
	// is handed no index and the fixture records by key.
	pos int
}

func (r *spyContainsBatchReader) Len() int { return r.keys.Len() }

func (r *spyContainsBatchReader) Next(mergeConc int) (*sroar.Bitmap, func(), error) {
	s := r.fixture
	// A fold reading past the batch is the reader's error to report, not a
	// panic here.
	if r.pos < r.keys.Len() {
		s.mu.Lock()
		s.reads = append(s.reads, string(r.keys.At(r.pos)))
		numReads, onRead := len(s.reads), s.onRead
		s.mu.Unlock()
		// called outside the lock: it cancels a context, and a fold that
		// re-entered the fixture from there would deadlock
		if onRead != nil {
			onRead(numReads)
		}
	}
	bm, release, err := r.reader.Next(mergeConc)
	if err == nil {
		s.mu.Lock()
		onHold := s.onHold
		s.mu.Unlock()
		if onHold != nil {
			onHold()
		}
	}
	if err != nil {
		// The contract the folds are written against: an error and neither a row
		// nor a release. Wrapping the nil release would hand back a closure that
		// panics, and a double looser than the reader it stands in for would let
		// a fold mishandle the error path and still pass.
		//
		// The position stays where it is for the same reason: the reader does not
		// advance its own on a failed read, so a double that advanced would name
		// the following key for a read that never happened.
		return nil, nil, err
	}
	r.pos++
	return bm, func() {
		s.mu.Lock()
		s.releaseCalls++
		s.mu.Unlock()
		release()
	}, nil
}

// newFoldSearcher builds the Searcher the batched Contains fold needs. The
// logger is not decoration: the parallel fold runs its readers under an error
// group, which logs on every wait, so a Searcher without one is a shape
// production cannot produce — NewSearcher requires it — and only a literal here
// can.
func newFoldSearcher(t *testing.T, pool roaringset.BitmapBufPool) *Searcher {
	t.Helper()
	if pool == nil {
		pool = roaringset.NewBitmapBufPoolNoop()
	}
	logger, _ := test.NewNullLogger()
	return &Searcher{
		logger:        logger,
		bitmapFactory: roaringset.NewBitmapFactory(pool, func() uint64 { return 300_000 }),
	}
}

func newContainsBatchFixture(t *testing.T, ctx context.Context, rows map[string][]uint64) *containsBatchFixture {
	t.Helper()
	return newContainsBatchFixtureSplit(t, ctx, rows, nil)
}

// newContainsBatchFixtureSplit flushes one set of rows and leaves the other in
// the active memtable. A fully flushed bucket has an empty active memtable, the
// reader drops it from the view, and the windowed read never runs — so the
// unflushed half is the only way a fold here reaches the reader it was built
// for. Passing nil for it gives the all-flushed bucket most tests want.
func newContainsBatchFixtureSplit(t *testing.T, ctx context.Context,
	flushed, unflushed map[string][]uint64,
) *containsBatchFixture {
	t.Helper()

	logger, _ := test.NewNullLogger()
	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	b, err := lsmkv.NewBucketCreator().NewBucket(ctx, t.TempDir(), "", logger, nil,
		cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
		lsmkv.WithStrategy(lsmkv.StrategyRoaringSet),
		lsmkv.WithBitmapBufPool(pool))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.Shutdown(context.Background())) })
	b.SetMemtableThreshold(1e9) // no auto-flush; keep the fixture deterministic

	for key, values := range flushed {
		require.NoError(t, b.RoaringSetAddList([]byte(key), values))
	}
	require.NoError(t, b.FlushAndSwitch())
	for key, values := range unflushed {
		require.NoError(t, b.RoaringSetAddList([]byte(key), values))
	}

	fixture := &containsBatchFixture{Bucket: b, pool: pool}
	// Registered here rather than called per test, so it covers the error and
	// cancellation paths for free and still runs when an earlier assertion has
	// already failed the test.
	t.Cleanup(func() {
		require.Zero(t, fixture.pool.Outstanding(), "no row buffer may outlive the fold")
	})
	return fixture
}

// TestDocBitmapContainsBatch_ReadsUnflushedRows folds a batch whose rows are
// split between disk and the active memtable, over enough keys to cross several
// windows. It is the only test that takes the fold, the reader, the windowing
// and the memtable walk together; the rest flush first and so exercise the disk
// path with the memtable skipped.
func TestDocBitmapContainsBatch_ReadsUnflushedRows(t *testing.T) {
	ctx := context.Background()

	// One worker, so one reader walks the whole batch. A split batch gives each
	// worker a share of a few hundred keys, every share fits one window, and the
	// crossing this test exists for stops happening — silently, since the fold
	// still answers correctly.
	forceContainsWorkers(t, 1)

	// Three windows at the production size of 1024, so the walk crosses two
	// boundaries and ends on a narrower one. The size is not reachable from here
	// — the production constructor picks it — so this is a key count rather than
	// a multiple of it.
	const n = 2500
	flushed, unflushed := map[string][]uint64{}, map[string][]uint64{}
	batch := make([][]byte, 0, n)
	want := make([]uint64, 0, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("key_%05d", i)
		batch = append(batch, []byte(key))
		switch i % 3 {
		case 0:
			flushed[key] = []uint64{uint64(i)}
			want = append(want, uint64(i))
		case 1:
			unflushed[key] = []uint64{uint64(i)}
			want = append(want, uint64(i))
		}
		// i%3 == 2 is asked for and held by neither layer
	}

	fixture := newContainsBatchFixtureSplit(t, ctx, flushed, unflushed)
	s := newFoldSearcher(t, fixture.pool)

	pv := &propValuePair{
		prop:         "some-prop",
		operator:     filters.ContainsAny,
		containsKeys: keysFrom(t, batch...),
	}
	dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
	require.NoError(t, err)
	require.Equal(t, want, dbm.docIDs.ToArray(),
		"the fold must see the memtable's rows as well as the disk's")
	dbm.release()
}

// TestMergeAllowlistBitmaps_UnsupportedOperator pins the defensive default
// arm: an operator that is neither ContainsAny nor ContainsAll must error
// and release both operands, so the backstop cannot rot into a silent
// nil return or a buffer leak.
func TestMergeAllowlistBitmaps_UnsupportedOperator(t *testing.T) {
	var aReleased, bReleased bool
	res, release, err := mergeAllowlistBitmaps(filters.OperatorEqual, 1,
		sroar.NewBitmap(), func() { aReleased = true },
		sroar.NewBitmap(), func() { bReleased = true })
	require.Error(t, err)
	require.Nil(t, res)
	require.Nil(t, release)
	require.True(t, aReleased, "unsupported operator must release the accumulator")
	require.True(t, bReleased, "unsupported operator must release the fetched operand")
}

func TestDocBitmapContainsBatch_ContainsAnyFold(t *testing.T) {
	ctx := context.Background()
	fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
		"present-a": {1, 2, 3},
		"present-b": {3, 4, 5},
	})

	pv := &propValuePair{
		operator:     filters.ContainsAny,
		containsKeys: keysFrom(t, []byte("present-a"), []byte("missing"), []byte("present-b")),
	}

	s := newFoldSearcher(t, nil)
	dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
	require.NoError(t, err)
	defer dbm.release()

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, dbm.docIDs.ToArray())
	require.False(t, dbm.IsDenyList())
	require.Equal(t, []string{"missing", "present-a", "present-b"}, fixture.reads,
		"every key must be read for ContainsAny, absent key included, in key order")
}

// TestDocBitmapContainsBatch_UnsupportedOperator pins the defensive default
// arm of the fold dispatch: a propValuePair that carries pre-encoded keys but
// a non-Contains operator must error instead of silently running the union
// fold and returning plausible but wrong results. Its sibling backstop, the
// routing check in resolveDocIDs, is pinned by
// TestResolveDocIDs_ContainsKeysRequireContainsOperator.
func TestDocBitmapContainsBatch_UnsupportedOperator(t *testing.T) {
	ctx := context.Background()
	fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
		"present-a": {1, 2, 3},
	})

	pv := &propValuePair{
		operator:     filters.OperatorEqual,
		containsKeys: keysFrom(t, []byte("present-a"), []byte("present-b")),
	}

	s := newFoldSearcher(t, nil)
	dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
	require.ErrorContains(t, err, "unsupported operator")
	require.Nil(t, dbm.docIDs)
	require.Empty(t, fixture.reads, "no key may be read for an unsupported operator")
}

// TestDocBitmapContainsBatch_NoKeys pins the fold's other defensive backstop:
// its accumulator is the first row it reads, so zero keys has no result to
// return. Erroring keeps that a loud caller bug rather than a docBitmap with a
// nil bitmap flowing into the merges. fetchContainsBatch answers the empty case
// before calling in, which TestFetchContainsBatch_EmptyKeySet pins.
//
// The count that decides it is pv's, which is also what the readers are built
// from — so there is no longer a reader that could hold a different number of
// keys than the leaf asking for them.
func TestDocBitmapContainsBatch_NoKeys(t *testing.T) {
	ctx := context.Background()
	fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{"present-a": {1, 2, 3}})

	for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
		{
			t.Run(op.Name(), func(t *testing.T) {
				s := newFoldSearcher(t, nil)
				pv := &propValuePair{prop: "some-prop", operator: op, containsKeys: keysFrom(t)}
				dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
				require.ErrorContains(t, err, "carries no keys")
				require.ErrorContains(t, err, `"some-prop"`, "the error must name the property")
				require.Nil(t, dbm.docIDs)
				require.Empty(t, fixture.reads, "no key may be read")
			})
		}
	}
}

// failingContainsBatchReader fails one key and reads every other through the
// wrapped reader, so a fold can be stopped at a chosen point mid-batch.
// failingContainsBatchReaderSource hands out failing readers, so a fold that
// mints one per worker still meets the injected failure whichever share holds
// the poisoned key.
type failingContainsBatchReaderSource struct {
	fixture *containsBatchFixture
	t       *testing.T
	failKey string
}

func (s *failingContainsBatchReaderSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	return &failingContainsBatchReader{
		containsBatchReader: s.fixture.reader(s.t, keys),
		failKey:             s.failKey,
		keys:                keys,
	}, nil
}

type failingContainsBatchReader struct {
	containsBatchReader
	failKey string
	keys    entsInverted.SortedKeys
	pos     int
}

func (r *failingContainsBatchReader) Next(mergeConc int) (*sroar.Bitmap, func(), error) {
	key := ""
	if r.pos < r.keys.Len() {
		key = string(r.keys.At(r.pos))
	}
	r.pos++
	if key == r.failKey {
		// The wrapped reader is left where it was, which is what a fold aborting
		// on this error does anyway.
		return nil, nil, fmt.Errorf("injected read failure")
	}
	return r.containsBatchReader.Next(mergeConc)
}

// TestDocBitmapContainsBatch_ReadError pins what a failed row read costs: the
// error reaches the caller wrapped rather than becoming a silently short
// result, the fold stops, and the rows it had already accumulated go back to
// the bucket's buffer pool instead of leaking.
func TestDocBitmapContainsBatch_ReadError(t *testing.T) {
	tests := []struct {
		name string
		gate int // containsAccumulatorMinKeysPerWorker, lowered to force the accumulator
		// asserted rather than inferred: the gate is read per worker, so which
		// fold a value selects depends on the plan's own worker count
		wantStrategy containsFoldStrategy
	}{
		{name: "incremental fold", gate: 256, wantStrategy: foldStrategyUnionIncremental},
		{name: "accumulator fold", gate: 2, wantStrategy: foldStrategyUnionAccumulator},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// "nothing after the failure is read" is a property of a single walk:
			// with a share each, the other workers read theirs before the failing
			// one can cancel them. The parallel fold's own error path is covered
			// separately.
			forceContainsWorkers(t, 1)
			forceContainsAccumulatorGate(t, tc.gate)

			ctx := context.Background()
			fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
				"a": {1, 2, 3},
				"z": {7, 8},
			})

			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)

			// "a" is read and accumulated, "poison" fails, "z" must never be
			// reached — the keys arrive ascending, so "poison" sits between them
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: keysFrom(t, []byte("a"), []byte("poison"), []byte("z")),
			}
			dbm, plan, err := s.docBitmapContainsBatch(ctx,
				&failingContainsBatchReaderSource{fixture: fixture, t: t, failKey: "poison"},
				pv)
			require.ErrorContains(t, err, "read row 1 of [0,3)",
				"the message must name which row of which share failed")
			require.ErrorContains(t, err, "injected read failure")
			require.Equal(t, tc.wantStrategy, plan.strategy,
				"a failed fold still reports the plan it was running")
			require.Equal(t, docBitmap{}, dbm, "a failed read must not yield a partial result")

			// the poisoned key is answered by the wrapper, so it never reaches
			// the fixture; "z" missing is what proves the fold stopped
			require.Equal(t, []string{"a"}, fixture.reads, "no key after the failure may be read")
			require.Equal(t, len(fixture.reads), fixture.releaseCalls,
				"every row the fold received must be released")
			require.Zero(t, fixture.pool.Outstanding(),
				"no row buffer may outlive the failed fold")
		})
	}
}

func TestResolveDocIDs_ContainsKeysRequireContainsOperator(t *testing.T) {
	tests := []struct {
		name     string
		operator filters.Operator
	}{
		{name: "defined non-contains operator", operator: filters.OperatorEqual},
		{name: "operator outside the enum", operator: filters.Operator(999)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pv := &propValuePair{
				operator:     tc.operator,
				containsKeys: keysFrom(t, []byte("a"), []byte("b")),
			}
			logger, hook := test.NewNullLogger()
			_, err := pv.resolveDocIDs(context.Background(), &Searcher{logger: logger}, 0)
			require.ErrorContains(t, err, "non-contains operator")

			// Reported nowhere else: the caller's wrap gives no hint it was ours.
			require.Len(t, hook.Entries, 1)
			require.Equal(t, logrus.ErrorLevel, hook.LastEntry().Level)
			require.Contains(t, hook.LastEntry().Message, "internal fault")
		})
	}
}

// TestDocBitmapContainsBatch_ContainsNoneFold pins ContainsNone as
// NOT(ContainsAny): the docIDs are the same union the ContainsAny fold
// produces, with isDenyList set so downstream merges and the final
// universe inversion treat it as a negation.
func TestDocBitmapContainsBatch_ContainsNoneFold(t *testing.T) {
	ctx := context.Background()
	fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
		"present-a": {1, 2, 3},
		"present-b": {3, 4, 5},
	})

	pv := &propValuePair{
		operator:     filters.ContainsNone,
		containsKeys: keysFrom(t, []byte("present-a"), []byte("missing"), []byte("present-b")),
	}

	s := newFoldSearcher(t, nil)
	dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
	require.NoError(t, err)
	defer dbm.release()

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, dbm.docIDs.ToArray(),
		"ContainsNone folds the same union as ContainsAny")
	require.True(t, dbm.IsDenyList())
	require.Equal(t, []string{"missing", "present-a", "present-b"}, fixture.reads)
}

// Same folds as above but forced through the Accumulator path, which the
// containsAccumulatorMinKeysPerWorker gate would otherwise route to the incremental
// fold at this key count. ContainsNone is covered here too: it shares the union
// but must still come back a deny list, and losing the flag on this arm alone
// would invert the filter against the universe at large key counts.
func TestDocBitmapContainsBatch_ContainsAnyAccumulatorFold(t *testing.T) {
	forceContainsAccumulatorGate(t, 2)

	ctx := context.Background()
	rows := map[string][]uint64{
		"present-a": {1, 2, 3},
		"present-b": {3, 4, 5},
		// A row spilling into further 64K ranges, so the union spans
		// multiple result containers.
		"present-c": {70_000, 200_000},
	}

	keys := keysFrom(t,
		[]byte("present-a"), []byte("missing"), []byte("present-b"), []byte("present-c"),
	)
	for _, tc := range []struct {
		operator     filters.Operator
		wantDenyList bool
	}{
		{operator: filters.ContainsAny, wantDenyList: false},
		{operator: filters.ContainsNone, wantDenyList: true},
	} {
		t.Run(tc.operator.Name(), func(t *testing.T) {
			fixture := newContainsBatchFixture(t, ctx, rows)

			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)
			pv := &propValuePair{operator: tc.operator, containsKeys: keys}
			dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
			require.NoError(t, err)

			require.Equal(t, []uint64{1, 2, 3, 4, 5, 70_000, 200_000}, dbm.docIDs.ToArray())
			require.Equal(t, tc.wantDenyList, dbm.IsDenyList())
			require.Equal(t, []string{"missing", "present-a", "present-b", "present-c"}, fixture.reads,
				"every key must be read, absent key included, in key order")

			dbm.release()
			require.Zero(t, pool.Outstanding(),
				"the pooled result buffer must flow back through dbm.release")
		})
	}
}

// TestDocBitmapContainsBatch_SingleKey covers the 1 of 0/1/N. The folds adopt
// their first row as the accumulator and merge every row after it, so a single
// key is the one count that exercises the adopt without any merge — and a fold
// that returned its accumulator unset here would hand back a docBitmap with a
// nil bitmap, which is exactly what the folds' closing comments claim cannot
// happen.
func TestDocBitmapContainsBatch_SingleKey(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		operator filters.Operator
		key      string
		want     []uint64
		wantDeny bool
	}{
		{operator: filters.ContainsAny, key: "a", want: []uint64{1, 2, 3}},
		{operator: filters.ContainsAll, key: "a", want: []uint64{1, 2, 3}},
		{operator: filters.ContainsAny, key: "missing", want: []uint64{}},
		{operator: filters.ContainsAll, key: "missing", want: []uint64{}},
		// ContainsNone is the only operator that marks the result a deny list,
		// and one key is newly reachable for it: a two-value filter naming the
		// same value twice arrives here with one.
		{operator: filters.ContainsNone, key: "a", want: []uint64{1, 2, 3}, wantDeny: true},
		{operator: filters.ContainsNone, key: "missing", want: []uint64{}, wantDeny: true},
	}
	for _, tc := range tests {
		t.Run(tc.operator.Name()+"/"+tc.key, func(t *testing.T) {
			fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{"a": {1, 2, 3}})

			s := newFoldSearcher(t, nil)
			pv := &propValuePair{
				operator:     tc.operator,
				containsKeys: keysFrom(t, []byte(tc.key)),
			}
			dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
			require.NoError(t, err)
			defer dbm.release()

			require.NotNil(t, dbm.docIDs, "a fold must never return a nil bitmap")
			require.Equal(t, tc.want, dbm.docIDs.ToArray())
			require.Equal(t, tc.wantDeny, dbm.IsDenyList(),
				"the deny flag is set on the result, not folded into it")
			require.Equal(t, []string{tc.key}, fixture.reads)
		})
	}
}

func TestDocBitmapContainsBatch_ContainsAllFold(t *testing.T) {
	ctx := context.Background()

	t.Run("non-empty intersection", func(t *testing.T) {
		fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"b": {2, 3, 4},
			"c": {2, 3, 5},
		})

		pv := &propValuePair{
			operator:     filters.ContainsAll,
			containsKeys: keysFrom(t, []byte("a"), []byte("b"), []byte("c")),
		}

		s := newFoldSearcher(t, nil)
		dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Equal(t, []uint64{2, 3}, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "b", "c"}, fixture.reads)
	})

	// The early exit below is a property of a single walk: a worker can only skip
	// keys in its OWN share, so with a share each the others are read before it
	// fires. Forced sequential here; the parallel fold's own cancellation is
	// covered separately.
	t.Run("folds to empty and stops reading remaining keys", func(t *testing.T) {
		forceContainsWorkers(t, 1)
		fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"b": {4, 5, 6}, // disjoint from "a" -> accumulator becomes empty here
			"c": {1, 2, 3}, // must never be read: the AND result can't change
		})

		pv := &propValuePair{
			operator:     filters.ContainsAll,
			containsKeys: keysFrom(t, []byte("a"), []byte("b"), []byte("c")),
		}

		s := newFoldSearcher(t, nil)
		dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Empty(t, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "b"}, fixture.reads,
			"key c must not be read once the AND accumulator is provably empty")
	})

	t.Run("absent key empties the intersection and stops reading", func(t *testing.T) {
		forceContainsWorkers(t, 1)
		fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"z": {1, 2, 3}, // must never be read: the absent key already emptied the AND
		})

		// keys arrive ascending, so "missing" sits between the two present ones
		pv := &propValuePair{
			operator:     filters.ContainsAll,
			containsKeys: keysFrom(t, []byte("a"), []byte("missing"), []byte("z")),
		}

		s := newFoldSearcher(t, nil)
		dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Empty(t, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "missing"}, fixture.reads,
			"key z must not be read once the absent key emptied the accumulator")
	})
}

// TestDocBitmapContainsBatch_ContextCancelledMidLoop pins fold cancellation on
// both arms: each checks the context before every read, so a context cancelled
// during the first read stops the loop before the second and releases the row
// already adopted. The accumulator arm is the one guarding the large-N folds it
// exists for, which the default gate would otherwise route past.
func TestDocBitmapContainsBatch_ContextCancelledMidLoop(t *testing.T) {
	tests := []struct {
		name string
		gate int // containsAccumulatorMinKeysPerWorker, lowered to force the accumulator
		// asserted rather than inferred: the gate is read per worker, so which
		// fold a value selects depends on the plan's own worker count
		wantStrategy containsFoldStrategy
	}{
		{name: "incremental fold", gate: 256, wantStrategy: foldStrategyUnionIncremental},
		{name: "accumulator fold", gate: 2, wantStrategy: foldStrategyUnionAccumulator},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// "nothing after the failure is read" is a property of a single walk:
			// with a share each, the other workers read theirs before the failing
			// one can cancel them. The parallel fold's own error path is covered
			// separately.
			forceContainsWorkers(t, 1)
			forceContainsAccumulatorGate(t, tc.gate)

			fixture := newContainsBatchFixture(t, context.Background(), map[string][]uint64{
				"a": {1, 2, 3},
				"b": {4, 5, 6},
				"c": {7, 8, 9},
			})

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			// cancel as key "a" is read, so the check before "b" sees it
			fixture.onRead = func(numReads int) {
				if numReads == 1 {
					cancel()
				}
			}

			s := newFoldSearcher(t, fixture.pool)
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: keysFrom(t, []byte("a"), []byte("b"), []byte("c")),
			}
			dbm, plan, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, docBitmap{}, dbm)
			require.Equal(t, tc.wantStrategy, plan.strategy,
				"a cancelled fold still reports the plan it was running")

			require.Equal(t, []string{"a"}, fixture.reads, "the fold must stop reading once ctx is cancelled")
			require.Equal(t, 1, fixture.releaseCalls, "the row read before cancellation must be released")
		})
	}
}

// parallelFixtureKeys is wide enough that every worker in the fetch counts
// these tests force still gets a share of several keys, and narrow enough that
// building the bucket stays cheap.
const parallelFixtureKeys = 40

// newParallelContainsBatchFixture builds a batch whose rows are split between
// disk and the active memtable, so a parallel fold here walks both. Every row
// holds doc 0, so the intersection over the whole batch is exactly {0}, and
// each row holds two doc IDs of its own, so the union is every key's own pair
// plus that one shared doc.
func newParallelContainsBatchFixture(t *testing.T) (*containsBatchFixture, []string) {
	t.Helper()
	keys := make([]string, parallelFixtureKeys)
	flushed, unflushed := map[string][]uint64{}, map[string][]uint64{}
	for i := range keys {
		// Widths vary so Build keeps the offsets, which is the layout a text
		// filter with unequal-length values produces and the one Range narrows
		// differently. The zero-padded head still decides the order, so the
		// keys sort exactly as an equal-width set would.
		keys[i] = fmt.Sprintf("k%02d%s", i, strings.Repeat("x", i%3))
		row := []uint64{0, uint64(1000 + i), uint64(2000 + i)}
		if i%2 == 0 {
			flushed[keys[i]] = row
		} else {
			unflushed[keys[i]] = row
		}
	}
	return newContainsBatchFixtureSplit(t, context.Background(), flushed, unflushed), keys
}

// sortedKeysFromStrings is keysFrom for keys a test generated rather than wrote
// out.
func sortedKeysFromStrings(tb testing.TB, keys []string) entsInverted.SortedKeys {
	tb.Helper()
	raw := make([][]byte, len(keys))
	for i, k := range keys {
		raw[i] = []byte(k)
	}
	return keysFrom(tb, raw...)
}

// TestDocBitmapContainsBatch_ParallelMatchesSequential is the parallel fold's
// central claim: splitting a batch across workers changes how it is read, not
// what it answers. Each case folds the same batch twice, once as a single walk
// and once across four, and the two must agree — on the doc IDs, on the deny
// list, and on having read every key exactly once and released every row.
//
// Each leg asserts the readers the fold actually opened, not only the count the
// planner asked for: a fold that ignored its plan and walked the batch once
// would answer correctly and pass every other assertion here.
//
// A worker's share stays inside one memtable window at this size. Crossing a
// window boundary inside a share is covered a level down, by lsmkv's
// TestBatchReadersShareOneViewIndependently, which opens its readers on a
// window narrow enough to force several fills each.
func TestDocBitmapContainsBatch_ParallelMatchesSequential(t *testing.T) {
	tests := []struct {
		name     string
		operator filters.Operator
		// gate is containsAccumulatorMinKeysPerWorker, which picks between the two
		// union folds; it does not apply to ContainsAll
		gate int
		// wantStrategy is asserted rather than inferred from gate. The gate is
		// read per worker, so which fold a value selects depends on the plan's
		// own worker count — growing the fixture could move a leg to the other
		// strategy and silently delete the coverage this case is named for.
		wantStrategy containsFoldStrategy
		wantDocIDs   []uint64
		wantDenyList bool
	}{
		{
			name: "ContainsAny, incremental", operator: filters.ContainsAny, gate: 256,
			wantStrategy: foldStrategyUnionIncremental,
			wantDocIDs:   parallelFixtureUnion(),
		},
		{
			name: "ContainsAny, accumulator", operator: filters.ContainsAny, gate: 2,
			wantStrategy: foldStrategyUnionAccumulator,
			wantDocIDs:   parallelFixtureUnion(),
		},
		{
			name: "ContainsNone, deny list over the same union", operator: filters.ContainsNone, gate: 256,
			wantStrategy: foldStrategyUnionIncremental,
			wantDocIDs:   parallelFixtureUnion(), wantDenyList: true,
		},
		{
			// the deny flag on the arm that stages through AccumulatorToBuf: the
			// incremental leg above adopts a fetched row, this one never does
			name: "ContainsNone, accumulator", operator: filters.ContainsNone, gate: 2,
			wantStrategy: foldStrategyUnionAccumulator,
			wantDocIDs:   parallelFixtureUnion(), wantDenyList: true,
		},
		{
			name: "ContainsAll, intersection", operator: filters.ContainsAll, gate: 256,
			wantStrategy: foldStrategyIntersection,
			// only doc 0 is in every row
			wantDocIDs: []uint64{0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forceContainsAccumulatorGate(t, tc.gate)

			fold := func(t *testing.T, workers int) (docIDs []uint64, isDenyList bool) {
				t.Helper()
				forceContainsWorkers(t, workers)
				fixture, keys := newParallelContainsBatchFixture(t)
				pool := roaringset.NewBitmapBufPoolTrackingForTests()
				s := newFoldSearcher(t, pool)
				pv := &propValuePair{
					operator:     tc.operator,
					containsKeys: sortedKeysFromStrings(t, keys),
				}

				src := fixture.source(t)
				dbm, plan, err := s.docBitmapContainsBatch(t.Context(), src, pv)
				require.NoError(t, err)
				require.Equal(t, workers, plan.workers)
				require.Len(t, src.shares, workers,
					"the fold must open the readers it planned, not just plan them")
				require.Equal(t, tc.wantStrategy, plan.strategy,
					"the case is named for a strategy; the fold must have run it")
				docIDs, isDenyList = dbm.docIDs.ToArray(), dbm.isDenyList
				dbm.release()

				require.ElementsMatch(t, keys, fixture.reads,
					"every key must be read exactly once, whoever read it")
				requireNoLeakedRows(t, fixture, pool)
				return docIDs, isDenyList
			}

			seqIDs, seqDeny := fold(t, 1)
			parIDs, parDeny := fold(t, 4)

			require.Equal(t, tc.wantDocIDs, seqIDs)
			require.Equal(t, seqIDs, parIDs, "a parallel fold must answer what a sequential one does")
			require.Equal(t, tc.wantDenyList, seqDeny)
			require.Equal(t, seqDeny, parDeny)
		})
	}
}

// parallelFixtureUnion is what newParallelContainsBatchFixture's rows union to,
// in the ascending order a bitmap yields: the doc every row shares, then each
// row's own two.
func parallelFixtureUnion() []uint64 {
	out := []uint64{0}
	for i := range parallelFixtureKeys {
		out = append(out, uint64(1000+i))
	}
	for i := range parallelFixtureKeys {
		out = append(out, uint64(2000+i))
	}
	return out
}

// TestDocBitmapContainsBatch_ParallelSplitsTheBatch pins the split the workers
// actually get, as opposed to the arithmetic TestContainsEvenSplit pins: one
// reader per non-empty share, each over its own contiguous run of keys, and the
// runs laid end to end are the batch in order.
//
// It also covers what the source is not asked: nothing about a share reaches it
// but the keys, so there is no per-worker window or budget that could grow with
// the fetch count.
func TestDocBitmapContainsBatch_ParallelSplitsTheBatch(t *testing.T) {
	for _, workers := range []int{1, 2, 4, 8} {
		t.Run(fmt.Sprintf("workers=%d", workers), func(t *testing.T) {
			forceContainsWorkers(t, workers)
			fixture, keys := newParallelContainsBatchFixture(t)
			s := newFoldSearcher(t, nil)
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			src := fixture.source(t)
			dbm, _, err := s.docBitmapContainsBatch(t.Context(), src, pv)
			require.NoError(t, err)
			dbm.release()

			require.Len(t, src.shares, workers,
				"one reader per worker: the batch is wider than the fetch count, so no share is empty")
			var laidEndToEnd []string
			for _, share := range src.shares {
				require.NotEmpty(t, share)
				laidEndToEnd = append(laidEndToEnd, share...)
			}
			require.Equal(t, keys, laidEndToEnd,
				"the shares must tile the batch in order, without overlap or gap")
		})
	}
}

// TestDocBitmapContainsBatch_ParallelReadError pins that one worker's failed
// read fails the whole fold rather than yielding a result short by that
// worker's share, and that the rows the other workers had already accumulated
// go back to the pool.
//
// Unlike the sequential case it cannot assert what was not read: the other
// workers are walking their own shares while this one fails, and cancelling
// them is a race the fold is not trying to win.
func TestDocBitmapContainsBatch_ParallelReadError(t *testing.T) {
	tests := []struct {
		name     string
		operator filters.Operator
		gate     int // containsAccumulatorMinKeysPerWorker, lowered to force the accumulator
		// asserted rather than inferred: the gate is read per worker, so which
		// fold a value selects depends on the plan's own worker count
		wantStrategy containsFoldStrategy
	}{
		{name: "incremental fold", operator: filters.ContainsAny, gate: 256, wantStrategy: foldStrategyUnionIncremental},
		{name: "accumulator fold", operator: filters.ContainsAny, gate: 2, wantStrategy: foldStrategyUnionAccumulator},
		{
			// the intersection is the arm where a read failure and the
			// ContainsAll early exit compete for the same cancelled gctx, so a
			// fold that let the exit win would answer an error case with a
			// result. Every row here holds doc 0, so no share empties and the
			// exit has nothing to fire on: the error is the only way out.
			name: "intersection fold", operator: filters.ContainsAll, gate: 256,
			wantStrategy: foldStrategyIntersection,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forceContainsWorkers(t, 4)
			forceContainsAccumulatorGate(t, tc.gate)

			fixture, keys := newParallelContainsBatchFixture(t)
			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)
			pv := &propValuePair{
				operator:     tc.operator,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			// in the third worker's share, so the fold fails with the others
			// mid-walk rather than before they start or after they finish
			dbm, plan, err := s.docBitmapContainsBatch(t.Context(),
				&failingContainsBatchReaderSource{fixture: fixture, t: t, failKey: keys[22]},
				pv)
			// keys[22] is the third of four shares over 40 keys: [20,30).
			require.ErrorContains(t, err, "read row 22 of [20,30)",
				"the message must name the failing worker by its own share")
			require.Equal(t, tc.wantStrategy, plan.strategy,
				"a failed fold still reports the plan it was running")
			require.ErrorContains(t, err, "injected read failure")
			require.Equal(t, docBitmap{}, dbm, "a failed read must not yield a partial result")

			requireNoLeakedRows(t, fixture, pool)
		})
	}
}

// openFailingSource opens readers normally until failAt, which fails. It needs
// no lock: runWorkers opens every reader from its own goroutine.
type openFailingSource struct {
	fixture *containsBatchFixture
	t       *testing.T
	failAt  int // the 0-based reader index whose open fails
	opened  int
}

func (s *openFailingSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	if s.opened == s.failAt {
		return nil, fmt.Errorf("injected reader-open failure")
	}
	s.opened++
	return s.fixture.reader(s.t, keys), nil
}

// TestDocBitmapContainsBatch_ParallelReaderOpenError pins the reader-open
// failure for a worker after the first: the workers already started are waited
// out, every row they read goes back to the pool, and the fold reports the open
// error rather than what those workers had reached.
//
// NewRoaringSetBatchReader's only error is the view's strategy check, which is
// a property of the one bucket every reader shares — so in production the
// failure always takes the first reader, before any goroutine starts. Worker 0
// is therefore the case that happens and the later one is the case runWorkers
// carries code for, and both are covered here.
func TestDocBitmapContainsBatch_ParallelReaderOpenError(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		failAt  int
	}{
		{
			// the shape production takes: nothing has started, so eg.Wait
			// returns at once and the error is returned unwrapped
			name:    "the first reader, before any worker starts",
			workers: 4, failAt: 0,
		},
		{
			// two workers are walking their shares when it fails, so the fold
			// has to wait them out and hand their rows back
			name:    "a later reader, with workers already running",
			workers: 4, failAt: 2,
		},
		{
			// workers 1 runs incremental rather than runWorkers, so the open
			// error is returned from a different arm
			name:    "the only reader of a sequential fold",
			workers: 1, failAt: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forceContainsWorkers(t, tc.workers)

			fixture, keys := newParallelContainsBatchFixture(t)
			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			src := &openFailingSource{fixture: fixture, t: t, failAt: tc.failAt}
			dbm, _, err := s.docBitmapContainsBatch(t.Context(), src, pv)
			require.ErrorContains(t, err, "injected reader-open failure")
			require.Equal(t, docBitmap{}, dbm, "a failed open must not yield a partial result")
			require.Equal(t, tc.failAt, src.opened, "the fold must stop opening readers at the failure")

			requireNoLeakedRows(t, fixture, pool)
		})
	}
}

// openFailsAfterWorkingReaderSource fails the second open only once the first
// reader's read has failed. Without that ordering the open wins the race and
// the worker returns context.Canceled — a path that passes against the bug.
type openFailsAfterWorkingReaderSource struct {
	fixture *containsBatchFixture
	t       *testing.T
	opened  int
	failed  chan struct{}
}

func (s *openFailsAfterWorkingReaderSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	if s.opened > 0 {
		// Bounded: a fold that stopped reading would otherwise hang the package
		// rather than failing with a diff.
		select {
		case <-s.failed:
		case <-time.After(5 * time.Second):
			s.t.Error("the first worker never reached its failing read")
		}
		return nil, fmt.Errorf("injected reader-open failure")
	}
	s.opened++
	return &signallingFailingReader{
		containsBatchReader: s.fixture.reader(s.t, keys),
		failed:              s.failed,
	}, nil
}

// signallingFailingReader fails every read and announces the first failure, so
// the source above can order the open failure after it.
type signallingFailingReader struct {
	containsBatchReader
	failed chan struct{}
	once   sync.Once
}

func (r *signallingFailingReader) Next(mergeConc int) (*sroar.Bitmap, func(), error) {
	r.once.Do(func() { close(r.failed) })
	return nil, nil, fmt.Errorf("injected read failure")
}

// TestDocBitmapContainsBatch_ParallelOpenErrorKeepsWorkerError pins that a
// reader-open failure past the first worker does not swallow the error a
// running worker produced — which is the one an operator would act on.
func TestDocBitmapContainsBatch_ParallelOpenErrorKeepsWorkerError(t *testing.T) {
	forceContainsWorkers(t, 4)

	fixture, keys := newParallelContainsBatchFixture(t)
	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := newFoldSearcher(t, pool)
	pv := &propValuePair{
		operator:     filters.ContainsAny,
		containsKeys: sortedKeysFromStrings(t, keys),
	}

	src := &openFailsAfterWorkingReaderSource{
		fixture: fixture, t: t, failed: make(chan struct{}),
	}
	dbm, _, err := s.docBitmapContainsBatch(t.Context(), src, pv)
	require.Error(t, err)
	require.Equal(t, docBitmap{}, dbm)

	require.ErrorContains(t, err, "injected reader-open failure",
		"the open failure that stopped the fold must be reported")
	require.ErrorContains(t, err, "injected read failure",
		"and so must the error the worker already running produced")

	requireNoLeakedRows(t, fixture, pool)
}

// panicAfterReadsReader serves rows, then panics — how a corrupt serialized
// bitmap reaches this code, through roaringset's own panic rather than an error.
type panicAfterReadsReader struct {
	containsBatchReader
	afterReads int
	reads      int
}

func (r *panicAfterReadsReader) Next(mergeConc int) (*sroar.Bitmap, func(), error) {
	// Before delegating, where the real panic fires: roaringset's
	// requireEvenLength runs inside cloneBytesToBuf ahead of the pool fetch.
	if r.reads >= r.afterReads {
		panic("injected read panic")
	}
	r.reads++
	return r.containsBatchReader.Next(mergeConc)
}

// panickingSource hands out readers that panic once they have served enough
// rows for a worker to be holding a merged partial.
type panickingSource struct {
	fixture    *containsBatchFixture
	t          *testing.T
	afterReads int
}

func (s *panickingSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	return &panicAfterReadsReader{
		containsBatchReader: s.fixture.reader(s.t, keys),
		afterReads:          s.afterReads,
	}, nil
}

// TestDocBitmapContainsBatch_PanicReleasesEverythingHeld pins that a fold which
// panics mid-share drops neither its running partial nor the row it was handed.
//
// Incremental only: the accumulator arms hold no pooled buffer for a panic to
// lose, so the assertion below would pass on them whatever the guard did.
func TestDocBitmapContainsBatch_PanicReleasesEverythingHeld(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		gate    int
	}{
		{name: "one walk", workers: 1, gate: 1 << 30},
		{name: "across workers", workers: 2, gate: 1 << 30},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// The integration job sets DISABLE_RECOVERY_ON_PANIC=true, which would
			// let the injected panic kill the binary instead of becoming an error.
			t.Setenv("DISABLE_RECOVERY_ON_PANIC", "false")

			forceContainsWorkers(t, tc.workers)
			forceContainsAccumulatorGate(t, tc.gate)

			fixture, keys := newParallelContainsBatchFixture(t)
			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			// Two rows first, so a merged partial is held when the third read
			// panics; at 0 there is nothing to lose and the test is vacuous.
			src := &panickingSource{fixture: fixture, t: t, afterReads: 2}
			run := func() (err error) {
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("recovered: %v", r)
					}
				}()
				_, _, err = s.docBitmapContainsBatch(t.Context(), src, pv)
				return err
			}
			// The sequential arm propagates the panic; the parallel one's error
			// group returns it instead.
			err := run()
			require.Error(t, err)
			require.ErrorContains(t, err, "injected read panic")

			require.Zero(t, fixture.pool.Outstanding(),
				"the partial the fold had merged must go back to the pool")
			require.Zero(t, pool.Outstanding())
		})
	}
}

// TestDocBitmapContainsBatch_SequentialAccumulatorOpenError covers the
// accumulator's own open-error arm, which the incremental fold's does not
// share and which no other test reaches.
func TestDocBitmapContainsBatch_SequentialAccumulatorOpenError(t *testing.T) {
	forceContainsWorkers(t, 1)
	forceContainsAccumulatorGate(t, 2) // any batch takes the accumulator

	fixture, keys := newParallelContainsBatchFixture(t)
	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := newFoldSearcher(t, pool)
	pv := &propValuePair{
		operator:     filters.ContainsAny,
		containsKeys: sortedKeysFromStrings(t, keys),
	}

	src := &openFailingSource{fixture: fixture, t: t, failAt: 0}
	dbm, plan, err := s.docBitmapContainsBatch(t.Context(), src, pv)
	require.ErrorContains(t, err, "injected reader-open failure")
	require.Equal(t, foldStrategyUnionAccumulator, plan.strategy,
		"the accumulator's arm is the one under test")
	require.Equal(t, docBitmap{}, dbm)

	requireNoLeakedRows(t, fixture, pool)
}

// TestDocBitmapContainsBatch_ParallelDeadlineKeepsItsError pins that a deadline
// reaches the caller as a deadline rather than as a plain cancellation.
//
// A worker watches the error group's context, which descends from the caller's,
// so what it returns is whatever propagated down the chain. If propagation
// flattened the two, a timed-out query would surface as a generic cancel and an
// operator would lose the one fact that says which it was.
func TestDocBitmapContainsBatch_ParallelDeadlineKeepsItsError(t *testing.T) {
	forceContainsWorkers(t, 4)

	fixture, keys := newParallelContainsBatchFixture(t)
	// already past, so every worker sees it on its first check and the test
	// needs no sleep to reach the state
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := newFoldSearcher(t, pool)
	pv := &propValuePair{
		operator:     filters.ContainsAny,
		containsKeys: sortedKeysFromStrings(t, keys),
	}

	dbm, _, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"a deadline must not reach the caller as a plain cancellation")
	require.Equal(t, docBitmap{}, dbm)

	requireNoLeakedRows(t, fixture, pool)
}

// TestDocBitmapContainsBatch_ParallelContextCancelled pins that a query
// cancelled while the workers are walking returns the cancellation rather than
// whatever they had reached, and hands every row back.
//
// The result is deterministic even though which worker notices first is not:
// the cancel lands on the first read, so no worker can reach its last one. A
// cancellation arriving after they all finish is the other case, and
// TestDocBitmapContainsBatch_CancelledAfterWorkers pins that the fold keeps its
// result there.
func TestDocBitmapContainsBatch_ParallelContextCancelled(t *testing.T) {
	tests := []struct {
		name string
		gate int // containsAccumulatorMinKeysPerWorker, lowered to force the accumulator
		// asserted rather than inferred: the gate is read per worker, so which
		// fold a value selects depends on the plan's own worker count
		wantStrategy containsFoldStrategy
	}{
		{name: "incremental fold", gate: 256, wantStrategy: foldStrategyUnionIncremental},
		{name: "accumulator fold", gate: 2, wantStrategy: foldStrategyUnionAccumulator},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forceContainsWorkers(t, 4)
			forceContainsAccumulatorGate(t, tc.gate)

			fixture, keys := newParallelContainsBatchFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture.onRead = func(numReads int) {
				if numReads == 1 {
					cancel()
				}
			}

			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			dbm, plan, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, tc.wantStrategy, plan.strategy,
				"a cancelled fold still reports the plan it was running")
			require.Equal(t, docBitmap{}, dbm)

			requireNoLeakedRows(t, fixture, pool)
		})
	}
}

// TestDocBitmapContainsBatch_ParallelContainsAllEarlyExitIsNotAnError pins the
// difference between the fold stopping itself and the query being cancelled. A
// worker that proves its own share empty stops the other workers, and a worker
// stopped that way must not report the stop as a failure — a ContainsAll filter
// that folds to nothing answers "no results", not "context canceled".
//
// The first worker empties after two reads while the second still has most of
// a hundred-key share to walk, so the second is normally mid-loop when the
// fetch is stopped. That is a timing expectation, not a guarantee: the second
// worker's rows are small, and under a mutation of the exit it still finished
// first on 2 runs in 30, so the second share's rows are sized to make the claim
// unconditional rather than left to the scheduler.
func TestDocBitmapContainsBatch_ParallelContainsAllEarlyExitIsNotAnError(t *testing.T) {
	const numKeys = 200 // two shares of 100

	// The second worker's rows are wide enough that it cannot finish its share
	// before the first stops it. Left to the scheduler the two race, and the
	// assertion on what was left unread fails on correct code.
	fat := make([]uint64, 200_000)
	for i := range fat {
		fat[i] = uint64(i)
	}

	keys := make([]string, numKeys)
	rows := map[string][]uint64{}
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
		if i < numKeys/2 {
			rows[keys[i]] = []uint64{1, 2, 3}
		} else {
			rows[keys[i]] = fat
		}
	}
	// the first worker's first two keys are disjoint, so its own share is
	// provably empty two reads in
	rows[keys[0]] = []uint64{1, 2}
	rows[keys[1]] = []uint64{8, 9}

	forceContainsWorkers(t, 2)
	fixture := newContainsBatchFixture(t, context.Background(), rows)
	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := newFoldSearcher(t, pool)
	pv := &propValuePair{
		operator:     filters.ContainsAll,
		containsKeys: sortedKeysFromStrings(t, keys),
	}

	dbm, _, err := s.docBitmapContainsBatch(t.Context(), fixture.source(t), pv)
	require.NoError(t, err, "the fold stopping itself must not surface as a failed query")
	require.Empty(t, dbm.docIDs.ToArray())
	dbm.release()

	// At one core the workers run one after another, so the second finishes its
	// share before the first can stop it — nothing left unread, without
	// anything being wrong with the exit.
	if concurrency.GOMAXPROCS > 1 {
		require.Less(t, len(fixture.reads), numKeys,
			"the early exit must have stopped a worker mid-share, which is the case under test")
	}
	requireNoLeakedRows(t, fixture, pool)
}

// TestDocBitmapContainsBatch_MemoryClampBoundsTheFold takes the clamp off the
// planner's own arithmetic and onto the production path: a real fold, over a
// real bucket, through the reader source production uses, with nothing forced.
// What TestClampWorkersBoundaries pins as a number has to hold here as
// readers actually opened and window bytes actually held.
//
// The shard sizes are stubbed rather than built — the point is what a 200M-doc
// shard makes the planner do, not storing 200M docs to find out.
func TestDocBitmapContainsBatch_MemoryClampBoundsTheFold(t *testing.T) {
	tests := []struct {
		name       string
		docIDCount uint64
		want       int
		// clampBinds says the memory clamp, not the cores or the key count,
		// holds want down; a row where it does not must skip, not pass.
		clampBinds bool
	}{
		{name: "an empty shard pays only for its windows", docIDCount: 0, want: 8},
		{name: "the row term has cost half the pool", docIDCount: 33_488_896, want: 4, clampBinds: true},
		{name: "one doc past the three-worker boundary", docIDCount: 55_836_673, want: 2, clampBinds: true},
		{name: "a shard this size folds alone", docIDCount: 300_000_000, want: 1, clampBinds: true},
	}

	// Enough keys that the key count never binds before the clamp does, and
	// enough per row that the windows hold something worth measuring.
	const numKeys, docsPerKey = 256, 500
	flushed, unflushed := map[string][]uint64{}, map[string][]uint64{}
	keys := make([]string, numKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
		row := make([]uint64, docsPerKey)
		for j := range row {
			row[j] = uint64(i*docsPerKey + j)
		}
		// interleaved, so every contiguous share holds unflushed keys and every
		// worker therefore fills windows of its own
		if i%2 == 0 {
			flushed[keys[i]] = row
		} else {
			unflushed[keys[i]] = row
		}
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// This case drives the production path rather than plan, so the
			// caller's own min(budget, GOMAXPROCS) applies and the core count
			// is part of the answer. The table is pinned against plan directly
			// in TestClampWorkersBoundaries.
			want := min(tc.want, concurrency.GOMAXPROCS, numKeys)

			// What the fold would take with no memory clamp at all.
			unclamped := max(1, min(concurrency.GOMAXPROCS,
				numKeys/containsMinKeysPerWorker))
			if tc.clampBinds && unclamped <= tc.want {
				t.Skipf("this runner allows %d workers, at or below the %d this row "+
					"clamps to, so the clamp is not what would bind", unclamped, tc.want)
			}

			fixture := newContainsBatchFixtureSplit(t, context.Background(), flushed, unflushed)
			logger, _ := test.NewNullLogger()
			s := &Searcher{
				logger: logger,
				bitmapFactory: roaringset.NewBitmapFactory(fixture.pool,
					func() uint64 { return tc.docIDCount }),
			}

			view := fixture.GetConsistentView()
			defer view.ReleaseView()
			source := &roaringSetBatchReaderSource{view: view.WithoutEmptyActiveMemtable()}

			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: sortedKeysFromStrings(t, keys),
			}
			dbm, plan, err := s.docBitmapContainsBatch(t.Context(), source, pv)
			require.NoError(t, err)
			require.Len(t, dbm.docIDs.ToArray(), numKeys*docsPerKey)
			dbm.release()

			require.Equal(t, want, plan.workers)
			require.Len(t, source.readers, want,
				"the plan's worker count is what memory is spent on, so it must be what was opened")

			st, ok := source.stats()
			require.True(t, ok)
			require.Positive(t, st.Memtables, "the windows must have had a memtable to fill from")
			require.Positive(t, st.BytesPeak, "a fold that held no window measures nothing")

			// The clamp's whole claim: what the readers hold at once is bounded
			// by the workers the budget granted, not by the core count that was
			// asked for. Per reader first, so a single overlarge window cannot
			// hide inside a total that several small ones keep under the bound.
			for i, r := range source.readers {
				require.LessOrEqualf(t, int64(r.Stats().BytesPeak), int64(lsmkv.BatchReaderWindowBytes),
					"reader %d held more than one window's allowance", i)
			}
			require.LessOrEqual(t, int64(st.BytesPeak), int64(want)*int64(lsmkv.BatchReaderWindowBytes),
				"the fold held more than the workers it was granted could account for")
			if tc.clampBinds {
				footprint := containsFoldPlanner{docIDCount: tc.docIDCount}.
					perWorkerFootprintBytes()
				// Above one worker only: the clamp floors at one, so a shard
				// whose single worker already costs more than the budget runs
				// anyway rather than not running at all.
				if plan.workers > 1 {
					require.LessOrEqual(t, int64(plan.workers)*footprint,
						containsFoldMemoryBudget,
						"the workers the fold planned must fit the budget")
				}
				require.Greater(t, int64(plan.workers+1)*footprint,
					containsFoldMemoryBudget,
					"one more worker must not fit, or something other than the "+
						"memory clamp is what bound this fold")
			}

			require.Zero(t, fixture.pool.Outstanding())
		})
	}
}

// TestDocBitmapContainsBatch_RowsHeldPerWorker pins the row term
// perWorkerFootprintBytes charges, which the clamp's own arithmetic cannot
// check and BytesPeak does not see — that counter measures memtable window
// bytes, while these are pooled bitmaps.
//
// The sequential cases are the model itself, one worker's worth: the
// incremental fold holds its running partial and the row it is merging in at
// once, and the accumulator holds only the row, since its staging is plain
// memory rather than a pooled buffer. Charging one row where the incremental
// fold holds two is what let the clamp admit half again as many workers as the
// budget covers.
//
// The parallel cases assert a floor as well as a ceiling. A ceiling alone
// cannot fail: left to the scheduler the workers finish in turn and never hold
// buffers together, so the peak sits far below any bound a per-worker
// regression would break. A barrier holds each worker while it has a row, so
// the floor is what the fold holds at once rather than what a race allowed.
func TestDocBitmapContainsBatch_RowsHeldPerWorker(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		gate    int
		// want is exact where one worker makes it deterministic, and a ceiling
		// where several workers make it a race
		want  int64
		exact bool
		// atLeast is what proves the workers overlapped at all, without which
		// the ceiling above is a bound nothing can exceed
		atLeast int64
	}{
		{name: "incremental holds a partial and a row", workers: 1, gate: 256, want: 2, exact: true},
		{name: "accumulator holds only a row", workers: 1, gate: 2, want: 1, exact: true},
		{name: "incremental over four workers", workers: 4, gate: 256, want: 2*4 + 1, atLeast: 4},
		{name: "accumulator over four workers", workers: 4, gate: 2, want: 4 + 1, atLeast: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forceContainsWorkers(t, tc.workers)
			forceContainsAccumulatorGate(t, tc.gate)

			fixture, keys := newParallelContainsBatchFixture(t)
			if tc.atLeast > 0 {
				// Hold every worker while it has a row, so what the fold holds
				// at once is what the pool sees rather than a race the workers
				// usually win by finishing in turn.
				var (
					mu      sync.Mutex
					arrived int64
					open    = make(chan struct{})
				)
				fixture.onHold = func() {
					mu.Lock()
					arrived++
					if arrived == tc.atLeast {
						close(open)
					}
					mu.Unlock()
					// Bounded, and t.Error rather than require: FailNow on a
					// worker goroutine is a runtime.Goexit that leaves the fold
					// hung, and an unbounded wait buries the message.
					select {
					case <-open:
					case <-time.After(5 * time.Second):
						mu.Lock()
						t.Errorf("only %d of %d workers reached the barrier",
							arrived, tc.atLeast)
						mu.Unlock()
					}
				}
			}
			s := newFoldSearcher(t, roaringset.NewBitmapBufPoolNoop())
			pv := &propValuePair{
				operator:     filters.ContainsAny,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			dbm, plan, err := s.docBitmapContainsBatch(t.Context(), fixture.source(t), pv)
			require.NoError(t, err)
			require.Equal(t, tc.workers, plan.workers)
			dbm.release()

			// the bucket's pool, which is where a fetched row comes from
			peak := fixture.pool.PeakOutstanding()
			if tc.exact {
				require.Equal(t, tc.want, peak)
			} else {
				require.LessOrEqual(t, peak, tc.want)
				require.GreaterOrEqual(t, peak, tc.atLeast,
					"the workers must have held rows at the same time, or the ceiling bounds nothing")
			}
			require.Zero(t, fixture.pool.Outstanding())
		})
	}
}

// TestContainsBatchReaderSourceStatsSumThePeaks pins the peak as the fold's
// concurrent residency rather than its largest single reader. The readers hold
// their windows at the same time, so a caller sizing a machine against this
// number needs their total; reporting the largest would understate a
// workers-way fold by nearly that factor.
func TestContainsBatchReaderSourceStatsSumThePeaks(t *testing.T) {
	ctx := context.Background()
	rows := map[string][]uint64{}
	keys := make([]string, 64)
	for i := range keys {
		keys[i] = fmt.Sprintf("k%02d", i)
		rows[keys[i]] = []uint64{uint64(i), uint64(1000 + i)}
	}
	// unflushed, so the readers fill windows and have a peak to report at all
	fixture := newContainsBatchFixtureSplit(t, ctx, nil, rows)

	view := fixture.GetConsistentView()
	defer view.ReleaseView()
	source := &roaringSetBatchReaderSource{view: view.WithoutEmptyActiveMemtable()}

	sorted := sortedKeysFromStrings(t, keys)
	var wantPeak, wantFills int
	for w := range 4 {
		reader, err := source.newContainsBatchReader(sorted.Sub(w*16, (w+1)*16))
		require.NoError(t, err)
		for range reader.Len() {
			_, release, err := reader.Next(concurrency.SROAR_MERGE)
			require.NoError(t, err)
			release()
		}
		st := reader.(*lsmkv.RoaringSetBatchReader).Stats()
		require.Positive(t, st.BytesPeak)
		wantPeak += st.BytesPeak
		wantFills += st.Fills
	}

	got, ok := source.stats()
	require.True(t, ok)
	require.Equal(t, wantPeak, got.BytesPeak, "the peak is what the readers hold together")
	require.Equal(t, wantFills, got.Fills)
	require.Equal(t, sorted.Len(), got.KeysServed)
}

// lateFailingSource fails one key, but only once the rest of the batch has been
// read — so the workers that succeed have merged their shares into the fold's
// shared result before the failing one brings the fold down.
type lateFailingSource struct {
	fixture    *containsBatchFixture
	t          *testing.T
	failKey    string
	readsFirst int
}

func (s *lateFailingSource) newContainsBatchReader(keys entsInverted.SortedKeys) (containsBatchReader, error) {
	return &lateFailingReader{
		containsBatchReader: s.fixture.reader(s.t, keys),
		src:                 s,
		keys:                keys,
	}, nil
}

type lateFailingReader struct {
	containsBatchReader
	src  *lateFailingSource
	keys entsInverted.SortedKeys
	pos  int
}

func (r *lateFailingReader) Next(mergeConc int) (*sroar.Bitmap, func(), error) {
	key := ""
	if r.pos < r.keys.Len() {
		key = string(r.keys.At(r.pos))
	}
	r.pos++
	if key != r.src.failKey {
		return r.containsBatchReader.Next(mergeConc)
	}
	// Wait for every other key, then give the workers that read them a moment
	// to merge. There is no happens-before edge to wait on — a worker merges
	// after its own loop, which nothing here observes — so a failure to line up
	// makes this test weaker rather than wrong: it would assert the same
	// release, on a fold that had merged less.
	deadline := time.Now().Add(5 * time.Second)
	for {
		r.src.fixture.mu.Lock()
		reads := len(r.src.fixture.reads)
		r.src.fixture.mu.Unlock()
		if reads >= r.src.readsFirst {
			break
		}
		// Bounded: the other workers advance this counter, and a fold that
		// stopped reading would otherwise spin here until the package timeout.
		if time.Now().After(deadline) {
			r.src.t.Errorf("only %d of %d reads landed before the injected failure",
				reads, r.src.readsFirst)
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	return nil, nil, fmt.Errorf("injected read failure")
}

// TestDocBitmapContainsBatch_ParallelReadErrorAfterMerges pins the release on
// the path a read error takes once other workers have already merged: what the
// fold is holding at that point is a pooled buffer nobody will ever be handed,
// and dropping it there leaks with nothing to say so.
//
// The plain read-error test cannot cover this — its failure races the other
// workers, so whether anything had been merged when the fold unwound is not
// something it decides.
func TestDocBitmapContainsBatch_ParallelReadErrorAfterMerges(t *testing.T) {
	forceContainsWorkers(t, 4)

	fixture, keys := newParallelContainsBatchFixture(t)
	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := newFoldSearcher(t, pool)
	pv := &propValuePair{
		operator:     filters.ContainsAny,
		containsKeys: sortedKeysFromStrings(t, keys),
	}

	// the batch's last key, so every other worker has finished reading by the
	// time it is asked for
	dbm, _, err := s.docBitmapContainsBatch(t.Context(), &lateFailingSource{
		fixture: fixture, t: t,
		failKey: keys[len(keys)-1], readsFirst: len(keys) - 1,
	}, pv)
	require.ErrorContains(t, err, "injected read failure")
	require.Equal(t, docBitmap{}, dbm)

	requireNoLeakedRows(t, fixture, pool)
}

// TestDocBitmapContainsBatch_CancelledAfterWorkers pins the deliberate answer
// on the one path that carries no error of its own: the batch was fully read
// and merged, and the cancellation landed in the window before the fold
// returned. The fold answers with the result rather than the cancellation,
// because the work is already paid for and the result is complete — a worker
// stopped mid-read fails the fold instead, so nothing partial reaches here.
//
// All four folds are covered, since the strategy a batch selects must not
// decide what a cancelled query gets back. Cancelling on the batch's final read
// is what makes it this path rather than the read loop's: no worker has an
// iteration left in which to notice.
func TestDocBitmapContainsBatch_CancelledAfterWorkers(t *testing.T) {
	tests := []struct {
		name     string
		operator filters.Operator
		workers  int
		gate     int
		// asserted rather than inferred: the gate is read per worker, so which
		// fold a value selects depends on the plan's own worker count
		wantStrategy containsFoldStrategy
	}{
		{
			name: "incremental, sequential", operator: filters.ContainsAny,
			workers: 1, gate: 256, wantStrategy: foldStrategyUnionIncremental,
		},
		{
			name: "accumulator, sequential", operator: filters.ContainsAny,
			workers: 1, gate: 2, wantStrategy: foldStrategyUnionAccumulator,
		},
		{
			name: "incremental, parallel", operator: filters.ContainsAny,
			workers: 4, gate: 256, wantStrategy: foldStrategyUnionIncremental,
		},
		{
			name: "accumulator, parallel", operator: filters.ContainsAny,
			workers: 4, gate: 2, wantStrategy: foldStrategyUnionAccumulator,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			forceContainsWorkers(t, tc.workers)
			forceContainsAccumulatorGate(t, tc.gate)

			fixture, keys := newParallelContainsBatchFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			fixture.onRead = func(numReads int) {
				if numReads == len(keys) {
					cancel()
				}
			}

			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := newFoldSearcher(t, pool)
			pv := &propValuePair{
				operator:     tc.operator,
				containsKeys: sortedKeysFromStrings(t, keys),
			}

			dbm, plan, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
			require.NoError(t, err, "a batch that finished before the cancellation keeps its result")
			require.Equal(t, tc.wantStrategy, plan.strategy)
			require.Equal(t, parallelFixtureUnion(), dbm.docIDs.ToArray(),
				"the result must be the whole batch, not a share of it")
			dbm.release()

			require.Len(t, fixture.reads, len(keys),
				"the cancellation must land after the batch was read, not during it")
			requireNoLeakedRows(t, fixture, pool)
		})
	}
}

// TestDocBitmapContainsBatch_ParallelContainsAllStopsOnCrossShareEmpty pins the
// exit the split makes possible to miss: two shares that are each non-empty,
// whose intersection is not. No worker can see it — each only ever holds its
// own share — so it only appears where the shares meet, and a fold that waited
// for every worker before looking would read the whole batch to answer nothing.
//
// The third worker's share is long and its reads are slow, so it is certainly
// still reading when the first two meet. What proves the exit fired is that the
// batch was not fully read.
func TestDocBitmapContainsBatch_ParallelContainsAllStopsOnCrossShareEmpty(t *testing.T) {
	const perShare, workers = 40, 3
	numKeys := perShare * workers

	// The last worker's rows are large enough that reading and intersecting them
	// takes far longer than the first two shares take in total. Shares are equal
	// in KEY count, so without that skew all three finish at once and there is
	// nobody left to stop — which is the honest limit of this exit.
	fat := make([]uint64, 200_000)
	for i := range fat {
		fat[i] = uint64(i)
	}

	keys := make([]string, numKeys)
	rows := map[string][]uint64{}
	for i := range keys {
		keys[i] = fmt.Sprintf("k%03d", i)
		switch i / perShare {
		case 0:
			rows[keys[i]] = []uint64{1, 2} // worker 0 intersects to {1,2}
		case 1:
			rows[keys[i]] = []uint64{3, 4} // worker 1 to {3,4} — disjoint from it
		default:
			rows[keys[i]] = fat // worker 2 stays non-empty, and slow
		}
	}

	forceContainsWorkers(t, workers)
	fixture := newContainsBatchFixture(t, context.Background(), rows)

	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := newFoldSearcher(t, pool)
	pv := &propValuePair{
		operator:     filters.ContainsAll,
		containsKeys: sortedKeysFromStrings(t, keys),
	}

	dbm, _, err := s.docBitmapContainsBatch(t.Context(), fixture.source(t), pv)
	require.NoError(t, err)
	require.Empty(t, dbm.docIDs.ToArray())
	dbm.release()

	// Only where the workers can actually overlap. On a single-processor run
	// they are interleaved rather than parallel, and the worker still reading
	// runs to the end of its share before the goroutine that cancelled it is
	// scheduled again — so the whole batch is read and the exit has stopped
	// nothing, without anything being wrong with the exit.
	if concurrency.GOMAXPROCS > 1 {
		require.Less(t, len(fixture.reads), numKeys,
			"the shares meeting empty must stop the workers still reading")
	}
	requireNoLeakedRows(t, fixture, pool)
}

// keysFrom builds a [entsInverted.SortedKeys] from literal keys so tests can
// state the keys they mean without mirroring a builder. Build orders them and
// drops duplicates, so the result is the distinct keys in ascending order
// whatever order they are given in.
func keysFrom(tb testing.TB, keys ...[]byte) entsInverted.SortedKeys {
	tb.Helper()
	total := 0
	for _, k := range keys {
		total += len(k)
	}
	kb := entsInverted.NewVarKeyBuilder(len(keys), total)
	for _, k := range keys {
		kb.AppendString(string(k))
	}
	built, err := kb.Build()
	require.NoError(tb, err)
	return built
}

// collectKeys materializes a [entsInverted.SortedKeys] as a [][]byte, so tests
// can compare against the keys they expect without asserting through an
// accessor.
func collectKeys(keys entsInverted.SortedKeys) [][]byte {
	out := make([][]byte, 0, keys.Len())
	for _, k := range keys.All() {
		out = append(out, k)
	}
	return out
}

// smallShard is a doc-ID ceiling low enough that the row term is negligible
// beside the window term, for tests about anything other than the clamp.
const smallShard = uint64(1_000_000)

// TestPlanContainsFold pins the routing matrix: which strategy an operator and
// key count select. TestPlanContainsFoldMemoryClamp covers what the shard's
// size does to the worker count.
func TestPlanContainsFold(t *testing.T) {
	// what a top-level query carries: two per core
	// Stated rather than derived from the machine: plan now takes the budget
	// already collapsed with GOMAXPROCS, so a test says how many workers the
	// caller would allow and the case reads the same on any host.
	const workerBudget = 64

	tests := []struct {
		name     string
		operator filters.Operator
		numKeys  int
		// budget overrides workerBudget where a case needs a specific worker
		// count to mean what it says; zero takes what a top-level query carries
		budget int
		// minCores skips a case that cannot mean what it says below that many
		// cores, since plan takes the minimum of the budget and GOMAXPROCS
		minCores     int
		wantStrategy containsFoldStrategy
		wantErr      string
	}{
		{
			name:     "ContainsAll intersects however big the batch",
			operator: filters.ContainsAll, numKeys: 1024,
			wantStrategy: foldStrategyIntersection,
		},
		{
			// budget 1, so the batch is the whole of one worker's share and the
			// gate reads it directly
			name:     "ContainsAny one key below its worker's share merges incrementally",
			operator: filters.ContainsAny, numKeys: containsAccumulatorMinKeysPerWorker - 1,
			budget:       1,
			wantStrategy: foldStrategyUnionIncremental,
		},
		{
			name:     "ContainsAny at its worker's share switches to the accumulator",
			operator: filters.ContainsAny, numKeys: containsAccumulatorMinKeysPerWorker,
			budget:       1,
			wantStrategy: foldStrategyUnionAccumulator,
		},
		{
			// the batch clears the gate but the split does not: every worker's
			// share is a fraction of it, and the share is what stages.
			// budget 3, so three shares of a two-gate batch land below the gate
			// whatever the core count
			name:     "a batch that clears the gate can still be split below it",
			operator: filters.ContainsAny, numKeys: 2 * containsAccumulatorMinKeysPerWorker,
			budget:       3,
			minCores:     3,
			wantStrategy: foldStrategyUnionIncremental,
		},
		{
			name:     "ContainsNone plans as its ContainsAny union",
			operator: filters.ContainsNone, numKeys: containsAccumulatorMinKeysPerWorker,
			budget:       1,
			wantStrategy: foldStrategyUnionAccumulator,
		},
		{
			name:     "non-contains operator is rejected",
			operator: filters.OperatorEqual, numKeys: 10,
			wantErr: "unsupported operator",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			budget := tc.budget
			if budget == 0 {
				budget = workerBudget
			}
			plan, err := containsFoldPlanner{docIDCount: smallShard}.plan(budget, tc.operator, tc.numKeys)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				require.Equal(t, containsFoldPlan{}, plan)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantStrategy, plan.strategy)
		})
	}
}

// TestPlanContainsFoldWorkSizeFloor pins that the plan sizes the worker count
// by the work a worker would get, not only by what the machine and the budget
// could afford. A batch too small to divide into shares worth having is walked
// by one worker, whatever the core count.
//
// The budget is deliberately larger than any count this asks for, so the floor
// is what the assertions are reading rather than GOMAXPROCS or the clamp.
func TestPlanContainsFoldWorkSizeFloor(t *testing.T) {
	tests := []struct {
		numKeys int
		want    int
	}{
		{numKeys: 1, want: 1},
		{numKeys: containsMinKeysPerWorker - 1, want: 1},
		{numKeys: containsMinKeysPerWorker, want: 1},
		{numKeys: 2*containsMinKeysPerWorker - 1, want: 1},
		{numKeys: 2 * containsMinKeysPerWorker, want: 2},
		{numKeys: 4 * containsMinKeysPerWorker, want: 4},
	}

	// larger than any count below, so neither the budget nor the core count is
	// what the case is measuring
	const budget = 1024

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%d keys plans %d workers", tc.numKeys, tc.want), func(t *testing.T) {
			plan, err := containsFoldPlanner{docIDCount: smallShard}.plan(budget,
				filters.ContainsAll, tc.numKeys)
			require.NoError(t, err)
			require.Equal(t, tc.want, plan.workers)

			// the floor is a floor on the share, so no worker may be handed less
			// than one — which is the property the count is a means to
			from, to := plan.keyRangeFor(tc.numKeys, plan.workers-1)
			require.GreaterOrEqual(t, to-from, min(tc.numKeys, containsMinKeysPerWorker),
				"the smallest share must still be worth a worker")
		})
	}
}

// TestPlanContainsFoldGateUnchangedAtOneWorker pins what moving the gate to a
// per-worker denominator deliberately does NOT change: a plan that runs one
// worker decides exactly as it decided when there was no split to divide by.
//
// This is the claim that keeps the fold's arrival from re-tuning a threshold
// the sequential path has always used — so it is worth a test of its own rather
// than being left to follow from the arithmetic. It holds for both reasons a
// plan runs one worker: a batch too small to split, and a shard too large to
// afford a second reader.
func TestPlanContainsFoldGateUnchangedAtOneWorker(t *testing.T) {
	shards := []struct {
		name       string
		docIDCount uint64
	}{
		{name: "small batch on a small shard", docIDCount: smallShard},
		// past the clamp's last boundary, where one worker is all any batch
		// gets, but still inside the budget — so the memory giveaway in
		// planUnion does not fire and the gate is what decides the strategy.
		// TestPlanContainsFoldMemoryClamp covers the giveaway itself.
		{name: "large shard the clamp cuts to one worker", docIDCount: 150_000_000},
	}

	for _, shard := range shards {
		t.Run(shard.name, func(t *testing.T) {
			for _, numKeys := range []int{
				1,
				containsAccumulatorMinKeysPerWorker - 1,
				containsAccumulatorMinKeysPerWorker,
				containsAccumulatorMinKeysPerWorker + 1,
			} {
				t.Run(fmt.Sprintf("%d keys", numKeys), func(t *testing.T) {
					plan, err := containsFoldPlanner{docIDCount: shard.docIDCount}.plan(1,
						filters.ContainsAny, numKeys)
					require.NoError(t, err)
					require.Equal(t, 1, plan.workers, "this case is about the one-worker plan")

					// the pre-split rule, stated in its own terms rather than
					// derived from the one under test
					want := foldStrategyUnionIncremental
					if numKeys >= containsAccumulatorMinKeysPerWorker {
						want = foldStrategyUnionAccumulator
					}
					require.Equal(t, want, plan.strategy,
						"one worker must decide on the batch, exactly as the sequential fold did")
				})
			}
		})
	}
}

// TestPlanContainsFoldGateIsPerWorker pins the property the old batch-wide
// constant could not express: the same batch gates differently depending on
// how many ways it was split, because what stages an Accumulator is a worker's
// share and not the batch.
func TestPlanContainsFoldGateIsPerWorker(t *testing.T) {
	// a var, not a const: the gate is a package var so benchmarks can sweep it
	numKeys := 4 * containsAccumulatorMinKeysPerWorker

	// unsplit, the batch is one worker's share and clears the gate four times over
	unsplit, err := containsFoldPlanner{docIDCount: smallShard}.plan(1, filters.ContainsAny, numKeys)
	require.NoError(t, err)
	require.Equal(t, 1, unsplit.workers)
	require.Equal(t, foldStrategyUnionAccumulator, unsplit.strategy)

	// split eight ways the same batch gives each worker half the gate
	split, err := containsFoldPlanner{docIDCount: smallShard}.plan(8, filters.ContainsAny, numKeys)
	require.NoError(t, err)
	require.Greater(t, split.workers, 4, "the budget must actually split it for this to mean anything")
	require.Equal(t, foldStrategyUnionIncremental, split.strategy,
		"the same batch must gate on the share it was split into, not on its own size")
}

// TestDocBitmapContainsBatch_AnswerIsPlanInvariant sweeps every plan the
// searcher can reach for one batch and requires them all to answer the same
// thing.
//
// This is the branch's own risk rather than a general one. plan derives the
// strategy and the worker count from GOMAXPROCS, the query budget and the
// shard's doc-ID count, so two replicas on differently sized machines take
// different code paths for the same filter — and replication returns whichever
// replica answered. A plan-dependent answer would be user-visible
// non-determinism rather than a performance detail.
//
// ParallelMatchesSequential compares one worker against four; this walks the
// counts between them and crosses the strategy gate for a fixed input, which is
// where the two folds could diverge without either being obviously wrong.
func TestDocBitmapContainsBatch_AnswerIsPlanInvariant(t *testing.T) {
	fixture, keys := newParallelContainsBatchFixture(t)

	operators := []struct {
		name     string
		operator filters.Operator
	}{
		{"ContainsAny", filters.ContainsAny},
		{"ContainsAll", filters.ContainsAll},
		{"ContainsNone", filters.ContainsNone},
	}

	for _, op := range operators {
		t.Run(op.name, func(t *testing.T) {
			var (
				want     []uint64
				wantDeny bool
				first    string
				plans    []string
			)

			// every worker count the batch can be split into, against both
			// sides of the accumulator gate
			for _, gate := range []int{2, 256} {
				for workers := 1; workers <= 8; workers++ {
					name := fmt.Sprintf("gate=%d/workers=%d", gate, workers)

					forceContainsAccumulatorGate(t, gate)
					forceContainsWorkers(t, workers)

					s := newFoldSearcher(t, roaringset.NewBitmapBufPoolNoop())
					pv := &propValuePair{
						operator:     op.operator,
						containsKeys: sortedKeysFromStrings(t, keys),
					}

					dbm, plan, err := s.docBitmapContainsBatch(t.Context(), fixture.source(t), pv)
					require.NoError(t, err, name)
					got := dbm.docIDs.ToArray()
					dbm.release()

					if plans == nil {
						want, wantDeny, first = got, dbm.isDenyList, name
					}
					require.Equal(t, want, got,
						"%s answered differently from %s", name, first)
					require.Equal(t, wantDeny, dbm.isDenyList, name)

					plans = append(plans,
						fmt.Sprintf("%s (%s, %d workers)", name, plan.strategy, plan.workers))
				}
			}

			// the sweep is worthless if every leg planned the same fold
			require.Greater(t, len(distinctPlans(plans)), 1,
				"the sweep must have crossed a plan boundary: %v", plans)
		})
	}
}

func distinctPlans(plans []string) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, p := range plans {
		// everything after the leg name is the plan it produced
		key := p[strings.Index(p, "("):]
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	return out
}

// TestContainsFoldStrategyString pins the names the slow query log carries, and
// the fallback for a value outside the enum — which exists so a strategy added
// without a String case prints its number rather than borrowing a real
// strategy's name in an operator's log.
func TestContainsFoldStrategyString(t *testing.T) {
	tests := []struct {
		strategy containsFoldStrategy
		want     string
	}{
		{foldStrategyIntersection, "intersection"},
		{foldStrategyUnionIncremental, "union-incremental"},
		{foldStrategyUnionAccumulator, "union-accumulator"},
		{containsFoldStrategy(99), "unknown(99)"},
	}

	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			require.Equal(t, tc.want, tc.strategy.String())
		})
	}
}

// TestContainsFoldRunUnsupportedStrategy pins run's terminal arm. It is
// unreachable today — plan returns one of the three — and exists so a fourth
// strategy added without a run case fails loudly instead of taking whichever
// arm the switch falls through to. An arm with no test is the kind that gets
// simplified away, and the two sibling backstops both have one.
func TestContainsFoldRunUnsupportedStrategy(t *testing.T) {
	fold := containsFoldRunner{
		plan: containsFoldPlan{strategy: containsFoldStrategy(99), workers: 1},
	}

	bm, release, err := fold.run(t.Context())
	require.ErrorIs(t, err, entsInverted.ErrInternal)
	require.ErrorContains(t, err, "unknown(99)",
		"the message must name the value rather than a strategy it is not")
	require.Nil(t, bm)
	require.Nil(t, release)
}

// TestPerWorkerFootprintOverflowBoundary walks the guard that stops a row near
// the ceiling doubling into a negative. One unit either side: a guard with the
// wrong constant still saturates at MaxInt64, which is the only row the sibling
// test reaches.
func TestPerWorkerFootprintOverflowBoundary(t *testing.T) {
	boundary := (int64(math.MaxInt64) - lsmkv.BatchReaderWindowBytes) / 2

	tests := []struct {
		name string
		row  int64
		want int64
	}{
		{"one below the boundary fits", boundary - 1, 2*(boundary-1) + lsmkv.BatchReaderWindowBytes},
		{"the boundary itself fits", boundary, 2*boundary + lsmkv.BatchReaderWindowBytes},
		{"one above saturates", boundary + 1, math.MaxInt64},
		{"a saturated row saturates", math.MaxInt64, math.MaxInt64},
		{"an ordinary row is two rows and a window", 1_200_000, 2*1_200_000 + lsmkv.BatchReaderWindowBytes},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := perWorkerFootprintFor(tc.row)
			require.Equal(t, tc.want, got)
			require.Positive(t, got,
				"a footprint that wrapped negative would floor the clamp at one worker")
		})
	}
}

// TestRowFootprintBytes pins what the clamp is priced against, including the
// two shard sizes where the uint64 arithmetic would wrap. Nothing in production
// can reach those today — the shard's counter is monotonic and is the only
// producer — so the last rows exist to keep that true: a getter that loses its
// guard fails here rather than quietly buying the fold every worker.
func TestRowFootprintBytes(t *testing.T) {
	tests := []struct {
		name       string
		docIDCount uint64
		wantRow    int64
		// wantWorkers is what the clamp affords for that row, asked for
		// separately because the doubling has a ceiling of its own
		wantWorkers int
	}{
		{
			name:       "a shard holding nothing spans no range",
			docIDCount: 0, wantRow: 0, wantWorkers: 8,
		},
		{
			name:       "one object still holds a container",
			docIDCount: 1, wantRow: roaringContainerMaxBytes, wantWorkers: 7,
		},
		{
			name:       "a range boundary is not crossed early",
			docIDCount: roaringContainerRange, wantRow: roaringContainerMaxBytes, wantWorkers: 7,
		},
		{
			name:       "one doc past a range takes a second container",
			docIDCount: roaringContainerRange + 1, wantRow: 2 * roaringContainerMaxBytes, wantWorkers: 7,
		},
		{
			name:       "a shard large enough to fold alone",
			docIDCount: 300_000_000, wantRow: 37_539_600, wantWorkers: 1,
		},
		{
			name:       "the largest count the rounding survives",
			docIDCount: math.MaxUint64 - (roaringContainerRange - 1),
			wantRow:    2_308_094_809_027_371_000, wantWorkers: 1,
		},
		{
			name:       "one past it, where rounding up wraps to nothing",
			docIDCount: math.MaxUint64 - (roaringContainerRange - 1) + 1,
			wantRow:    math.MaxInt64, wantWorkers: 1,
		},
		{
			name:       "MaxUint64",
			docIDCount: math.MaxUint64, wantRow: math.MaxInt64, wantWorkers: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p := containsFoldPlanner{docIDCount: tc.docIDCount}

			require.Equal(t, tc.wantRow, p.rowFootprintBytes())
			require.Positive(t, p.perWorkerFootprintBytes(),
				"a negative divisor would floor the clamp by accident")
			require.Equal(t, tc.wantWorkers, p.clampWorkers(8))
		})
	}
}

// TestClampWorkers pins the memory clamp in isolation: how many worst-case
// workers the budget affords, and what happens when it affords none.
func TestClampWorkers(t *testing.T) {
	tests := []struct {
		name       string
		planned    int
		docIDCount uint64
		want       int
	}{
		{
			name:    "an unknown ceiling still pays for a window",
			planned: 64, docIDCount: 0, want: 8,
		},
		{
			name:    "a small shard is bounded by the window, not the rows",
			planned: 64, docIDCount: smallShard, want: 7,
		},
		{
			name:    "the row term bites on a large shard",
			planned: 64, docIDCount: 100_000_000, want: 2,
		},
		{
			name:    "one worker is all a very large shard affords",
			planned: 64, docIDCount: 300_000_000, want: 1,
		},
		{
			name:    "past the budget it floors at one rather than zero",
			planned: 64, docIDCount: 600_000_000, want: 1,
		},
		{
			name:    "it never raises what was planned",
			planned: 2, docIDCount: 0, want: 2,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, containsFoldPlanner{docIDCount: tc.docIDCount}.clampWorkers(tc.planned))
		})
	}
}

// clampBoundaries is where the budget stops affording a worker: the largest
// shard that still gets `workers`, and the doc ID one past it. They are stated
// rather than recomputed, so a change to the budget, the window or the
// container size has to be argued for here instead of quietly moving the whole
// table with the code that sets it.
var clampBoundaries = []struct {
	workers  int
	lastFits uint64
}{
	{workers: 8, lastFits: 0},
	{workers: 7, lastFits: 4_784_128},
	{workers: 6, lastFits: 11_141_120},
	{workers: 5, lastFits: 20_054_016},
	{workers: 4, lastFits: 33_488_896},
	{workers: 3, lastFits: 55_836_672},
	{workers: 2, lastFits: 100_532_224},
}

// TestClampWorkersBoundaries walks the crossover table one doc ID at a time
// across every step in it. Checkpoints inside a step only show that the clamp
// is in the right region; the boundaries are what pin the table itself, and
// they are what the memory argument is actually stated in terms of — a shard of
// 111.7M doc IDs still affording three concurrent windows, and one doc past it
// affording two.
// TestMaxContainsFoldWorkersBoundsEveryClamp pins that no shard size clamps
// above the constant the reader list is sized against. The widest plan is the
// empty-shard one — the footprint's floor is one window — and the sizes either
// side keep the constant honest if the footprint gains a term.
func TestMaxContainsFoldWorkersBoundsEveryClamp(t *testing.T) {
	for _, docIDCount := range []uint64{
		0, 1, 65_535, 65_536, 1_000_000, 33_488_896, 55_836_673, 300_000_000,
		math.MaxUint64 / 2, math.MaxUint64,
	} {
		p := containsFoldPlanner{docIDCount: docIDCount}
		require.LessOrEqualf(t, p.clampWorkers(1<<30), maxContainsFoldWorkers,
			"a shard of %d doc IDs clamped above the reader list's capacity", docIDCount)
	}
}

func TestClampWorkersBoundaries(t *testing.T) {
	for _, tc := range clampBoundaries {
		t.Run(fmt.Sprintf("%d workers", tc.workers), func(t *testing.T) {
			p := containsFoldPlanner{docIDCount: tc.lastFits}
			require.Equal(t, tc.workers, p.clampWorkers(64),
				"the largest shard that still affords %d workers", tc.workers)

			past := containsFoldPlanner{docIDCount: tc.lastFits + 1}
			require.Equal(t, tc.workers-1, past.clampWorkers(64),
				"one doc ID further must cost a worker")
		})
	}
}

// TestPlanContainsFoldMemoryClamp pins what the shard's size does to a plan. It
// sheds workers for every strategy; only the accumulator, which alone has
// somewhere to fall back to, gives up its strategy as well.
func TestPlanContainsFoldMemoryClamp(t *testing.T) {
	// what a top-level query carries: two per core
	// Stated rather than derived from the machine: plan now takes the budget
	// already collapsed with GOMAXPROCS, so a test says how many workers the
	// caller would allow and the case reads the same on any host.
	const workerBudget = 64
	// Large enough that every worker's share still clears the accumulator gate
	// after the clamp has split the batch — otherwise these cases would be
	// reading the gate rather than the clamp, which is what they are about. Eight
	// workers is the most any shard affords, so eight shares of the gate is the
	// size that holds for all of them.
	manyKeys := 8 * containsAccumulatorMinKeysPerWorker

	t.Run("an intersection sheds workers but keeps its strategy", func(t *testing.T) {
		plan, err := containsFoldPlanner{docIDCount: 100_000_000}.plan(workerBudget, filters.ContainsAll, manyKeys)
		require.NoError(t, err)
		require.Equal(t, foldStrategyIntersection, plan.strategy)
		require.Equal(t, 2, plan.workers)
	})

	t.Run("an accumulator sheds workers but keeps its strategy", func(t *testing.T) {
		plan, err := containsFoldPlanner{docIDCount: 100_000_000}.plan(workerBudget, filters.ContainsAny, manyKeys)
		require.NoError(t, err)
		require.Equal(t, foldStrategyUnionAccumulator, plan.strategy)
		require.Equal(t, 2, plan.workers)
	})

	t.Run("a shard too large for one worker gives up the strategy", func(t *testing.T) {
		plan, err := containsFoldPlanner{docIDCount: 600_000_000}.plan(workerBudget, filters.ContainsAny, manyKeys)
		require.NoError(t, err)
		require.Equal(t, foldStrategyUnionIncremental, plan.strategy,
			"with no worker left to shed, the only lever is which fold runs")
		require.Equal(t, 1, plan.workers)
	})

	t.Run("an empty shard still pays for its window", func(t *testing.T) {
		plan, err := containsFoldPlanner{docIDCount: 0}.plan(workerBudget, filters.ContainsAny, manyKeys)
		require.NoError(t, err)
		require.Equal(t, foldStrategyUnionAccumulator, plan.strategy)
		require.Equal(t, 8, plan.workers, "the window alone caps the pool")
	})
}

// TestPlanContainsFoldOverride pins the worker-count override at the one place
// it applies: whatever a strategy planned, the override replaces it, capped by
// the key count.
func TestPlanContainsFoldOverride(t *testing.T) {
	// what a top-level query carries: two per core
	// Stated rather than derived from the machine: plan now takes the budget
	// already collapsed with GOMAXPROCS, so a test says how many workers the
	// caller would allow and the case reads the same on any host.
	const workerBudget = 64

	for _, op := range []filters.Operator{filters.ContainsAll, filters.ContainsAny, filters.ContainsNone} {
		t.Run(op.Name(), func(t *testing.T) {
			forceContainsWorkers(t, 6)

			plan, err := containsFoldPlanner{docIDCount: smallShard}.plan(workerBudget, op, 100)
			require.NoError(t, err)
			require.Equal(t, 6, plan.workers)

			capped, err := containsFoldPlanner{docIDCount: smallShard}.plan(workerBudget, op, 3)
			require.NoError(t, err)
			require.Equal(t, 3, capped.workers, "the override is capped by the key count")

			// the budget and the clamp both bound what the policy asks for, but
			// the override is a deliberate pin: a benchmark forcing 6 workers
			// gets 6 whatever the query carries or the shard costs
			starved, err := containsFoldPlanner{docIDCount: smallShard}.plan(1, op, 100)
			require.NoError(t, err)
			require.Equal(t, 6, starved.workers, "the override outranks the query budget")

			overClamped, err := containsFoldPlanner{docIDCount: 600_000_000}.plan(workerBudget, op, 100)
			require.NoError(t, err)
			require.Equal(t, 6, overClamped.workers, "the override outranks the memory clamp")
		})
	}
}

// TestContainsEvenSplit pins that the shares tile the batch exactly and differ
// by at most one key — the property that keeps one worker from being handed a
// remainder while the others hold full shares.
func TestContainsEvenSplit(t *testing.T) {
	tests := []struct {
		name    string
		numKeys int
		workers int
		want    [][2]int
	}{
		{"an uneven split spreads the remainder", 4097, 5, [][2]int{{0, 820}, {820, 1640}, {1640, 2459}, {2459, 3278}, {3278, 4097}}},
		{"an even split needs no spreading", 100, 4, [][2]int{{0, 25}, {25, 50}, {50, 75}, {75, 100}}},
		{"one worker takes everything", 7, 1, [][2]int{{0, 7}}},
		{"more workers than keys leaves some empty", 2, 4, [][2]int{{0, 1}, {1, 2}, {2, 2}, {2, 2}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got [][2]int
			for w := 0; w < tc.workers; w++ {
				from, to := containsFoldPlan{workers: tc.workers}.keyRangeFor(tc.numKeys, w)
				got = append(got, [2]int{from, to})
			}
			require.Equal(t, tc.want, got)

			// the shares tile the batch: contiguous, starting at 0, ending at
			// numKeys, and no two differing by more than one key
			require.Equal(t, 0, got[0][0])
			require.Equal(t, tc.numKeys, got[len(got)-1][1])
			minShare, maxShare := tc.numKeys, 0
			for i, share := range got {
				if i > 0 {
					require.Equal(t, got[i-1][1], share[0], "shares must be contiguous")
				}
				n := share[1] - share[0]
				minShare, maxShare = min(minShare, n), max(maxShare, n)
			}
			assert.LessOrEqual(t, maxShare-minShare, 1, "no two shares may differ by more than one key")
		})
	}
}

// forceContainsWorkers pins the fetch-worker count for the duration of a
// test. It is package state, so a test calling this must not use t.Parallel.
func forceContainsWorkers(tb testing.TB, workers int) {
	tb.Helper()
	old := containsWorkersOverrideForTests
	containsWorkersOverrideForTests = workers
	tb.Cleanup(func() { containsWorkersOverrideForTests = old })
}

// forceContainsAccumulatorGate pins which union fold a batch selects, under the
// same constraint as forceContainsWorkers.
func forceContainsAccumulatorGate(tb testing.TB, gate int) {
	tb.Helper()
	old := containsAccumulatorMinKeysPerWorker
	containsAccumulatorMinKeysPerWorker = gate
	tb.Cleanup(func() { containsAccumulatorMinKeysPerWorker = old })
}

// requireNoLeakedRows asserts the fold released every row it was handed and
// left neither pool holding anything.
func requireNoLeakedRows(t *testing.T, fixture *containsBatchFixture, pool *roaringset.BitmapBufPoolTrackingForTests) {
	t.Helper()
	require.Equal(t, len(fixture.reads), fixture.releaseCalls,
		"every row the fold received must be released")
	if pool != nil {
		require.Zero(t, pool.Outstanding(), "no result buffer may outlive the fold")
	}
}
