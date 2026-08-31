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
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
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
	// mu guards the counters below. Reading the fields directly is safe only
	// after the fold has returned, which is where every assertion here happens.
	mu           sync.Mutex
	reads        []string
	releaseCalls int
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

// source hands the fold spied readers, so the reads of a fold that opens its
// own reader land in the same fixture.
//
// They come off ONE view, as production's do: a view is what pins the segments
// the readers walk, and taking one per reader would have each release its own
// while the others are still reading.
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
}

func (s *spyContainsBatchReaderSource) newContainsBatchReader(
	keys entsInverted.SortedKeys,
) (containsBatchReader, error) {
	rdr, err := lsmkv.NewRoaringSetBatchReader(s.view, keys)
	if err != nil {
		return nil, err
	}
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
// logger is not decoration: a fault on this path is logged as well as returned,
// so a Searcher without one is a shape production cannot produce — NewSearcher
// requires it — and only a literal here can.
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
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
				dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
		gate int // containsAnyAccumulatorMinKeys, lowered to force the accumulator
		// asserted rather than inferred: which fold a gate value selects is
		// what the arm below is named for
		wantStrategy containsFoldStrategy
	}{
		{name: "incremental fold", gate: 256, wantStrategy: foldStrategyUnionIncremental},
		{name: "accumulator fold", gate: 2, wantStrategy: foldStrategyUnionAccumulator},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			strategy, err := containsFoldStrategyFor(pv.operator, pv.containsKeys.Len())
			require.NoError(t, err)
			require.Equal(t, tc.wantStrategy, strategy, "the fixture must reach the fold under test")

			dbm, err := s.docBitmapContainsBatch(ctx,
				&failingContainsBatchReaderSource{fixture: fixture, t: t, failKey: "poison"},
				pv)
			require.ErrorContains(t, err, "read row")
			require.ErrorContains(t, err, "injected read failure")
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
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
	require.NoError(t, err)
	defer dbm.release()

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, dbm.docIDs.ToArray(),
		"ContainsNone folds the same union as ContainsAny")
	require.True(t, dbm.IsDenyList())
	require.Equal(t, []string{"missing", "present-a", "present-b"}, fixture.reads)
}

// Same folds as above but forced through the Accumulator path, which the
// containsAnyAccumulatorMinKeys gate would otherwise route to the incremental
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
			dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
			dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
		dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
		dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Empty(t, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "b"}, fixture.reads,
			"key c must not be read once the AND accumulator is provably empty")
	})

	t.Run("absent key empties the intersection and stops reading", func(t *testing.T) {
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
		dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
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
		gate int // containsAnyAccumulatorMinKeys, lowered to force the accumulator
		// asserted rather than inferred: which fold a gate value selects is
		// what the arm below is named for
		wantStrategy containsFoldStrategy
	}{
		{name: "incremental fold", gate: 256, wantStrategy: foldStrategyUnionIncremental},
		{name: "accumulator fold", gate: 2, wantStrategy: foldStrategyUnionAccumulator},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
			strategy, err := containsFoldStrategyFor(pv.operator, pv.containsKeys.Len())
			require.NoError(t, err)
			require.Equal(t, tc.wantStrategy, strategy, "the fixture must reach the fold under test")

			dbm, err := s.docBitmapContainsBatch(ctx, fixture.source(t), pv)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, docBitmap{}, dbm)

			require.Equal(t, []string{"a"}, fixture.reads, "the fold must stop reading once ctx is cancelled")
			require.Equal(t, 1, fixture.releaseCalls, "the row read before cancellation must be released")
		})
	}
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
// unreachable today — containsFoldStrategyFor returns one of the three — and
// exists so a fourth strategy added without a run case fails loudly instead of
// taking whichever arm the switch falls through to. An arm with no test is the
// kind that gets simplified away, and the two sibling backstops both have one.
func TestContainsFoldRunUnsupportedStrategy(t *testing.T) {
	fold := containsFoldRunner{strategy: containsFoldStrategy(99)}

	bm, release, err := fold.run(t.Context())
	require.ErrorIs(t, err, entsInverted.ErrInternal)
	require.ErrorContains(t, err, "unknown(99)",
		"the message must name the value rather than a strategy it is not")
	require.Nil(t, bm)
	require.Nil(t, release)
}

// forceContainsAccumulatorGate pins which union fold a batch selects. It writes
// a package var, so a test using it must not run in parallel with another.
func forceContainsAccumulatorGate(tb testing.TB, gate int) {
	tb.Helper()
	old := containsAnyAccumulatorMinKeys
	containsAnyAccumulatorMinKeys = gate
	tb.Cleanup(func() { containsAnyAccumulatorMinKeys = old })
}
