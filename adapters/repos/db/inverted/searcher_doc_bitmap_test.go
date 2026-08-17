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
	"testing"

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
	pool         *roaringset.BitmapBufPoolTrackingForTests
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
	rdr, err := lsmkv.NewRoaringSetBatchReader(view, keys)
	require.NoError(t, err)
	return &spyContainsBatchReader{reader: rdr, fixture: s, keys: keys}
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
		s.reads = append(s.reads, string(r.keys.At(r.pos)))
		if s.onRead != nil {
			s.onRead(len(s.reads))
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
		s.releaseCalls++
		release()
	}, nil
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

	return &containsBatchFixture{Bucket: b, pool: pool}
}

// TestDocBitmapContainsBatch_ReadsUnflushedRows folds a batch whose rows are
// split between disk and the active memtable, over enough keys to cross several
// windows. It is the only test that takes the fold, the reader, the windowing
// and the memtable walk together; the rest flush first and so exercise the disk
// path with the memtable skipped.
func TestDocBitmapContainsBatch_ReadsUnflushedRows(t *testing.T) {
	ctx := context.Background()

	// Three windows at the production size of 1024, so the walk crosses two
	// boundaries and ends on a narrower one, which is where an off-by-one in the
	// window's end shows. The size is not reachable from here — the exported
	// constructor picks it — so this is a key count rather than a multiple of it.
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
	s := &Searcher{bitmapFactory: roaringset.NewBitmapFactory(fixture.pool, func() uint64 { return 300_000 })}

	pv := &propValuePair{
		prop:           "some-prop",
		operator:       filters.ContainsAny,
		containsValues: keysFrom(t, batch...),
	}
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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
		operator:       filters.ContainsAny,
		containsValues: keysFrom(t, []byte("present-a"), []byte("missing"), []byte("present-b")),
	}

	s := &Searcher{}
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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
		operator:       filters.OperatorEqual,
		containsValues: keysFrom(t, []byte("present-a"), []byte("present-b")),
	}

	s := &Searcher{}
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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
// The count that decides it is the reader's, because the reader is what the
// fold walks. Taking it off pv instead lets a reader holding nothing reach a
// fold that runs no iterations and returns a nil bitmap with no error.
func TestDocBitmapContainsBatch_NoKeys(t *testing.T) {
	ctx := context.Background()
	fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{"present-a": {1, 2, 3}})

	tests := []struct {
		name   string
		pvKeys entsInverted.SortedKeys
	}{
		{name: "pv is empty too", pvKeys: keysFrom(t)},
		{name: "pv still carries keys", pvKeys: keysFrom(t, []byte("present-a"))},
	}

	for _, tc := range tests {
		for _, op := range []filters.Operator{filters.ContainsAny, filters.ContainsAll, filters.ContainsNone} {
			t.Run(tc.name+"/"+op.Name(), func(t *testing.T) {
				s := &Searcher{}
				pv := &propValuePair{prop: "some-prop", operator: op, containsValues: tc.pvKeys}
				dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, keysFrom(t)), pv)
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
	}{
		{name: "incremental fold", gate: 256},
		{name: "accumulator fold", gate: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldGate := containsAnyAccumulatorMinKeys
			containsAnyAccumulatorMinKeys = tc.gate
			defer func() { containsAnyAccumulatorMinKeys = oldGate }()

			ctx := context.Background()
			fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
				"a": {1, 2, 3},
				"z": {7, 8},
			})

			pool := roaringset.NewBitmapBufPoolTrackingForTests()
			s := &Searcher{bitmapFactory: roaringset.NewBitmapFactory(pool, func() uint64 { return 300_000 })}

			// "a" is read and accumulated, "poison" fails, "z" must never be
			// reached — the keys arrive ascending, so "poison" sits between them
			pv := &propValuePair{
				operator:       filters.ContainsAny,
				containsValues: keysFrom(t, []byte("a"), []byte("poison"), []byte("z")),
			}
			dbm, err := s.docBitmapContainsBatch(ctx,
				&failingContainsBatchReader{
					containsBatchReader: fixture.reader(t, pv.containsValues),
					failKey:             "poison",
					keys:                pv.containsValues,
				},
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
				operator:       tc.operator,
				containsValues: keysFrom(t, []byte("a"), []byte("b")),
			}
			_, err := pv.resolveDocIDs(context.Background(), &Searcher{}, 0)
			require.ErrorContains(t, err, "non-contains operator")
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
		operator:       filters.ContainsNone,
		containsValues: keysFrom(t, []byte("present-a"), []byte("missing"), []byte("present-b")),
	}

	s := &Searcher{}
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
	require.NoError(t, err)
	defer dbm.release()

	require.Equal(t, []uint64{1, 2, 3, 4, 5}, dbm.docIDs.ToArray(),
		"ContainsNone folds the same union as ContainsAny")
	require.True(t, dbm.IsDenyList())
	require.Equal(t, []string{"missing", "present-a", "present-b"}, fixture.reads)
}

// TestDocBitmapContainsBatch_AccumulatorGateReadsTheReader pins which count
// picks the fold. Both the emptiness guard and the gate read the reader rather
// than pv.containsValues, and every other test hands the two the same length —
// so reading the gate off pv instead would be invisible to all of them.
//
// The reader carries enough keys for the accumulator and pv carries fewer, so
// the two disagree about which side of the gate the batch falls on. The result
// has to be the union of what the reader holds either way; what would change is
// the fold that produced it, which the read counts make visible.
func TestDocBitmapContainsBatch_AccumulatorGateReadsTheReader(t *testing.T) {
	oldGate := containsAnyAccumulatorMinKeys
	containsAnyAccumulatorMinKeys = 3
	defer func() { containsAnyAccumulatorMinKeys = oldGate }()

	ctx := context.Background()
	fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
		"present-a": {1, 2}, "present-b": {3}, "present-c": {4},
	})

	readerKeys := keysFrom(t, []byte("present-a"), []byte("present-b"), []byte("present-c"))
	pool := roaringset.NewBitmapBufPoolTrackingForTests()
	s := &Searcher{bitmapFactory: roaringset.NewBitmapFactory(pool, func() uint64 { return 300_000 })}

	// pv is one key short of the gate, the reader is on it
	pv := &propValuePair{operator: filters.ContainsAny, containsValues: keysFrom(t, []byte("present-a"))}
	dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, readerKeys), pv)
	require.NoError(t, err)

	require.Equal(t, []uint64{1, 2, 3, 4}, dbm.docIDs.ToArray(),
		"the fold must cover every key the reader carries, not pv's shorter list")
	require.Equal(t, []string{"present-a", "present-b", "present-c"}, fixture.reads,
		"every key of the reader's batch must be read")

	// Which fold ran, stated as something observable: the accumulator deposits
	// each row and releases it straight away, so every read is released during
	// the fold. The incremental fold adopts its first row instead and hands that
	// release to the caller, so one fewer.
	require.Equal(t, len(fixture.reads), fixture.releaseCalls,
		"the reader's count puts this batch on the accumulator side of the gate")
	dbm.release()
}

// Same folds as above but forced through the Accumulator path, which the
// containsAnyAccumulatorMinKeys gate would otherwise route to the incremental
// fold at this key count. ContainsNone is covered here too: it shares the union
// but must still come back a deny list, and losing the flag on this arm alone
// would invert the filter against the universe at large key counts.
func TestDocBitmapContainsBatch_ContainsAnyAccumulatorFold(t *testing.T) {
	oldGate := containsAnyAccumulatorMinKeys
	containsAnyAccumulatorMinKeys = 2
	defer func() { containsAnyAccumulatorMinKeys = oldGate }()

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
			s := &Searcher{bitmapFactory: roaringset.NewBitmapFactory(pool, func() uint64 { return 300_000 })}
			pv := &propValuePair{operator: tc.operator, containsValues: keys}
			dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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

			s := &Searcher{}
			pv := &propValuePair{
				operator:       tc.operator,
				containsValues: keysFrom(t, []byte(tc.key)),
			}
			dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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
			operator:       filters.ContainsAll,
			containsValues: keysFrom(t, []byte("a"), []byte("b"), []byte("c")),
		}

		s := &Searcher{}
		dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
		require.NoError(t, err)
		defer dbm.release()

		require.Equal(t, []uint64{2, 3}, dbm.docIDs.ToArray())
		require.Equal(t, []string{"a", "b", "c"}, fixture.reads)
	})

	t.Run("folds to empty and stops reading remaining keys", func(t *testing.T) {
		fixture := newContainsBatchFixture(t, ctx, map[string][]uint64{
			"a": {1, 2, 3},
			"b": {4, 5, 6}, // disjoint from "a" -> accumulator becomes empty here
			"c": {1, 2, 3}, // must never be read: the AND result can't change
		})

		pv := &propValuePair{
			operator:       filters.ContainsAll,
			containsValues: keysFrom(t, []byte("a"), []byte("b"), []byte("c")),
		}

		s := &Searcher{}
		dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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
			operator:       filters.ContainsAll,
			containsValues: keysFrom(t, []byte("a"), []byte("missing"), []byte("z")),
		}

		s := &Searcher{}
		dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
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
	}{
		{name: "incremental fold", gate: 256},
		{name: "accumulator fold", gate: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldGate := containsAnyAccumulatorMinKeys
			containsAnyAccumulatorMinKeys = tc.gate
			defer func() { containsAnyAccumulatorMinKeys = oldGate }()

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

			s := &Searcher{bitmapFactory: roaringset.NewBitmapFactory(fixture.pool, func() uint64 { return 300_000 })}
			pv := &propValuePair{
				operator:       filters.ContainsAny,
				containsValues: keysFrom(t, []byte("a"), []byte("b"), []byte("c")),
			}
			dbm, err := s.docBitmapContainsBatch(ctx, fixture.reader(t, pv.containsValues), pv)
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, docBitmap{}, dbm)

			require.Equal(t, []string{"a"}, fixture.reads, "the fold must stop reading once ctx is cancelled")
			require.Equal(t, 1, fixture.releaseCalls, "the row read before cancellation must be released")
			require.Zero(t, fixture.pool.Outstanding(), "no row buffer may outlive the cancelled fold")
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
