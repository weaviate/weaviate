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

package keydoccolumn

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/adapters/repos/db/roaringset"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// segFromPairs builds a segment from sorted (key, docID) pairs.
//
// Through the builder production uses rather than by hand: a segment carries
// state a reader trusts without rechecking — the largest docID it holds, above
// all, which a merge sizes its output column from — and a fixture that sets some
// of it produces something no build ever would.
func segFromPairs(keys [][]byte, docs []uint64) *segment {
	b := newSegmentBuilder()
	for i, k := range keys {
		b.append(k, docs[i])
	}
	return b.segment()
}

// mockCursor is a roaringset.InnerCursor over fixed sorted entries, for testing
// MergeMemtableByCursor without a real memtable.
type mockCursor struct {
	keys   [][]byte
	layers []roaringset.BitmapLayer
	pos    int
	// failAt is the position at which the cursor reports a read error instead of
	// a row, as a memtable cursor does when its segment cannot be read.
	failAt int
	fail   error
}

func newMockCursor() *mockCursor { return &mockCursor{pos: -1, failAt: -1} }

// failsAt makes the cursor return err once it reaches position at.
func (c *mockCursor) failsAt(at int, err error) *mockCursor {
	c.failAt, c.fail = at, err
	return c
}

func (c *mockCursor) add(key []byte, adds, dels []uint64) *mockCursor {
	var a, d *sroar.Bitmap
	if len(adds) > 0 {
		a = sroar.FromSortedList(adds)
	}
	if len(dels) > 0 {
		d = sroar.FromSortedList(dels)
	}
	c.keys = append(c.keys, key)
	c.layers = append(c.layers, roaringset.BitmapLayer{Additions: a, Deletions: d})
	return c
}

func (c *mockCursor) cur() ([]byte, roaringset.BitmapLayer, error) {
	if c.fail != nil && c.pos >= c.failAt {
		return nil, roaringset.BitmapLayer{}, c.fail
	}
	if c.pos < 0 || c.pos >= len(c.keys) {
		return nil, roaringset.BitmapLayer{}, nil
	}
	return c.keys[c.pos], c.layers[c.pos], nil
}

func (c *mockCursor) First() ([]byte, roaringset.BitmapLayer, error) { c.pos = 0; return c.cur() }
func (c *mockCursor) Next() ([]byte, roaringset.BitmapLayer, error)  { c.pos++; return c.cur() }

func (c *mockCursor) Seek([]byte) ([]byte, roaringset.BitmapLayer, error) {
	return nil, roaringset.BitmapLayer{}, nil
}

// newTestIndex builds an index over base with a logger attached, which the
// background flattening needs.
func newTestIndex(base *segment) *Index {
	logger, _ := test.NewNullLogger()
	// a bound comfortably inside what a narrow slot holds, so resolutions here
	// take the narrow path; the wide one has its own tests
	idx := &Index{
		logger:     logger,
		docIDCount: func() uint64 { return 1_000_000 },
		// the real schedule spans minutes; a test waiting one out should not
		retryDelays: []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
	}
	idx.state.Store(&indexState{base: base})
	return idx
}

// waitForFlatten blocks until no background flattening is in flight. It says
// nothing about one having run: the flag is taken inside the goroutine, so a
// caller that needs the result waits on the result.
func waitForFlatten(t *testing.T, idx *Index) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !idx.flattening.Load()
	}, 5*time.Second, time.Millisecond, "background flattening must finish")
}

func resolveSorted(tb testing.TB, idx *Index, keys ...string) []uint64 {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	arr := sroar.FromSortedList(idx.resolve(testKeys(tb, sorted...)).sortedDocs()).ToArray()
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr
}

// keyLayer is what one key carried in one flush: the documents added under it,
// the documents retired from it, or both.
type keyLayer struct {
	key  string
	adds []uint64
	dels []uint64
}

// expectation is a query and the documents it must answer with, once the step
// carrying it has landed.
type expectation struct {
	keys []string
	want []uint64
	why  string
}

// layerStep is one thing that happens to an index — a flush, or a flattening of
// what has accumulated — and what the index must answer afterwards. Steps run in
// order against one index, so a row states a history rather than a single state,
// which is what these paths get wrong: a deletion that a later re-add undoes, a
// document that survives in the extras after the slot is retired.
type layerStep struct {
	flush   []keyLayer
	flatten bool
	expect  []expectation
}

// TestLayersOverFlushes covers what an index answers as flushes land on it: keys
// added, documents retired, a key holding more than one document, and a
// flattening in the middle of it all.
//
// Every row is (base + a sequence of flushes) → what a query resolves to, which
// is the whole contract these layers exist for. Written as one table because the
// interesting part is the sequence, and a row states one.
func TestLayersOverFlushes(t *testing.T) {
	tests := []struct {
		name  string
		base  []keyLayer // only the key and its single document are read
		steps []layerStep
	}{
		{
			name: "a flush adds a key and retires another key's document",
			base: []keyLayer{{key: "a", adds: []uint64{1}}, {key: "b", adds: []uint64{2}}, {key: "c", adds: []uint64{3}}},
			steps: []layerStep{
				{
					expect: []expectation{{keys: []string{"a", "b", "c"}, want: []uint64{1, 2, 3}, why: "base only"}},
				},
				{
					// the deletion of b's document is issued under key b, in a
					// flush of its own, as a memtable would carry it
					flush: []keyLayer{{key: "d", adds: []uint64{4}}},
				},
				{
					flush: []keyLayer{{key: "b", dels: []uint64{2}}},
					expect: []expectation{
						{keys: []string{"a", "b", "c", "d"}, want: []uint64{1, 3, 4}, why: "b's document retired, d added"},
						{keys: []string{"b"}, want: []uint64{}, why: "b answers nothing once its only document is retired"},
					},
				},
				{
					// newest wins: the re-add is later than the deletion above
					flush: []keyLayer{{key: "b", adds: []uint64{5}}},
					expect: []expectation{
						{keys: []string{"b"}, want: []uint64{5}, why: "a re-add beats the earlier deletion"},
						{keys: []string{"a", "b", "c", "d"}, want: []uint64{1, 3, 4, 5}},
					},
				},
			},
		},
		{
			name: "one flush retires a document and adds its replacement",
			base: []keyLayer{{key: "x", adds: []uint64{10}}},
			steps: []layerStep{{
				flush:  []keyLayer{{key: "x", adds: []uint64{20}, dels: []uint64{10}}},
				expect: []expectation{{keys: []string{"x"}, want: []uint64{20}, why: "an update is a deletion and an addition in one flush"}},
			}},
		},
		{
			name: "a flush carrying only deletions empties the key",
			base: []keyLayer{{key: "k", adds: []uint64{7}}},
			steps: []layerStep{{
				flush:  []keyLayer{{key: "k", dels: []uint64{7}}},
				expect: []expectation{{keys: []string{"k"}, want: []uint64{}}},
			}},
		},
		{
			// the property was configured on the understanding that its values
			// are unique; a violation must cost the operator a warning, not a
			// document
			name: "a key arriving with several documents keeps all of them",
			base: []keyLayer{{key: "a", adds: []uint64{1}}},
			steps: []layerStep{
				{
					flush: []keyLayer{{key: "b", adds: []uint64{5, 9}}},
					expect: []expectation{
						{keys: []string{"b"}, want: []uint64{5, 9}, why: "both documents under the key resolve"},
						{keys: []string{"a", "b"}, want: []uint64{1, 5, 9}},
					},
				},
				{
					flatten: true,
					expect:  []expectation{{keys: []string{"a", "b"}, want: []uint64{1, 5, 9}, why: "flattening must not lose the extra document"}},
				},
				{
					flush:  []keyLayer{{key: "b", dels: []uint64{5}}},
					expect: []expectation{{keys: []string{"b"}, want: []uint64{9}, why: "a deletion retires one document, not the key"}},
				},
			},
		},
		{
			// the two ways the working set holds a document — the slot and the
			// extras list — against each other
			name: "the slot and the extras are retired independently",
			base: []keyLayer{{key: "a", adds: []uint64{1}}, {key: "b", adds: []uint64{2}}},
			steps: []layerStep{
				{
					flush: []keyLayer{{key: "b", adds: []uint64{7, 9}}},
					expect: []expectation{
						{keys: []string{"b"}, want: []uint64{2, 7, 9}, why: "the key now holds three"},
						{keys: []string{"a", "b"}, want: []uint64{1, 2, 7, 9}},
					},
				},
				{
					flush:  []keyLayer{{key: "b", dels: []uint64{2}}},
					expect: []expectation{{keys: []string{"b"}, want: []uint64{7, 9}, why: "retiring the slot leaves the extras"}},
				},
				{
					flush:  []keyLayer{{key: "b", dels: []uint64{7}}},
					expect: []expectation{{keys: []string{"b"}, want: []uint64{9}, why: "retiring an extra leaves the other"}},
				},
			},
		},
		{
			// zero is a document like any other; nothing about it may read as an
			// empty slot
			name: "document zero is ordinary",
			base: []keyLayer{{key: "z", adds: []uint64{0}}},
			steps: []layerStep{{
				expect: []expectation{{keys: []string{"z"}, want: []uint64{0}}},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			keys := make([][]byte, 0, len(tt.base))
			docs := make([]uint64, 0, len(tt.base))
			for _, b := range tt.base {
				keys = append(keys, []byte(b.key))
				docs = append(docs, b.adds[0])
			}
			idx := newTestIndex(segFromPairs(keys, docs))

			for i, step := range tt.steps {
				if step.flatten {
					idx.flattenIntoBase()
				} else if len(step.flush) > 0 {
					cursor := newMockCursor()
					for _, k := range step.flush {
						cursor.add([]byte(k.key), k.adds, k.dels)
					}
					require.NoError(t, idx.MergeMemtableByCursor(cursor))
				}
				for _, e := range step.expect {
					assert.Equal(t, e.want, resolveSorted(t, idx, e.keys...),
						"step %d: %s", i, e.why)
				}
			}
		})
	}
}

func TestFlattenIntoBase(t *testing.T) {
	// base: a→1, b→2, c→3
	idx := newTestIndex(segFromPairs(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")}, []uint64{1, 2, 3}))

	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("d"), []uint64{4}, nil))) // add d
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), nil, []uint64{2}))) // delete b
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("e"), []uint64{5}, nil))) // add e

	// flushes that change nothing, until the layer count reaches the threshold
	// and the next merge starts a flattening
	for i := len(idx.state.Load().layers); i < flattenLayersThreshold; i++ {
		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
			add(fmt.Appendf(nil, "filler%02d", i), []uint64{uint64(100 + i)}, nil)))
	}

	require.Eventually(t, func() bool {
		return len(idx.state.Load().layers) == 0
	}, 5*time.Second, time.Millisecond,
		"layers must be flattened into the base at the threshold")

	state := idx.state.Load()
	require.Equal(t, 4+flattenLayersThreshold-3, state.base.keys.len(),
		"base holds the net state (a,c,d,e and the fillers; b deleted)")

	require.Equal(t, []uint64{1, 3, 4, 5}, resolveSorted(t, idx, "a", "b", "c", "d", "e"),
		"net state after flattening: b deleted, d/e added")
	require.Equal(t, []uint64{}, resolveSorted(t, idx, "b"))

	// keeps working after the flattening: re-add b over the flattened base
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), []uint64{6}, nil)))
	require.Equal(t, []uint64{6}, resolveSorted(t, idx, "b"))
	require.Equal(t, []uint64{1, 3, 4, 5, 6}, resolveSorted(t, idx, "a", "b", "c", "d", "e"))
}

// TestResolveIsIdempotentAcrossAFlush pins what lets a flush be merged into the
// index before the swap that hides its memtable rather than during it.
//
// For that instant the same documents are in both, and a resolution applies the
// unflushed layers over the index — so every one of them is applied twice.
// Answering the same either way is what makes the ordering a choice: merging
// after the swap would leave an instant with the documents in neither, which no
// amount of idempotence recovers from.
func TestResolveIsIdempotentAcrossAFlush(t *testing.T) {
	base := segFromPairs([][]byte{[]byte("a"), []byte("b")}, []uint64{1, 2})
	idx := newTestIndex(base)

	// the flush: a's document is retired, b gains one
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("a"), nil, []uint64{1}).
		add([]byte("b"), []uint64{7}, nil)))

	once := resolveSorted(t, idx, "a", "b")
	require.Equal(t, []uint64{2, 7}, once, "the flush alone")

	// ... and the same memtable, still visible until the swap, layered on top
	res := idx.resolve(testKeys(t, "a", "b"))
	res.ApplyMemtableWindow(0, [][]roaringset.BitmapLayer{{
		{Additions: bitmapOf(), Deletions: bitmapOf(1)},
		{Additions: bitmapOf(7), Deletions: bitmapOf()},
	}})

	// The working set carries the duplicate — a document reached twice is listed
	// twice, as it is when two query keys reach it — and the bitmap the query is
	// built into collapses it, which is the contract SortedDocs states.
	assert.Equal(t, []uint64{2, 7, 7}, res.sortedDocs(), "held twice")
	assert.Equal(t, once, sroar.FromSortedList(res.sortedDocs()).ToArray(),
		"applied twice, answered once")
}

// TestFlattenIntoBaseOwnsItsSingleFlightFlag pins that the guard cannot be
// cleared by a caller that never took it. Acquiring in the flush path and
// releasing here would mean any other call — a test, a future scheduler — frees
// a flag a running flattening still holds, and the two then each drop the layers
// the other consumed. [TestFlattenDoesNotDropConcurrentFlushes] covers what that
// costs when the timing lines up; this covers the structure that allows it.
func TestFlattenIntoBaseOwnsItsSingleFlightFlag(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))
	require.Len(t, idx.state.Load().layers, 1, "a layer to consume")

	idx.flattening.Store(true) // as if a flattening were already in flight
	idx.flattenIntoBase()

	assert.True(t, idx.flattening.Load(),
		"a call that did not acquire the flag must not release it")
	assert.Len(t, idx.state.Load().layers, 1,
		"and must not consume the layers the running flattening will")
}

// TestFlattenDoesNotDropConcurrentFlushes pins the single-flight guard: flattenings
// run in the background while flushes keep arriving, and each flattening drops exactly
// the layers it consumed. Without the guard two overlapping flattenings each drop the
// same count, discarding whatever arrived in between.
func TestFlattenDoesNotDropConcurrentFlushes(t *testing.T) {
	const numFlushes = 200
	idx := newTestIndex(segFromPairs([][]byte{[]byte("base")}, []uint64{1}))

	keys := make([]string, 0, numFlushes+1)
	want := make([]uint64, 0, numFlushes+1)
	keys = append(keys, "base")
	want = append(want, 1)
	for i := 0; i < numFlushes; i++ {
		key := fmt.Sprintf("key_%04d", i)
		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
			add([]byte(key), []uint64{uint64(100 + i)}, nil)))
		keys = append(keys, key)
		want = append(want, uint64(100+i))
	}

	waitForFlatten(t, idx)
	require.Equal(t, want, resolveSorted(t, idx, keys...),
		"every flushed key must survive the flattenings that ran alongside them")
}

// TestConcurrentReadsDuringFlatten resolves from many goroutines while flushes
// are merged in and flattenings publish new bases underneath them. Readers work
// from a state they loaded once, so a state swapped mid-resolve must not change
// what they see: a key present in the base stays resolvable throughout, and a
// key merged in before a resolve started never disappears. Run with -race, this
// is what pins the lock-free publication.
func TestConcurrentReadsDuringFlatten(t *testing.T) {
	const (
		numFlushes = 300
		numReaders = 8
	)
	idx := newTestIndex(segFromPairs([][]byte{[]byte("base")}, []uint64{1}))

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		enterrors.GoWrapper(func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				require.Equal(t, []uint64{1}, resolveSorted(t, idx, "base"),
					"the base key must resolve across every published state")
			}
		}, idx.logger)
	}

	keys := []string{"base"}
	want := []uint64{1}
	for i := 0; i < numFlushes; i++ {
		key := fmt.Sprintf("key_%04d", i)
		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
			add([]byte(key), []uint64{uint64(100 + i)}, nil)))
		keys = append(keys, key)
		want = append(want, uint64(100+i))
	}
	close(stop)
	wg.Wait()

	waitForFlatten(t, idx)
	require.Equal(t, want, resolveSorted(t, idx, keys...),
		"every flushed key must survive flattenings that ran under concurrent readers")
}

// TestMergeMemtableByCursorKeepsDeletionsKeyed pins that a flush records which
// key each deleted docID belonged to, not just the set of docIDs — resolution
// and a flattening both apply a deletion to that key alone.
func TestMergeMemtableByCursorKeepsDeletionsKeyed(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))

	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("a"), nil, []uint64{3, 7}). // one key retiring two docIDs
		add([]byte("b"), nil, []uint64{5}).
		add([]byte("c"), []uint64{9}, nil)))

	layers := idx.state.Load().layers
	require.Len(t, layers, 1)
	r := layers[0]

	gotDels := map[string][]uint64{}
	for i := 0; i < r.dels.keys.len(); i++ {
		k := string(r.dels.keys.appendKey(i, nil))
		gotDels[k] = segmentDocs(r.dels, i)
	}
	require.Equal(t, map[string][]uint64{"a": {3, 7}, "b": {5}}, gotDels,
		"every deletion keeps the key it was issued under")

	require.Equal(t, 1, r.adds.keys.len(), "only c was added")
	require.Equal(t, "c", string(r.adds.keys.appendKey(0, nil)))
	require.Equal(t, uint64(9), r.adds.docs.at(0))
}

// countingCursor yields n ascending keys that all hold the same document,
// sharing one bitmap so a test can drive a long build without allocating per
// key. The key buffer is reused, which the builder tolerates because it copies
// every key into its blob.
type countingCursor struct {
	n, pos int
	docs   *sroar.Bitmap
	key    []byte
	pad    []byte
}

func newCountingCursor(n int) *countingCursor {
	return &countingCursor{n: n, docs: sroar.FromSortedList([]uint64{1})}
}

// newWideCountingCursor is [newCountingCursor] with each key padded out to
// width, which is how a build reaches a byte checkpoint on few keys.
func newWideCountingCursor(n, width int) *countingCursor {
	c := newCountingCursor(n)
	c.pad = make([]byte, max(0, width-9))
	return c
}

func (c *countingCursor) at() ([]byte, *sroar.Bitmap) {
	if c.pos >= c.n {
		return nil, nil
	}
	// the counter leads, so padding cannot disturb the order
	c.key = append(fmt.Appendf(c.key[:0], "k%08d", c.pos), c.pad...)
	return c.key, c.docs
}

func (c *countingCursor) First() ([]byte, *sroar.Bitmap) { c.pos = 0; return c.at() }
func (c *countingCursor) Next() ([]byte, *sroar.Bitmap)  { c.pos++; return c.at() }

// TestBuildStopsUnderMemoryPressure pins a build giving up rather than finishing
// into an out-of-memory kill.
//
// It asks as it goes rather than estimating up front, because what it could
// estimate is not what runs the process out of memory: the build keeps a few
// dozen bytes per key and allocates far more than that in passing, and several
// buckets open at once at startup. What it asks is whether the process has room
// now, and the answer already accounts for everything the build has churned.
func TestBuildStopsUnderMemoryPressure(t *testing.T) {
	logger, _ := test.NewNullLogger()
	opts := BuildOptions{
		DocIDCount: func() uint64 { return 1_000 },
		Logger:     logger,
	}

	t.Run("no checker builds regardless", func(t *testing.T) {
		idx, err := BuildFromCursor(newCountingCursor(checkAllocEvery+1), opts)
		require.NoError(t, err)
		require.Equal(t, checkAllocEvery+1, idx.Info().Keys)
	})

	t.Run("a short build is weighed once, at the end", func(t *testing.T) {
		asked := 0
		short := opts
		short.CheckAlloc = func(int64) error { asked++; return nil }

		idx, err := BuildFromCursor(newCountingCursor(checkAllocEvery-1), short)
		require.NoError(t, err)
		require.Equal(t, checkAllocEvery-1, idx.Info().Keys)
		require.Equal(t, 1, asked,
			"a corpus too small to reach a checkpoint is still weighed before it is kept")
	})

	t.Run("a short build can be refused", func(t *testing.T) {
		short := opts
		short.CheckAlloc = func(int64) error { return errors.New("no memory") }

		_, err := BuildFromCursor(newCountingCursor(1), short)
		require.ErrorContains(t, err, "no memory")
	})

	t.Run("wide keys reach a checkpoint on bytes", func(t *testing.T) {
		asked := 0
		wide := opts
		wide.CheckAlloc = func(int64) error { asked++; return nil }

		// far short of the row checkpoint, past the byte one
		_, err := BuildFromCursor(newWideCountingCursor(5_000, 8<<10), wide)
		require.NoError(t, err)
		require.Greater(t, asked, 1,
			"key width, not only key count, has to reach a checkpoint")
	})

	t.Run("a refusal stops the build", func(t *testing.T) {
		var held int64
		pressured := opts
		pressured.CheckAlloc = func(bytes int64) error { held = bytes; return errors.New("no memory") }

		_, err := BuildFromCursor(newCountingCursor(checkAllocEvery+1), pressured)
		require.ErrorContains(t, err, "no memory")
		require.ErrorContains(t, err, "building over")
		assert.Positive(t, held, "the check is told what the build is holding")
	})
}

// TestFlatteningAsksForWhatItWillAllocate pins the figure the memory check is
// given against the figure the merge then allocates. The base alone understates
// it by every layer being merged in, and those layers are the whole reason a
// flattening runs at all.
func TestFlatteningAsksForWhatItWillAllocate(t *testing.T) {
	var keys [][]byte
	var docs []uint64
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Appendf(nil, "k%03d", i))
		docs = append(docs, uint64(i+1))
	}
	idx := newTestIndex(segFromPairs(keys, docs))

	for round := 0; round < 4; round++ {
		cursor := newMockCursor()
		for i := 0; i < 50; i++ {
			cursor.add(fmt.Appendf(nil, "new%03d%03d", round, i),
				[]uint64{uint64(10_000 + round*50 + i)}, nil)
		}
		require.NoError(t, idx.MergeMemtableByCursor(cursor))
	}

	var asked int64
	idx.checkAlloc = func(bytes int64) error { asked = bytes; return nil }

	state := idx.state.Load()
	want := planFlatten(state.base, state.layers).sizeBytes()
	require.Greater(t, want, int64(state.base.sizeBytes()),
		"the layers have to cost something, or this proves nothing")

	idx.flattenIntoBase()
	require.Equal(t, want, asked)
}

// TestFlatteningDoesNotInflateKeyBytes pins a flattening's key-byte estimate to
// the keys it holds, not to the backing arrays' spare capacity.
func TestFlatteningDoesNotInflateKeyBytes(t *testing.T) {
	// mixed widths, so the backing is the one that has to estimate
	var keys [][]byte
	var docs []uint64
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Appendf(nil, "k%03d%s", i, strings.Repeat("x", i%7)))
		docs = append(docs, uint64(i+1))
	}
	idx := newTestIndex(segFromPairs(keys, docs))
	require.Equal(t, -1, idx.state.Load().base.keys.info().width)

	first := keyBytesOf(idx.state.Load().base.keys)
	for round := 1; round <= 10; round++ {
		cursor := newMockCursor()
		// an existing key taking one more document, so the key set is untouched
		cursor.add(keys[0], []uint64{uint64(1_000 + round)}, nil)
		require.NoError(t, idx.MergeMemtableByCursor(cursor))
		idx.flattenIntoBase()

		require.Equal(t, first, keyBytesOf(idx.state.Load().base.keys),
			"round %d: the same keys must cost the same estimate", round)
	}
}

// TestInfoCountsEverythingHeld pins the reported footprint against what the
// index actually holds. The base alone leaves out the two parts that grow
// without a flattening — the layers, and the documents beyond the first under a
// key — which is what a node is sized from.
func TestInfoCountsEverythingHeld(t *testing.T) {
	var keys [][]byte
	var docs []uint64
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Appendf(nil, "k%03d", i))
		docs = append(docs, uint64(i+1))
	}
	idx := newTestIndex(segFromPairs(keys, docs))

	base := idx.Info()
	require.Positive(t, base.BaseBytes)
	require.Zero(t, base.LayersBytes, "nothing has been flushed yet")
	require.Zero(t, base.OverflowBytes, "every value is unique")
	require.Equal(t, base.BaseBytes, base.SizeBytes)

	// a flush of new keys, and one key taking a second document
	cursor := newMockCursor()
	for i := 0; i < 50; i++ {
		cursor.add(fmt.Appendf(nil, "new%03d", i), []uint64{uint64(1_000 + i)}, nil)
	}
	require.NoError(t, idx.MergeMemtableByCursor(cursor))

	withLayer := idx.Info()
	require.Positive(t, withLayer.LayersBytes, "the flush is held as a layer")
	require.Equal(t, base.BaseBytes, withLayer.BaseBytes, "the base did not move")
	require.Equal(t,
		withLayer.BaseBytes+withLayer.OverflowBytes+withLayer.LayersBytes,
		withLayer.SizeBytes)
	require.Greater(t, withLayer.SizeBytes, base.SizeBytes,
		"a flush the index is holding has to show up in what it costs")

	// a key holding several documents costs more than its slot, in the base it
	// was built into and in a layer that brings more
	b := newSegmentBuilder()
	b.append([]byte("shared"), 1)
	b.appendExtra(2)
	b.appendExtra(3)
	overflowing := newTestIndex(b.segment())

	inBase := overflowing.Info().OverflowBytes
	require.Positive(t, inBase, "the base's own overflow has to be counted")

	require.NoError(t, overflowing.MergeMemtableByCursor(
		newMockCursor().add([]byte("shared"), []uint64{4, 5, 6}, nil)))
	require.Greater(t, overflowing.Info().OverflowBytes, inBase,
		"and so does a layer's")
}

// TestFlatteningReportsItsNewSize pins the owner hearing about a flattening.
func TestFlatteningReportsItsNewSize(t *testing.T) {
	resized := 0
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	idx.onResize = func() { resized++ }

	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))
	require.Zero(t, resized, "a flush reports on its own path, not through this")

	idx.flattenIntoBase()
	require.Equal(t, 1, resized, "publishing a new base reports what it now holds")
}

// TestOffsetsStillFit pins where a segment stops being able to address what it
// holds. Both an offset and an overflow position are uint32, and past either
// ceiling the segment does not fail — offsets stop ascending, and a key read
// through one comes back as another key's bytes or as a reversed slice.
//
// The boundary is checked here rather than through a builder because the
// smallest input that reaches it is four gibibytes of keys.
func TestOffsetsStillFit(t *testing.T) {
	const ceiling = uint64(math.MaxUint32)

	tests := []struct {
		name     string
		keyBytes uint64
		rows     uint64
		wantErr  string
	}{
		{name: "empty"},
		{name: "at the ceiling", keyBytes: ceiling, rows: ceiling},
		{name: "one key byte past it", keyBytes: ceiling + 1, rows: 1, wantErr: "bytes of keys"},
		{name: "one row past it", keyBytes: 1, rows: ceiling + 1, wantErr: "keys, above"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := offsetsStillFit(tt.keyBytes, tt.rows)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestBuildRefusesWithoutWhatItNeeds pins a build declining rather than panicking
// on options it cannot work without. Both are required, and both would otherwise
// fail some way into the pass — the docID count on the first key, the logger on
// the first property whose values turn out not to be unique.
func TestBuildRefusesWithoutWhatItNeeds(t *testing.T) {
	logger, _ := test.NewNullLogger()
	docIDCount := func() uint64 { return 1_000 }

	tests := []struct {
		name    string
		opts    BuildOptions
		wantErr string
	}{
		{name: "no docID count", opts: BuildOptions{Logger: logger}, wantErr: "docID count"},
		{name: "no logger", opts: BuildOptions{DocIDCount: docIDCount}, wantErr: "logger"},
		{name: "both", opts: BuildOptions{DocIDCount: docIDCount, Logger: logger}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildFromCursor(newCountingCursor(1), tt.opts)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// TestCloseStopsFlattening pins that an index on its way out stops working on
// its own account: a merge in progress gives up rather than finishing a base for
// an owner that has stopped pointing at it.
//
// What closing must not do is change an answer. Every state published is
// immutable, so a reader holding one keeps resolving from it, and the layers a
// stopped flattening did not consume are still there to be read.
func TestCloseStopsFlattening(t *testing.T) {
	t.Run("the merge gives up and builds nothing", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		require.NoError(t, idx.MergeMemtableByCursor(
			newMockCursor().add([]byte("b"), []uint64{2}, nil)))
		state := idx.state.Load()

		assert.Nil(t, flatten(state.base, state.layers, func() bool { return true }),
			"a merge told to stop hands back nothing to publish")
		assert.NotNil(t, flatten(state.base, state.layers, func() bool { return false }),
			"and finishes when it is not")
	})

	t.Run("a closed index publishes nothing", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		require.NoError(t, idx.MergeMemtableByCursor(
			newMockCursor().add([]byte("b"), []uint64{2}, nil)))

		idx.Close()
		idx.flattenIntoBase()

		assert.Len(t, idx.state.Load().layers, 1, "nothing was consumed")
		assert.Equal(t, []uint64{1, 2}, resolveSorted(t, idx, "a", "b"),
			"and everything still resolves")
	})

	t.Run("closing does not stop a flush being absorbed", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		idx.Close()

		require.NoError(t, idx.MergeMemtableByCursor(
			newMockCursor().add([]byte("b"), []uint64{2}, nil)))
		assert.Equal(t, []uint64{2}, resolveSorted(t, idx, "b"))
	})
}

// TestFlattenDefersUnderMemoryPressure pins a flattening putting itself off
// rather than building a second base beside the one already held. The layers
// stay where they are, so the next flush tries again, and reads are unaffected
// meanwhile — they cost more layers to walk, not wrong answers.
func TestFlattenDefersUnderMemoryPressure(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))

	var asked int64
	attempts := 0
	idx.checkAlloc = func(bytes int64) error {
		asked, attempts = bytes, attempts+1
		return errors.New("no memory")
	}
	idx.flattenIntoBase()

	assert.Positive(t, asked, "the check is told what the merge is about to allocate")
	assert.Len(t, idx.state.Load().layers, 1, "the layers stay for the next attempt")
	assert.Equal(t, []uint64{2}, resolveSorted(t, idx, "b"), "and are still answered")
	assert.Equal(t, len(idx.retryDelays), attempts,
		"a deferral is retried on the whole schedule before being left to the next flush")
}

// TestFlattenRetriesUntilTheNodeHasRoom pins a rebuild that was deferred
// finishing on its own, without waiting for another flush.
func TestFlattenRetriesUntilTheNodeHasRoom(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))

	refusals := 0
	idx.checkAlloc = func(int64) error {
		if refusals++; refusals < 3 {
			return errors.New("no memory")
		}
		return nil
	}

	idx.flattenIntoBase()

	assert.Equal(t, 3, refusals, "it kept asking")
	assert.Empty(t, idx.state.Load().layers, "and folded the layers in once it could")
	assert.Equal(t, []uint64{1, 2}, resolveSorted(t, idx, "a", "b"))
}

// TestFlattenDefersWithoutARebuildSlot pins the node-wide bound on how many
// columns rebuild at once.
func TestFlattenDefersWithoutARebuildSlot(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))

	asked, released := 0, 0
	free := false
	idx.acquireSlot = func() (func(), bool) {
		asked++
		if !free {
			free = true // the slot the next attempt finds
			return nil, false
		}
		return func() { released++ }, true
	}

	idx.flattenIntoBase()

	assert.Equal(t, 2, asked, "refused once, taken on the retry")
	assert.Equal(t, 1, released, "and given back")
	assert.Empty(t, idx.state.Load().layers)
}

// TestFlattenWarnsOnceItHasCompounded pins what an operator is told, at a level
// the debug line it used to emit did not reach.
func TestFlattenWarnsOnceItHasCompounded(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)

	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	idx.logger = logger
	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))
	idx.checkAlloc = func(int64) error { return errors.New("no memory") }

	idx.flattenIntoBase()

	var warned *logrus.Entry
	for _, e := range hook.AllEntries() {
		if e.Level == logrus.WarnLevel {
			warned = e
		}
	}
	require.NotNil(t, warned, "a column that stopped being rebuilt is operator news")
	assert.Contains(t, warned.Message, "every query walks them")
	assert.Contains(t, warned.Message, "no memory", "and says what refused it")
}

// TestFlattenReportsABaseNoLongerWorthHolding pins the bound being re-applied to
// what a flattening produces.
//
// Every flush is measured on its own, and one holding fewer duplicated keys than
// the floor is allowed however small it is — so a base can drift, one tolerated
// flush at a time, past a total the build would have refused outright. Each of
// the three flushes below passes; together they do not.
func TestFlattenReportsABaseNoLongerWorthHolding(t *testing.T) {
	// enough keys that the bound is the floor rather than the ratio, and two
	// flushes that each stay under that floor while their sum does not
	const (
		baseKeys       = 3_000
		sharedPerFlush = overflowFloor/2 + 1 // so that two of them clear the floor
	)

	keys := make([][]byte, baseKeys)
	docs := make([]uint64, baseKeys)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "k%06d", i)
		docs[i] = uint64(i + 1)
	}
	idx := newTestIndex(segFromPairs(keys, docs))

	unfit := 0
	idx.onUnfit = func() { unfit++ }

	for f := range 2 {
		cursor := newMockCursor()
		for i := f * sharedPerFlush; i < (f+1)*sharedPerFlush; i++ {
			cursor.add(keys[i], []uint64{uint64(baseKeys + i + 1)}, nil)
		}
		require.NoError(t, idx.MergeMemtableByCursor(cursor),
			"a flush holding fewer such keys than the floor is allowed on its own")
	}
	require.Len(t, idx.state.Load().layers, 2, "every flush was accepted")

	idx.flattenIntoBase()

	assert.Equal(t, 1, unfit, "the flattened base is past the bound and says so")
	assert.Len(t, idx.state.Load().layers, 2,
		"and is not published: the index is about to be dropped")
	assert.Equal(t, []uint64{1, baseKeys + 1}, resolveSorted(t, idx, "k000000"),
		"until it is, it still answers")
}

// TestOverflowLimit covers the bound itself, which is arithmetic and needs no
// data: how many keys of a segment may hold more than one document before the
// property is judged the wrong shape for this structure.
//
// The counts it takes are real, so the tests that drive a producer past it have
// to be real too — the bound is not lowered to make them cheap. Those are one
// apiece, below and in the flush path.
func TestOverflowLimit(t *testing.T) {
	tests := []struct {
		name string
		rows int
		want int
	}{
		{name: "empty", rows: 0, want: overflowFloor},
		{name: "small corpora get the floor", rows: 1_000, want: overflowFloor},
		{name: "at the crossover", rows: overflowFloor * overflowRowsPerKeyLimit, want: overflowFloor},
		{
			name: "past it, one such key per rows",
			rows: overflowFloor * overflowRowsPerKeyLimit * 2,
			want: overflowFloor * 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, overflowLimit(tt.rows))
		})
	}
}

// TestOverflowBoundCountsKeysNotDocuments pins what the bound counts: keys that
// hold more than one document, not the extra documents themselves. One value
// shared by very many documents is cheap — the overflow is a map, so its cost
// follows entries — and is allowed; very many values each shared by a few is
// what this structure is wrong for.
func TestOverflowBoundCountsKeysNotDocuments(t *testing.T) {
	t.Run("one key with many documents is allowed", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		docs := make([]uint64, 0, 4*overflowFloor)
		for i := range cap(docs) {
			docs = append(docs, uint64(i+5))
		}

		require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), docs, nil)))
		require.Equal(t, docs, resolveSorted(t, idx, "b"))
	})

	t.Run("many keys each with a duplicate is refused", func(t *testing.T) {
		idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		cursor := newMockCursor()
		for i := range overflowFloor + 1 {
			cursor.add(fmt.Appendf(nil, "k%06d", i), []uint64{uint64(2 * i), uint64(2*i + 1)}, nil)
		}

		err := idx.MergeMemtableByCursor(cursor)
		require.Error(t, err, "past the bound the structure is the wrong one for this data")
		require.Contains(t, err.Error(), "unique values")
	})
}

// TestReAddedDocumentIsRetiredOnce pins the same through the layers: a flush
// that re-adds a document its key already holds must not make that document
// survive the flush that later retires it. Re-adding happens whenever a write
// repeats a value the index already records for that document — an ordinary
// object update rewrites every inverted entry, not only the changed ones.
func TestReAddedDocumentIsRetiredOnce(t *testing.T) {
	type flush struct{ adds, dels []uint64 }
	tests := []struct {
		name    string
		flushes []flush
		want    []uint64
	}{
		{
			// the key holds 9 in its extras throughout
			name: "re-added while the key still holds it",
			flushes: []flush{
				{adds: []uint64{7, 9}},
				{adds: []uint64{9}},
				{dels: []uint64{9}},
			},
			want: []uint64{2, 7},
		},
		{
			// 7 moves from the extras into the slot the base document vacated,
			// leaving the key holding it in both places at once
			name: "re-added after the slot it vacated",
			flushes: []flush{
				{adds: []uint64{7}},
				{dels: []uint64{2}},
				{adds: []uint64{7}},
				{dels: []uint64{7}},
			},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := segFromPairs([][]byte{[]byte("a"), []byte("b")}, []uint64{1, 2})
			idx := newTestIndex(base)
			for _, f := range tt.flushes {
				require.NoError(t, idx.MergeMemtableByCursor(
					newMockCursor().add([]byte("b"), f.adds, f.dels)))
			}
			assert.ElementsMatch(t, tt.want, resolveSorted(t, idx, "b"))
		})
	}
}

// pairsCursor is a [MergedCursor] over keys given with their bitmaps, including
// the empty and over-range ones a real merge can hand over.
type pairsCursor struct {
	keys [][]byte
	bms  []*sroar.Bitmap
	pos  int
}

func (c *pairsCursor) at() ([]byte, *sroar.Bitmap) {
	if c.pos >= len(c.keys) {
		return nil, nil
	}
	return c.keys[c.pos], c.bms[c.pos]
}

func (c *pairsCursor) First() ([]byte, *sroar.Bitmap) { c.pos = 0; return c.at() }
func (c *pairsCursor) Next() ([]byte, *sroar.Bitmap)  { c.pos++; return c.at() }

// TestBuildSkipsEmptyKeysAndRefusesOutOfRangeDocs covers the two things a build
// can meet in its cursor that are not a row to append.
//
// An empty bitmap is a key every document has left — ordinary, and nothing to
// store. A docID at or above the count the shard reports cannot be stored at the
// width the column was sized to, so it is refused rather than truncated into a
// slot that would answer as a different document.
func TestBuildSkipsEmptyKeysAndRefusesOutOfRangeDocs(t *testing.T) {
	logger, _ := test.NewNullLogger()
	opts := BuildOptions{DocIDCount: func() uint64 { return 100 }, Logger: logger}

	t.Run("a key whose documents are all gone is not a row", func(t *testing.T) {
		idx, err := BuildFromCursor(&pairsCursor{
			keys: [][]byte{[]byte("a"), []byte("b"), []byte("c")},
			bms: []*sroar.Bitmap{
				sroar.FromSortedList([]uint64{1}),
				sroar.NewBitmap(),
				sroar.FromSortedList([]uint64{3}),
			},
		}, opts)
		require.NoError(t, err)
		require.Equal(t, 2, idx.Info().Keys, "the empty key is skipped, not stored empty")
		require.Equal(t, []uint64{1, 3}, resolveSorted(t, idx, "a", "b", "c"))
	})

	t.Run("a document the shard has not issued is refused", func(t *testing.T) {
		_, err := BuildFromCursor(&pairsCursor{
			keys: [][]byte{[]byte("a")},
			bms:  []*sroar.Bitmap{sroar.FromSortedList([]uint64{100})},
		}, opts)
		require.ErrorContains(t, err, "outside the 100 docIDs issued")
	})
}

// TestMergeStopsOnACursorError pins a flush that cannot be read taking the index
// with it rather than being merged in part. A layer holding some of a flush and
// none of the rest would answer as if the missing documents had never been
// written.
func TestMergeStopsOnACursorError(t *testing.T) {
	idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	unreadable := errors.New("segment cannot be read")

	err := idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("b"), []uint64{2}, nil).
		add([]byte("c"), []uint64{3}, nil).
		failsAt(1, unreadable))

	require.ErrorIs(t, err, unreadable)
	require.Empty(t, idx.state.Load().layers, "a flush read in part is not a layer")
	require.Equal(t, []uint64{1}, resolveSorted(t, idx, "a", "b", "c"))
}
