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
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
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

// runFromPairs builds a column run through newColumnRunBuilder, so it carries the max docID a merge sizes from.
func runFromPairs(keys [][]byte, docs []uint64) *columnRun {
	b := newColumnRunBuilder()
	for i, k := range keys {
		b.append(k, docs[i])
	}
	return b.columnRun()
}

// mockCursor is a roaringset.InnerCursor over fixed sorted entries, for MergeMemtableByCursor tests.
type mockCursor struct {
	keys   [][]byte
	layers []roaringset.BitmapLayer
	pos    int
	// failAt is where the cursor returns fail instead of a row, as an unreadable segment does.
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

// newTestIndex builds an index over base with the logger the background flattening needs.
func newTestIndex(base *columnRun) *Index {
	logger, _ := test.NewNullLogger()
	idx := &Index{logger: logger}
	idx.state.Store(&indexState{base: base})
	return idx
}

// testDocIDCount fits a narrow slot, so resolutions here take the narrow path.
const testDocIDCount = 1_000_000

func neverStopped() bool { return false }

// flattenRuns plans and builds a flattening of base and layers to completion.
func flattenRuns(t *testing.T, base *columnRun, layers []*layer) *columnRun {
	t.Helper()
	plan, ok := planFlatten(base, layers, neverStopped)
	require.True(t, ok)
	return flatten(base, layers, plan, neverStopped)
}

func waitForFlatten(t *testing.T, idx *Index) {
	t.Helper()
	require.Eventually(t, func() bool {
		return !idx.flattening.Load()
	}, 5*time.Second, time.Millisecond, "background flattening must finish")
}

// flattenNow runs one flattening over the layers as they stand and waits for it.
func flattenNow(t *testing.T, idx *Index) {
	t.Helper()
	waitForFlatten(t, idx)
	require.True(t, idx.startFlatten(), "a flattening must start")
	waitForFlatten(t, idx)
}

func resolveSorted(tb testing.TB, idx *Index, keys ...string) []uint64 {
	sorted := slices.Clone(keys)
	slices.Sort(sorted)
	res, err := idx.State().Resolve(context.Background(), testKeys(tb, sorted...), testDocIDCount)
	require.NoError(tb, err)
	arr := sroar.FromSortedList(heldDocs(res)).ToArray()
	sort.Slice(arr, func(i, j int) bool { return arr[i] < arr[j] })
	return arr
}

// keyLayer is what one key carried in one flush: added documents, retired documents, or both.
type keyLayer struct {
	key  string
	adds []uint64
	dels []uint64
}

// expectation is a query and the documents it must answer once its step has run.
type expectation struct {
	keys []string
	want []uint64
	why  string
}

// layerStep is a flush or a flattening and what the index must answer after it; steps run in order.
type layerStep struct {
	flush   []keyLayer
	flatten bool
	expect  []expectation
}

// TestLayersOverFlushes pins what an index resolves after each step of a flush and flattening sequence.
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
			// non-unique values must cost the operator a warning, never a document
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
			// a document in the slot and documents in the extras, retired independently
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
			// docID zero must not read as an empty slot
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
			idx := newTestIndex(runFromPairs(keys, docs))

			for i, step := range tt.steps {
				if step.flatten {
					flattenNow(t, idx)
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
	idx := newTestIndex(runFromPairs(
		[][]byte{[]byte("a"), []byte("b"), []byte("c")}, []uint64{1, 2, 3}))

	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("d"), []uint64{4}, nil))) // add d
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), nil, []uint64{2}))) // delete b
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("e"), []uint64{5}, nil))) // add e

	// filler flushes until flattenLayersThreshold, where the merge starts a flattening
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

// TestResolveIsIdempotentAcrossAFlush pins a flush applied in the index and again as a memtable layer answering once.
func TestResolveIsIdempotentAcrossAFlush(t *testing.T) {
	base := runFromPairs([][]byte{[]byte("a"), []byte("b")}, []uint64{1, 2})
	idx := newTestIndex(base)

	// the flush: a's document is retired, b gains one
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("a"), nil, []uint64{1}).
		add([]byte("b"), []uint64{7}, nil)))

	once := resolveSorted(t, idx, "a", "b")
	require.Equal(t, []uint64{2, 7}, once, "the flush alone")

	// ... and the same memtable, still visible until the swap, layered on top
	res, err := idx.State().Resolve(context.Background(), testKeys(t, "a", "b"), testDocIDCount)
	require.NoError(t, err)
	require.NoError(t, res.ApplyMemtableWindow(0, [][]roaringset.BitmapLayer{{
		{Additions: bitmapOf(), Deletions: bitmapOf(1)},
		{Additions: bitmapOf(7), Deletions: bitmapOf()},
	}}))

	assert.Equal(t, once, heldDocs(res), "applied twice, answered once")
}

// TestResolveStopsWhenCancelled pins a cancelled query giving up between layers.
func TestResolveStopsWhenCancelled(t *testing.T) {
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name  string
		layer bool
		ctx   context.Context
	}{
		{name: "before the base", ctx: cancelled},
		{name: "after the base", layer: true, ctx: &errAfter{Context: context.Background(), left: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
			if tt.layer {
				require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), []uint64{2}, nil)))
			}
			res, err := idx.State().Resolve(tt.ctx, testKeys(t, "a", "b"), testDocIDCount)
			require.ErrorIs(t, err, context.Canceled)
			assert.Nil(t, res)
		})
	}
}

// errAfter is a context whose Err reports cancellation once it has been asked
// left times.
type errAfter struct {
	context.Context
	left int
}

func (c *errAfter) Err() error {
	if c.left <= 0 {
		return context.Canceled
	}
	c.left--
	return nil
}

// A start while a flattening is in flight must neither run a second one nor free its flag.
func TestStartFlattenOwnsItsSingleFlightFlag(t *testing.T) {
	idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))
	require.Len(t, idx.state.Load().layers, 1, "a layer to consume")

	idx.flattening.Store(true) // as if a flattening were already in flight
	assert.False(t, idx.startFlatten())

	assert.True(t, idx.flattening.Load(),
		"a call that did not acquire the flag must not release it")
	assert.Len(t, idx.state.Load().layers, 1,
		"and must not consume the layers the running flattening will")
}

// TestConcurrentReadsDuringFlatten pins resolves, under -race, staying correct while flattenings publish new bases.
func TestConcurrentReadsDuringFlatten(t *testing.T) {
	const (
		numFlushes = 300
		numReaders = 8
	)
	idx := newTestIndex(runFromPairs([][]byte{[]byte("base")}, []uint64{1}))

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

// TestMergeMemtableByCursorKeepsDeletionsKeyed pins a flush recording each deleted docID under its own key.
func TestMergeMemtableByCursorKeepsDeletionsKeyed(t *testing.T) {
	idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))

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
		gotDels[k] = columnRunDocs(r.dels, i)
	}
	require.Equal(t, map[string][]uint64{"a": {3, 7}, "b": {5}}, gotDels,
		"every deletion keeps the key it was issued under")

	require.Equal(t, 1, r.adds.keys.len(), "only c was added")
	require.Equal(t, "c", string(r.adds.keys.appendKey(0, nil)))
	require.Equal(t, uint64(9), r.adds.docs.at(0))
}

// countingCursor yields n ascending keys sharing one document bitmap and one reused key buffer.
type countingCursor struct {
	n, pos int
	docs   *sroar.Bitmap
	key    []byte
	pad    []byte
}

func newCountingCursor(n int) *countingCursor {
	return &countingCursor{n: n, docs: sroar.FromSortedList([]uint64{1})}
}

// newWideCountingCursor is [newCountingCursor] with keys padded to width, to reach a byte checkpoint.
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

// TestBuildStopsUnderMemoryPressure pins CheckAlloc being asked during the build and a refusal stopping it.
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

	t.Run("the documents beyond a key's first are weighed", func(t *testing.T) {
		var asked int64
		counted := opts
		counted.CheckAlloc = func(bytes int64) error { asked = max(asked, bytes); return nil }

		_, err := BuildFromCursor(&pairsCursor{
			keys: [][]byte{[]byte("a")},
			bms:  []*sroar.Bitmap{sroar.FromSortedList(sequence(0, 1_000))},
		}, counted)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, asked, int64(999*8))
	})

	t.Run("a key's documents are weighed before they are copied out", func(t *testing.T) {
		n := uint64(checkAllocBytes/8 + 1)
		big := BuildOptions{
			DocIDCount: func() uint64 { return n },
			Logger:     logger,
			CheckAlloc: func(int64) error { return errors.New("no memory") },
		}
		_, err := BuildFromCursor(&pairsCursor{
			keys: [][]byte{[]byte("a")},
			bms:  []*sroar.Bitmap{sroar.FromSortedList(sequence(0, n))},
		}, big)
		require.ErrorContains(t, err, "building over 0 keys")
	})
}

// sequence is the documents [from, to).
func sequence(from, to uint64) []uint64 {
	out := make([]uint64, 0, to-from)
	for d := from; d < to; d++ {
		out = append(out, d)
	}
	return out
}

// TestFlatteningAsksForWhatItWillAllocate pins checkAlloc receiving the size of base plus layers, not the base alone.
func TestFlatteningAsksForWhatItWillAllocate(t *testing.T) {
	var keys [][]byte
	var docs []uint64
	for i := 0; i < 200; i++ {
		keys = append(keys, fmt.Appendf(nil, "k%03d", i))
		docs = append(docs, uint64(i+1))
	}
	idx := newTestIndex(runFromPairs(keys, docs))

	// every base key is 4 bytes wide; the layers' keys vary, so they are blob-backed
	numKeys, keyBytes := len(keys), 4*len(keys)
	for round := 0; round < 4; round++ {
		cursor := newMockCursor()
		if round == 3 {
			// an existing key gaining three documents, all in overflow beside its first,
			// the last of them the widest document
			cursor.add([]byte("k000"), []uint64{70_000, 70_001, 70_002}, nil)
		}
		for i := 0; i < 50; i++ {
			key := append(fmt.Appendf(nil, "n%d%03d", round, i), strings.Repeat("x", i%3)...)
			cursor.add(key, []uint64{uint64(10_000 + round*50 + i)}, nil)
			numKeys, keyBytes = numKeys+1, keyBytes+len(key)
		}
		require.NoError(t, idx.MergeMemtableByCursor(cursor))
	}

	var asked int64
	idx.checkAlloc = func(bytes int64) error { asked = bytes; return nil }

	const docWidth = 3 // the largest document, 70_002, needs three bytes
	want := int64(keyBytes + 4*(numKeys+1) + docWidth*numKeys + overflowEntryBytes + 3*8)

	flattenNow(t, idx)
	require.Equal(t, want, asked)
}

// TestInfoCountsEverythingHeld pins that the reported footprint counts the base, the layers and the overflow.
func TestInfoCountsEverythingHeld(t *testing.T) {
	// presized, so every array's capacity is exactly what it holds
	sized := func(keys [][]byte, docs [][]uint64, docWidth int) (run *columnRun, keyBytes int) {
		for _, k := range keys {
			keyBytes += len(k)
		}
		b := newColumnRunBuilderSized(len(keys), keyBytes, docWidth)
		for i, k := range keys {
			b.append(k, docs[i][0])
			for _, d := range docs[i][1:] {
				b.appendExtra(d)
			}
		}
		return b.columnRun(), keyBytes
	}

	// 200 four-byte keys sharing no prefix, so a fixed column; the first 10 hold two extras each
	var keys [][]byte
	var docs [][]uint64
	for i := 0; i < 200; i++ {
		keys = append(keys, []byte{byte(i), 'k', 'e', 'y'})
		d := []uint64{uint64(i)}
		if i < 10 {
			d = append(d, uint64(200+2*i), uint64(201+2*i))
		}
		docs = append(docs, d)
	}
	base, _ := sized(keys, docs, 1)

	// variable-length keys, so blob columns; the first add holds three extras
	var addKeys, delKeys [][]byte
	var addDocs, delDocs [][]uint64
	for i := 0; i < 50; i++ {
		addKeys = append(addKeys, append(fmt.Appendf(nil, "n%03d", i), strings.Repeat("x", i%3)...))
		addDocs = append(addDocs, []uint64{uint64(1_000 + i)})
	}
	addDocs[0] = append(addDocs[0], 2_000, 2_001, 2_002)
	for i := 0; i < 5; i++ {
		delKeys = append(delKeys, []byte(strings.Repeat("d", i+1)))
		delDocs = append(delDocs, []uint64{uint64(i)})
	}
	adds, addKeyBytes := sized(addKeys, addDocs, 8)
	dels, delKeyBytes := sized(delKeys, delDocs, 8)

	idx := newTestIndex(base)
	idx.state.Store(&indexState{base: base, layers: []*layer{{adds: adds, dels: dels}}})

	wantBase := 4*200 + 1*200
	wantOverflow := (10*overflowEntryBytes + 20*8) + (overflowEntryBytes + 3*8)
	wantLayers := (addKeyBytes + 4*(50+1) + 8*50) + (delKeyBytes + 4*(5+1) + 8*5)

	gotBase, gotOverflow, gotLayers := idx.state.Load().footprint()
	assert.Equal(t, wantBase, gotBase, "base")
	assert.Equal(t, wantOverflow, gotOverflow, "overflow, across base and layer")
	assert.Equal(t, wantLayers, gotLayers, "layers")
	require.Equal(t, wantBase+wantOverflow+wantLayers, idx.Info().SizeBytes)

	// a flush of new keys
	cursor := newMockCursor()
	for i := 0; i < 50; i++ {
		cursor.add(fmt.Appendf(nil, "new%03d", i), []uint64{uint64(1_000 + i)}, nil)
	}
	require.NoError(t, idx.MergeMemtableByCursor(cursor))

	withBase, _, withLayers := idx.state.Load().footprint()
	assert.Equal(t, wantBase, withBase, "the base did not move")
	assert.Greater(t, withLayers, wantLayers, "the flush is held as a layer")
	assert.Greater(t, idx.Info().SizeBytes, wantBase+wantOverflow+wantLayers,
		"a flush the index is holding has to show up in what it costs")
}

// TestFlatteningReportsItsNewSize pins the owner hearing about a flattening.
func TestFlatteningReportsItsNewSize(t *testing.T) {
	resized := 0
	idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	idx.onResize = func() { resized++ }

	require.NoError(t, idx.MergeMemtableByCursor(
		newMockCursor().add([]byte("b"), []uint64{2}, nil)))
	require.Zero(t, resized, "a flush reports on its own path, not through this")

	flattenNow(t, idx)
	require.Equal(t, 1, resized, "publishing a new base reports what it now holds")

	flattenNow(t, idx)
	require.Equal(t, 1, resized, "with no layers left there is nothing to publish")
}

// TestOffsetsStillFit pins offsetsStillFit refusing key bytes or rows past MaxUint32, which uint32 offsets cannot address.
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

// TestBuildRefusesWithoutWhatItNeeds pins BuildFromCursor erroring, not panicking, without DocIDCount or Logger.
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

// TestCloseStopsFlattening pins Close stopping flattening without changing an answer or refusing a flush.
func TestCloseStopsFlattening(t *testing.T) {
	t.Run("the merge gives up and builds nothing", func(t *testing.T) {
		idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		require.NoError(t, idx.MergeMemtableByCursor(
			newMockCursor().add([]byte("b"), []uint64{2}, nil)))
		state := idx.state.Load()

		plan, ok := planFlatten(state.base, state.layers, neverStopped)
		require.True(t, ok)
		stopAfter := func(keys int) func() bool {
			asked := 0
			return func() bool { asked++; return asked > keys }
		}

		_, ok = planFlatten(state.base, state.layers, stopAfter(1))
		assert.False(t, ok, "a count told to stop after its first key stops there")
		assert.Nil(t, flatten(state.base, state.layers, plan, stopAfter(0)),
			"a merge told to stop hands back nothing to publish")
		assert.Nil(t, flatten(state.base, state.layers, plan, stopAfter(1)),
			"told to stop after its first key, it stops there")
		assert.NotNil(t, flatten(state.base, state.layers, plan, neverStopped),
			"and finishes when it is not")
	})

	t.Run("a closed index publishes nothing", func(t *testing.T) {
		idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		require.NoError(t, idx.MergeMemtableByCursor(
			newMockCursor().add([]byte("b"), []uint64{2}, nil)))

		idx.Close()
		assert.False(t, idx.startFlatten(), "a closed index starts no flattening")

		assert.Len(t, idx.state.Load().layers, 1, "nothing was consumed")
		assert.Equal(t, []uint64{1, 2}, resolveSorted(t, idx, "a", "b"),
			"and everything still resolves")
	})

	t.Run("closing does not stop a flush being absorbed", func(t *testing.T) {
		idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
		idx.Close()

		require.NoError(t, idx.MergeMemtableByCursor(
			newMockCursor().add([]byte("b"), []uint64{2}, nil)))
		assert.Equal(t, []uint64{2}, resolveSorted(t, idx, "b"))
	})
}

// A deferred flattening must keep its layers and wait, unretried, for the owner to restart it.
func TestFlattenDefersUnderMemoryPressure(t *testing.T) {
	for _, tt := range []struct {
		name  string
		slot  bool  // refused a rebuild slot rather than memory
		exact bool  // refused only the output's size, past the heap check and holding a slot
		first int32 // asks in the refused attempt
		asked int32 // asks after both attempts
	}{
		{name: "no memory", first: 1, asked: 3}, // refused at the heap check; then that check and the exact ask
		{name: "no memory for the output", exact: true, first: 2, asked: 4},
		{name: "no rebuild slot", slot: true, first: 1, asked: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
			require.NoError(t, idx.MergeMemtableByCursor(
				newMockCursor().add([]byte("b"), []uint64{2}, nil)))

			var refuse atomic.Bool
			refuse.Store(true)
			var asked, released, taken atomic.Int32
			var bytes atomic.Int64
			if tt.exact {
				idx.acquireSlot = func() (func(), bool) {
					taken.Add(1)
					return func() { released.Add(1) }, true
				}
			}
			if tt.slot {
				idx.acquireSlot = func() (func(), bool) {
					asked.Add(1)
					if refuse.Load() {
						return nil, false
					}
					return func() { released.Add(1) }, true
				}
			} else {
				idx.checkAlloc = func(n int64) error {
					asked.Add(1)
					bytes.Store(n)
					if refuse.Load() && (n > 0 || !tt.exact) {
						return errors.New("no memory")
					}
					return nil
				}
			}

			flattenNow(t, idx)
			assert.Len(t, idx.state.Load().layers, 1, "the layers stay for the next attempt")
			assert.Equal(t, []uint64{2}, resolveSorted(t, idx, "b"), "and are still answered")
			assert.True(t, idx.flattenPending.Load(), "the deferral waits for the owner")
			assert.Equal(t, tt.first, asked.Load(), "and nothing retries it in the meantime")
			if tt.exact {
				assert.Equal(t, taken.Load(), released.Load(), "the slot it held is given back")
			}

			refuse.Store(false)
			require.True(t, idx.StartPendingFlatten(), "the owner's call starts it")
			waitForFlatten(t, idx)
			assert.Equal(t, tt.asked, asked.Load())
			assert.Empty(t, idx.state.Load().layers, "the layers are folded in")
			assert.Equal(t, []uint64{1, 2}, resolveSorted(t, idx, "a", "b"))
			assert.False(t, idx.StartPendingFlatten(), "and nothing is left pending")
			if tt.slot {
				assert.Equal(t, int32(1), released.Load(), "the slot is given back")
			} else {
				assert.Positive(t, bytes.Load(), "the check is told what the merge will allocate")
			}
		})
	}
}

// A deferred flattening walked the whole merge before it learned it was refused memory or a slot.
func TestDeferredFlattenWalksNothing(t *testing.T) {
	for _, tt := range []struct {
		name string
		slot bool // refused a rebuild slot rather than memory
	}{
		{name: "no memory"},
		{name: "no rebuild slot", slot: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var keys [][]byte
			var docs []uint64
			for i := range 1000 {
				keys, docs = append(keys, fmt.Appendf(nil, "k%04d", i)), append(docs, uint64(i))
			}
			idx := newTestIndex(runFromPairs(keys, docs))
			require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("z"), []uint64{5_000}, nil)))
			if tt.slot {
				idx.checkAlloc = func(int64) error { return nil }
				idx.acquireSlot = func() (func(), bool) { return nil, false }
			} else {
				idx.checkAlloc = func(int64) error { return errors.New("no memory") }
			}
			state := idx.state.Load()
			walk := testing.AllocsPerRun(20, func() { planFlatten(state.base, state.layers, neverStopped) })

			refusal := testing.AllocsPerRun(20, func() {
				deferredBy, _ := idx.rebuildBase()
				require.Error(t, deferredBy)
			})
			assert.Less(t, refusal, walk, "a refused attempt costs less than the walk it must skip")
		})
	}
}

// Layers that reached the threshold while a flattening ran were left behind for good once the bucket went idle.
func TestFlattenLeavesTheBacklogItRacedPending(t *testing.T) {
	for _, tt := range []struct {
		name string
		// whileHeld counts flushes merged while the flattening is held, afterPublish
		// those merged between its publish and the clear of its in-flight flag
		whileHeld, afterPublish int
	}{
		{name: "flushes while it runs", whileHeld: flattenLayersThreshold},
		{name: "a flush between its publish and its clear", whileHeld: flattenLayersThreshold - 1, afterPublish: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
			entered, release := make(chan struct{}), make(chan struct{})
			var held atomic.Bool
			idx.acquireSlot = func() (func(), bool) {
				if held.CompareAndSwap(false, true) {
					close(entered)
					<-release
				}
				return func() {}, true
			}
			doc := uint64(2)
			flush := func() {
				require.NoError(t, idx.MergeMemtableByCursor(
					newMockCursor().add(fmt.Appendf(nil, "k%02d", doc), []uint64{doc}, nil)))
				doc++
			}
			var published atomic.Bool
			idx.onResize = func() {
				if published.CompareAndSwap(false, true) {
					for range tt.afterPublish {
						flush()
					}
				}
			}

			for range flattenLayersThreshold {
				flush()
			}
			select { // the threshold's flush starts a flattening, held in flight
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the threshold's flush must start a flattening")
			}
			for range tt.whileHeld {
				flush()
			}
			close(release)
			waitForFlatten(t, idx)
			require.Len(t, idx.state.Load().layers, flattenLayersThreshold, "the flattening consumed only what it saw")

			require.True(t, idx.StartPendingFlatten(), "the backlog is left pending for the owner")
			waitForFlatten(t, idx)
			assert.Empty(t, idx.state.Load().layers)
			assert.False(t, idx.StartPendingFlatten(), "and nothing is left pending")
		})
	}
}

// A flattening asks once for exactly what it builds, and the base it publishes holds no slack.
func TestFlattenAsksForAndBuildsExactlyItsOutput(t *testing.T) {
	const keys = 1000
	notTenth := func(i int) bool { return i%10 != 0 }
	for _, tt := range []struct {
		name    string
		key     func(i int) []byte // ascending in i
		deletes func(i int) bool   // else the flush adds ten keys past the base
		backing keyColumn
		asks    int64
	}{
		{
			name: "prefix", key: func(i int) []byte { return fmt.Appendf(nil, "k%04d", i) },
			deletes: notTenth, backing: &prefixKeyColumn{},
			// 100 keys of 5 bytes, offsets, 2-byte docIDs, then re-packed past "k0"
			asks: 500 + 4*101 + 2*100 + (100*3 + 2),
		},
		{
			// survivors span the corpus, so the keys share no prefix to re-pack
			name: "fixed", key: func(i int) []byte { return []byte{byte(i >> 8), byte(i)} },
			deletes: notTenth, backing: &fixedKeyColumn{},
			asks: 200 + 4*101 + 2*100,
		},
		{
			name: "blob", key: func(i int) []byte { return fmt.Appendf(nil, "%04d%s", i, strings.Repeat("x", i%3)) },
			deletes: notTenth, backing: &blobKeyColumn{},
			asks: 499 + 4*101 + 2*100,
		},
		{
			name:    "mixed widths whose odd one out is deleted",
			key:     func(i int) []byte { return fmt.Appendf(nil, "%04d%s", i, strings.Repeat("x", i%2)) },
			deletes: func(i int) bool { return i%2 == 1 }, backing: &prefixKeyColumn{},
			asks: 2000 + 4*501 + 2*500 + (500*3 + 1),
		},
		{
			name: "fixed, inserts only", key: func(i int) []byte { return []byte{byte(i >> 8), byte(i)} },
			backing: &fixedKeyColumn{},
			asks:    2020 + 4*1011 + 2*1010,
		},
		{
			name: "prefix, inserts only", key: func(i int) []byte { return fmt.Appendf(nil, "k%04d", i) },
			backing: &prefixKeyColumn{},
			// re-packed past "k", since the inserts reach k1000
			asks: 5050 + 4*1011 + 2*1010 + (1010*4 + 1),
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			baseKeys, baseDocs := make([][]byte, keys), make([]uint64, keys)
			cursor := newMockCursor()
			for i := range keys {
				baseKeys[i], baseDocs[i] = tt.key(i), uint64(i)
				if tt.deletes != nil && tt.deletes(i) {
					cursor.add(baseKeys[i], nil, []uint64{uint64(i)})
				}
			}
			if tt.deletes == nil {
				for i := keys; i < keys+10; i++ {
					cursor.add(tt.key(i), []uint64{uint64(i)}, nil)
				}
			}
			idx := newTestIndex(runFromPairs(baseKeys, baseDocs))
			require.NoError(t, idx.MergeMemtableByCursor(cursor))
			var asks []int64
			idx.checkAlloc = func(n int64) error { asks = append(asks, n); return nil }

			flattenNow(t, idx)
			require.Empty(t, idx.state.Load().layers)
			assert.Equal(t, []int64{0, tt.asks}, asks, "a check for any room at all, then exactly what the flattening builds")

			run := idx.state.Load().base
			require.IsType(t, tt.backing, run.keys)
			assert.Equal(t, run.keys.len()*run.docs.w, cap(run.docs.data), "the docID column holds no slack")
			switch c := run.keys.(type) {
			case *blobKeyColumn:
				assert.Equal(t, len(c.blob), cap(c.blob), "the key blob holds no slack")
				assert.Equal(t, len(c.offsets), cap(c.offsets), "the offsets hold no slack")
			case *fixedKeyColumn:
				assert.Equal(t, len(c.data), cap(c.data), "the keys hold no slack")
			}
		})
	}
}

// Rebuilds deferred in a row warn once, at the threshold, and a rebuild that lands re-arms it.
func TestFlattenWarnsAfterDeferralsInARow(t *testing.T) {
	logger, hook := test.NewNullLogger()
	idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	idx.logger = logger
	idx.name = fmt.Sprintf("%s/%d", t.Name(), time.Now().UnixNano())
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("b"), []uint64{2}, nil)))
	var refuse atomic.Bool
	idx.checkAlloc = func(int64) error {
		if refuse.Load() {
			return errors.New("no memory")
		}
		return nil
	}
	deferredWarnings := func() int {
		n := 0
		for _, e := range hook.AllEntries() {
			if e.Level == logrus.WarnLevel && strings.Contains(e.Message, "deferred its rebuild") {
				n++
			}
		}
		return n
	}
	deferTimes := func(n int) {
		for range n {
			flattenNow(t, idx)
			require.True(t, idx.flattenPending.Load())
		}
	}

	refuse.Store(true)
	deferTimes(flattenDeferralsBeforeWarning - 1)
	assert.Zero(t, deferredWarnings(), "not before the threshold")
	deferTimes(flattenDeferralsBeforeWarning + 1)
	assert.Equal(t, 1, deferredWarnings(), "once at the threshold, not at every deferral past it")
	assert.Contains(t, hook.LastEntry().Message, "no memory", "the warning names what refused the rebuild")

	refuse.Store(false)
	flattenNow(t, idx)
	require.Empty(t, idx.state.Load().layers)
	require.NoError(t, idx.MergeMemtableByCursor(newMockCursor().add([]byte("c"), []uint64{3}, nil)))
	refuse.Store(true)
	deferTimes(flattenDeferralsBeforeWarning)
	assert.Equal(t, 2, deferredWarnings(), "a rebuild that lands re-arms the warning")
}

// TestFlattenReportsABaseNoLongerWorthHolding pins onUnfit firing when two flushes, each under overflowFloor, sum past it.
func TestFlattenReportsABaseNoLongerWorthHolding(t *testing.T) {
	// enough keys that both bounds are the floor rather than the ratio
	const baseKeys = 3_000
	keys := make([][]byte, baseKeys)
	docs := make([]uint64, baseKeys)
	for i := range keys {
		keys[i] = fmt.Appendf(nil, "k%06d", i)
		docs[i] = uint64(i + 1)
	}
	const half = overflowFloor/2 + 1 // two of these clear the floor

	tests := []struct {
		name  string
		flush func(f int) *mockCursor
	}{
		{
			name: "keys holding several documents",
			flush: func(f int) *mockCursor {
				cursor := newMockCursor()
				for i := f * half; i < (f+1)*half; i++ {
					cursor.add(keys[i], []uint64{uint64(baseKeys + i + 1)}, nil)
				}
				return cursor
			},
		},
		{
			name: "documents under one key",
			flush: func(f int) *mockCursor {
				from := uint64(baseKeys + 1 + f*half)
				return newMockCursor().add(keys[0], sequence(from, from+half), nil)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newTestIndex(runFromPairs(keys, docs))
			unfit := 0
			idx.onUnfit = func() { unfit++ }
			var asked []int64
			idx.checkAlloc = func(n int64) error {
				asked = append(asked, n)
				if n > 0 {
					return errors.New("no memory")
				}
				return nil
			}

			for f := range 2 {
				require.NoError(t, idx.MergeMemtableByCursor(tt.flush(f)),
					"a flush under the floor is allowed on its own")
			}
			flattenNow(t, idx)

			assert.Equal(t, 1, unfit, "the flattened base is past the bound and says so")
			assert.Equal(t, []int64{0}, asked, "caught from its counts, before asking memory for a base it will not build")
			assert.Len(t, idx.state.Load().layers, 2,
				"and is not published: the index is about to be dropped")
			assert.Contains(t, resolveSorted(t, idx, "k000000"), uint64(1),
				"until it is, it still answers")
		})
	}
}

// TestOverflowRefusal covers the bounds themselves, at each one's edge.
func TestOverflowRefusal(t *testing.T) {
	const (
		keysAtRatio   = overflowFloor * overflowRowsPerKeyLimit * 2   // key bound 2*floor
		extrasAtRatio = overflowFloor * overflowRowsPerExtraLimit * 2 // document bound 2*floor
	)
	tests := []struct {
		name               string
		rows, keys, extras int
		refused            bool
	}{
		{name: "nothing duplicated", rows: 10},
		{name: "a small corpus stays under the floor", rows: 2, keys: 2, extras: overflowFloor},
		{name: "documents past the floor", rows: 2, keys: 2, extras: overflowFloor + 1, refused: true},
		{name: "keys at the floor", rows: 1_000, keys: overflowFloor, extras: overflowFloor},
		{name: "keys past the floor", rows: 1_000, keys: overflowFloor + 1, extras: overflowFloor + 1, refused: true},
		{name: "keys at the ratio", rows: keysAtRatio, keys: 2 * overflowFloor, extras: 2 * overflowFloor},
		{name: "keys past the ratio", rows: keysAtRatio, keys: 2*overflowFloor + 1, extras: 2*overflowFloor + 1, refused: true},
		{name: "documents at the ratio", rows: extrasAtRatio, keys: 1, extras: 2 * overflowFloor},
		{name: "documents past the ratio", rows: extrasAtRatio, keys: 1, extras: 2*overflowFloor + 1, refused: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			why := overflowRefusal(tt.rows, tt.keys, tt.extras)
			assert.Equal(t, tt.refused, why != "", why)
		})
	}
}

// TestOverflowBoundAtBuildAndFlush pins the build and a flush both refusing data past either bound and accepting near-unique data.
func TestOverflowBoundAtBuildAndFlush(t *testing.T) {
	const docIDCount = 100_000
	// corpus[i] is key i's documents; keys are generated sorted
	type corpus [][]uint64
	nearUnique := func(n, duplicated int) corpus {
		c := make(corpus, n)
		for i := range c {
			c[i] = []uint64{uint64(2 * i)}
			if i < duplicated {
				c[i] = append(c[i], uint64(2*i+1))
			}
		}
		return c
	}
	tests := []struct {
		name    string
		corpus  corpus
		refused bool
	}{
		{
			name:    "a boolean: two values over many documents",
			corpus:  corpus{sequence(0, overflowFloor/2+2), sequence(overflowFloor, overflowFloor*3/2+2)},
			refused: true,
		},
		{name: "near-unique with a handful duplicated", corpus: nearUnique(5_000, 3)},
		{
			// past the key bound and under the document one
			name:    "many values each duplicated",
			corpus:  nearUnique(20_000, overflowFloor+1),
			refused: true,
		},
	}

	logger, _ := test.NewNullLogger()
	keyOf := func(i int) []byte { return fmt.Appendf(nil, "k%06d", i) }
	for _, tt := range tests {
		t.Run(tt.name+", at build", func(t *testing.T) {
			cursor := &pairsCursor{}
			for i, docs := range tt.corpus {
				cursor.keys = append(cursor.keys, keyOf(i))
				cursor.bms = append(cursor.bms, sroar.FromSortedList(docs))
			}
			_, err := BuildFromCursor(cursor, BuildOptions{
				DocIDCount: func() uint64 { return docIDCount },
				Logger:     logger,
			})
			assertRefused(t, tt.refused, err)
		})
		t.Run(tt.name+", at a flush", func(t *testing.T) {
			idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{docIDCount - 1}))
			cursor := newMockCursor()
			for i, docs := range tt.corpus {
				cursor.add(keyOf(i), docs, nil)
			}
			assertRefused(t, tt.refused, idx.MergeMemtableByCursor(cursor))
		})
	}
}

// A flush with one common value was refused against its own few rows though the column it joins accepts it.
func TestFlushWeighedAgainstTheColumnItJoins(t *testing.T) {
	const (
		bigColumn = 20_000 // its tenth is past the hot value's extras
		hotDocs   = overflowFloor + 76
	)
	unique := func(prefix string, n int, firstDoc uint64) ([][]byte, []uint64) {
		keys, docs := make([][]byte, n), make([]uint64, n)
		for i := range keys {
			keys[i], docs[i] = fmt.Appendf(nil, "%s%06d", prefix, i), firstDoc+uint64(i)
		}
		return keys, docs
	}
	tests := []struct {
		name                           string
		baseRows, layerRows, flushRows int
		refused                        bool
	}{
		{name: "a column too small for the value", baseRows: 1, flushRows: 400, refused: true},
		{name: "a big base", baseRows: bigColumn, flushRows: 400},
		{name: "a small base under big layers", baseRows: 1, layerRows: bigColumn, flushRows: 400},
		{name: "a flush big enough for its value", baseRows: 1, flushRows: bigColumn},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			idx := newTestIndex(runFromPairs(unique("a", tt.baseRows, 0)))
			if tt.layerRows > 0 {
				layer := newMockCursor()
				keys, docs := unique("b", tt.layerRows, bigColumn)
				for i, k := range keys {
					layer.add(k, docs[i:i+1], nil)
				}
				require.NoError(t, idx.MergeMemtableByCursor(layer))
			}

			flush := newMockCursor()
			keys, docs := unique("c", tt.flushRows, 2*bigColumn)
			for i, k := range keys {
				flush.add(k, docs[i:i+1], nil)
			}
			flush.add([]byte("d"), sequence(3*bigColumn, 3*bigColumn+hotDocs), nil)
			assertRefused(t, tt.refused, idx.MergeMemtableByCursor(flush))
		})
	}
}

// A boolean is refused before its documents are copied out of their bitmaps.
func TestBuildRefusesLowCardinalityWithoutExpandingIt(t *testing.T) {
	const docs = 1 << 21
	cursor := &pairsCursor{
		keys: [][]byte{[]byte("false"), []byte("true")},
		bms:  []*sroar.Bitmap{sroar.FromSortedList(sequence(0, docs/2)), sroar.FromSortedList(sequence(docs/2, docs))},
	}
	logger, _ := test.NewNullLogger()

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	_, err := BuildFromCursor(cursor, BuildOptions{DocIDCount: func() uint64 { return docs }, Logger: logger})
	runtime.ReadMemStats(&after)

	require.ErrorContains(t, err, "unique values")
	assert.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(docs*8/16),
		"a refused build allocates a small share of the 8 bytes per document expanding costs")
}

func assertRefused(t *testing.T, refused bool, err error) {
	t.Helper()
	if !refused {
		require.NoError(t, err)
		return
	}
	require.ErrorContains(t, err, "unique values")
}

// TestReAddedDocumentIsRetiredOnce pins a document re-added by a later flush retired by one deletion, in the slot and the extras alike.
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
			// 7 takes the vacated slot while still in the extras, so the key holds it twice
			name: "re-added after the slot it vacated",
			flushes: []flush{
				{adds: []uint64{7}},
				{dels: []uint64{2}},
				{adds: []uint64{7}},
				{dels: []uint64{7}},
			},
			want: nil,
		},
		{
			name:    "re-added with no deletion is held once",
			flushes: []flush{{adds: []uint64{2}}, {adds: []uint64{2, 3}}},
			want:    []uint64{2, 3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := runFromPairs([][]byte{[]byte("a"), []byte("b")}, []uint64{1, 2})
			idx := newTestIndex(base)
			for _, f := range tt.flushes {
				require.NoError(t, idx.MergeMemtableByCursor(
					newMockCursor().add([]byte("b"), f.adds, f.dels)))
			}
			assert.ElementsMatch(t, tt.want, resolveSorted(t, idx, "b"))

			flattenNow(t, idx)
			require.Empty(t, idx.state.Load().layers)
			assert.Equal(t, tt.want, columnRunPairs(idx.state.Load().base)["b"],
				"a flattened base holds each document once")
		})
	}
}

// pairsCursor is a [MergedCursor] over given keys and bitmaps, empty and out-of-range ones included.
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

// TestBuildSkipsEmptyKeysAndRefusesOutOfRangeDocs pins an empty bitmap skipped and a docID at DocIDCount refused, not truncated.
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

// TestWarnOncePerName pins WarnOncePerName warning once per name and logging repeats at debug.
func TestWarnOncePerName(t *testing.T) {
	logger, hook := test.NewNullLogger()
	logger.SetLevel(logrus.DebugLevel)
	// unique per run, so -count above 1 does not meet an earlier run's latch
	run := fmt.Sprintf("%s/%d", t.Name(), time.Now().UnixNano())

	for _, name := range []string{run + "/a", run + "/a", run + "/b"} {
		_, err := BuildFromCursor(&pairsCursor{
			keys: [][]byte{[]byte("k")},
			bms:  []*sroar.Bitmap{sroar.FromSortedList([]uint64{1, 2})},
		}, BuildOptions{Name: name, DocIDCount: func() uint64 { return 100 }, Logger: logger})
		require.NoError(t, err)
	}

	levels := map[logrus.Level]int{}
	for _, e := range hook.AllEntries() {
		levels[e.Level]++
	}
	assert.Equal(t, 2, levels[logrus.WarnLevel], "one warning per name")
	assert.Equal(t, 1, levels[logrus.DebugLevel], "the repeat stays at debug")
}

// TestMergeStopsOnACursorError pins a cursor error failing the merge with no partial layer kept.
func TestMergeStopsOnACursorError(t *testing.T) {
	idx := newTestIndex(runFromPairs([][]byte{[]byte("a")}, []uint64{1}))
	unreadable := errors.New("segment cannot be read")

	err := idx.MergeMemtableByCursor(newMockCursor().
		add([]byte("b"), []uint64{2}, nil).
		add([]byte("c"), []uint64{3}, nil).
		failsAt(1, unreadable))

	require.ErrorIs(t, err, unreadable)
	require.Empty(t, idx.state.Load().layers, "a flush read in part is not a layer")
	require.Equal(t, []uint64{1}, resolveSorted(t, idx, "a", "b", "c"))
}
