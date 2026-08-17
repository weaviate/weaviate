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
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// segmentDocs reads every document row i holds — its column entry and any
// overflow — in stored order. Production reads a row through applyRow or
// segmentCursor, both of which apply deletions on the way; this is for tests that
// want to see a segment exactly as it was built.
func segmentDocs(seg *segment, i int) []uint64 {
	return append([]uint64{seg.docs.at(i)}, seg.overflow[uint32(i)]...)
}

// segmentPairs reads a segment back out as key → its documents, sorted, for
// comparing a merged base against an independently computed expectation.
func segmentPairs(seg *segment) map[string][]uint64 {
	out := make(map[string][]uint64, seg.keys.len())
	for i := 0; i < seg.keys.len(); i++ {
		docs := segmentDocs(seg, i)
		sort.Slice(docs, func(a, b int) bool { return docs[a] < docs[b] })
		out[string(seg.keys.appendKey(i, nil))] = docs
	}
	return out
}

// docSet is the test's model of what one key holds.
type docSet map[uint64]bool

func (s docSet) sorted() []uint64 {
	out := make([]uint64, 0, len(s))
	for d := range s {
		out = append(out, d)
	}
	sort.Slice(out, func(a, b int) bool { return out[a] < out[b] })
	return out
}

// modelPairs renders the model the same way segmentPairs renders a segment.
func modelPairs(model map[string]docSet) map[string][]uint64 {
	out := make(map[string][]uint64, len(model))
	for k, set := range model {
		if len(set) > 0 {
			out[k] = set.sorted()
		}
	}
	return out
}

// TestFlattenMatchesReplay drives randomized base+layer layouts through the
// merge and compares against a map replaying the same layers in the same order.
// The oracle is the naive formulation: apply each layer's deletions to whatever
// docID the key currently holds, then its addition.
func TestFlattenMatchesReplay(t *testing.T) {
	const (
		universe  = 200
		numLayers = 6
	)
	keyOf := func(i int) string { return fmt.Sprintf("key_%04d", i) }

	for _, seed := range []int64{1, 2, 3, 7, 11, 42} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			model := map[string]docSet{}

			// base: a random subset of the universe, docIDs 1..universe
			var baseKeys [][]byte
			var baseDocs []uint64
			for i := 0; i < universe; i++ {
				if rnd.Intn(2) == 0 {
					continue
				}
				baseKeys = append(baseKeys, []byte(keyOf(i)))
				baseDocs = append(baseDocs, uint64(i+1))
				model[keyOf(i)] = docSet{uint64(i + 1): true}
			}
			idx := newTestIndex(segFromPairs(baseKeys, baseDocs))

			nextDoc := uint64(universe + 1)
			for r := 0; r < numLayers; r++ {
				cursor := newMockCursor()
				// walk the universe in order so the cursor's keys stay sorted
				for i := 0; i < universe; i++ {
					k := keyOf(i)
					var adds, dels []uint64

					switch rnd.Intn(6) {
					case 0: // delete whatever the key currently holds
						dels = model[k].sorted()
					case 1: // stale delete: a docID this key does not hold, sometimes one
						// another key does. Both resolution and a flattening apply a
						// deletion only to the key it was issued under, so neither is
						// allowed to disturb the owning key.
						dels = []uint64{uint64(rnd.Intn(universe) + 1)}
					case 2: // replace: retire what is there and add a fresh document
						dels = model[k].sorted()
						adds = []uint64{nextDoc}
						nextDoc++
					case 3: // add without deleting — the key ends up holding several,
						// which is the non-unique case the overflow carries
						adds = []uint64{nextDoc}
						nextDoc++
					}
					if len(adds) == 0 && len(dels) == 0 {
						continue
					}
					cursor.add([]byte(k), adds, dels)

					// same order the merge replays: deletions, then additions
					for _, d := range dels {
						delete(model[k], d)
					}
					for _, a := range adds {
						if model[k] == nil {
							model[k] = docSet{}
						}
						model[k][a] = true
					}
				}
				require.NoError(t, idx.MergeMemtableByCursor(cursor))
			}

			state := idx.state.Load()
			merged := flatten(state.base, state.layers, nil)
			require.Equal(t, modelPairs(model), segmentPairs(merged),
				"merged base must equal the replayed net state")

			// keys must come out sorted, since everything downstream binary-searches
			for i := 1; i < merged.keys.len(); i++ {
				prev := string(merged.keys.appendKey(i-1, nil))
				cur := string(merged.keys.appendKey(i, nil))
				require.Less(t, prev, cur, "merged keys must be strictly ascending")
			}

			// and the merged base must answer queries identically to the layers it replaced
			all := make([]string, 0, universe)
			for i := 0; i < universe; i++ {
				all = append(all, keyOf(i))
			}
			before := resolveSorted(t, idx, all...)
			idx.flattenIntoBase()
			require.Equal(t, before, resolveSorted(t, idx, all...),
				"flattening must not change what the index resolves")
		})
	}
}

// TestFlattenEdgeCases covers the shapes the randomized layer reaches rarely or
// never: nothing in the base, nothing in a layer, and a key whose only mention is
// a deletion.
func TestFlattenEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		base  [][2]any // key, docID
		flush []keyLayer
		want  map[string][]uint64
	}{
		{
			name:  "empty base, layer adds",
			flush: []keyLayer{{key: "a", adds: []uint64{1}}},
			want:  map[string][]uint64{"a": {1}},
		},
		{
			name:  "deletion of a key the base never held",
			base:  [][2]any{{"b", uint64(2)}},
			flush: []keyLayer{{key: "a", dels: []uint64{9}}},
			want:  map[string][]uint64{"b": {2}},
		},
		{
			name:  "delete then re-add in the same flush",
			base:  [][2]any{{"a", uint64(1)}},
			flush: []keyLayer{{key: "a", adds: []uint64{5}, dels: []uint64{1}}},
			want:  map[string][]uint64{"a": {5}},
		},
		{
			// a text property tokenized as a field produces one for an empty value
			name:  "zero-length key in the base",
			base:  [][2]any{{"", uint64(1)}, {"a", uint64(2)}},
			flush: []keyLayer{{key: "b", adds: []uint64{3}}},
			want:  map[string][]uint64{"": {1}, "a": {2}, "b": {3}},
		},
		{
			name:  "zero-length key arrives in a layer",
			base:  [][2]any{{"a", uint64(1)}},
			flush: []keyLayer{{key: "", adds: []uint64{7}}},
			want:  map[string][]uint64{"": {7}, "a": {1}},
		},
		{
			name:  "every base key deleted",
			base:  [][2]any{{"a", uint64(1)}, {"b", uint64(2)}},
			flush: []keyLayer{{key: "a", dels: []uint64{1}}, {key: "b", dels: []uint64{2}}},
			want:  map[string][]uint64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var keys [][]byte
			var docs []uint64
			for _, p := range tt.base {
				// append rather than convert: a zero-length key has to reach the
				// column as a non-nil empty slice, which is how the write path
				// delivers one
				keys = append(keys, append([]byte{}, p[0].(string)...))
				docs = append(docs, p[1].(uint64))
			}
			idx := newTestIndex(segFromPairs(keys, docs))

			cursor := newMockCursor()
			for _, f := range tt.flush {
				cursor.add(append([]byte{}, f.key...), f.adds, f.dels)
			}
			require.NoError(t, idx.MergeMemtableByCursor(cursor))

			state := idx.state.Load()
			require.Equal(t, tt.want, segmentPairs(flatten(state.base, state.layers, nil)))
		})
	}
}

// TestFlattenCarriesDocIDsOfEveryWidth pins that a merged segment gives back the
// documents it was given, whatever their width.
//
// The output column is packed at the narrowest width holding the largest docID
// across the inputs, taken from what each input says it holds rather than by
// rescanning them. Nothing rechecks that afterwards: encoding a docID wider than
// the column drops its high bytes, so a bound that is too small is not an error
// but a different document, returned to a query that has no way to tell.
func TestFlattenCarriesDocIDsOfEveryWidth(t *testing.T) {
	tests := []struct {
		name string
		doc  uint64
	}{
		{name: "one byte", doc: 200},
		{name: "two bytes", doc: 300},
		{name: "three bytes", doc: 70_000},
		{name: "the largest the test bound allows", doc: 999_999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want := map[string][]uint64{"a": {tt.doc}}

			// with no layers the base is the only thing bounding the output
			base := segFromPairs([][]byte{[]byte("a")}, []uint64{tt.doc})
			require.Equal(t, want, segmentPairs(flatten(base, nil, nil)))

			// and again with the document arriving in a layer, over a base whose
			// own documents are narrower
			idx := newTestIndex(segFromPairs([][]byte{[]byte("a")}, []uint64{1}))
			require.NoError(t, idx.MergeMemtableByCursor(
				newMockCursor().add([]byte("a"), []uint64{tt.doc}, []uint64{1})))

			state := idx.state.Load()
			require.Equal(t, want, segmentPairs(flatten(state.base, state.layers, nil)))
		})
	}
}

// TestFlattenAcrossKeyBackings runs the merge over each key backing, since
// the merge compares keys through appendKey and the prefix backing reconstructs
// them from an elided prefix.
func TestFlattenAcrossKeyBackings(t *testing.T) {
	tests := []struct {
		name    string
		keyOf   func(i int) string
		backing any
	}{
		{
			name:    "fixed width, shared prefix",
			keyOf:   func(i int) string { return fmt.Sprintf("key_%04d", i) },
			backing: &prefixKeyColumn{},
		},
		{
			name: "variable width",
			// widths differ so buildKeyColumn cannot take the fixed-width path
			keyOf:   func(i int) string { return fmt.Sprintf("k%d", i) },
			backing: &blobKeyColumn{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var keys [][]byte
			var docs []uint64
			model := map[string][]uint64{}
			names := make([]string, 0, 40)
			for i := 0; i < 40; i++ {
				names = append(names, tt.keyOf(i))
			}
			sort.Strings(names)
			for i, n := range names {
				keys = append(keys, []byte(n))
				docs = append(docs, uint64(i+1))
				model[n] = []uint64{uint64(i + 1)}
			}
			idx := newTestIndex(segFromPairs(keys, docs))
			require.IsType(t, tt.backing, idx.state.Load().base.keys)

			cursor := newMockCursor()
			for i, n := range names {
				if i%3 != 0 {
					continue
				}
				cursor.add([]byte(n), []uint64{uint64(1000 + i)}, []uint64{uint64(i + 1)})
				model[n] = []uint64{uint64(1000 + i)}
			}
			require.NoError(t, idx.MergeMemtableByCursor(cursor))

			state := idx.state.Load()
			require.Equal(t, model, segmentPairs(flatten(state.base, state.layers, nil)))
		})
	}
}
