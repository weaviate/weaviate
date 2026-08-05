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

package columnar

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

// segmentPairs reads a segment back out as key → docID, for comparing a merged
// base against an independently computed expectation.
func segmentPairs(seg *columnarSegment) map[string]uint64 {
	out := make(map[string]uint64, seg.keys.len())
	for i := 0; i < seg.keys.len(); i++ {
		out[string(seg.keys.appendKey(i, nil))] = seg.docs.at(i)
	}
	return out
}

// TestMergeTiersMatchesReplay drives randomized base+run layouts through the
// merge and compares against a map replaying the same tiers in the same order.
// The oracle is deliberately the naive formulation the merge replaced: apply
// each run's deletions to whatever docID the key currently holds, then its
// addition.
func TestMergeTiersMatchesReplay(t *testing.T) {
	const (
		universe = 200
		numRuns  = 6
	)
	keyOf := func(i int) string { return fmt.Sprintf("key_%04d", i) }

	for _, seed := range []int64{1, 2, 3, 7, 11, 42} {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			rnd := rand.New(rand.NewSource(seed))
			model := map[string]uint64{}

			// base: a random subset of the universe, docIDs 1..universe
			var baseKeys [][]byte
			var baseDocs []uint64
			for i := 0; i < universe; i++ {
				if rnd.Intn(2) == 0 {
					continue
				}
				baseKeys = append(baseKeys, []byte(keyOf(i)))
				baseDocs = append(baseDocs, uint64(i+1))
				model[keyOf(i)] = uint64(i + 1)
			}
			idx := newTestIndex(segFromPairs(baseKeys, baseDocs))

			nextDoc := uint64(universe + 1)
			for r := 0; r < numRuns; r++ {
				cursor := newMockCursor()
				// walk the universe in order so the cursor's keys stay sorted
				for i := 0; i < universe; i++ {
					k := keyOf(i)
					var adds, dels []uint64

					switch rnd.Intn(6) {
					case 0: // delete whatever the key currently holds
						if cur, ok := model[k]; ok {
							dels = []uint64{cur}
						}
					case 1: // stale delete: a docID this key does not hold, sometimes one
						// another key does. Both resolution and the fold apply a
						// deletion only to the key it was issued under, so neither is
						// allowed to disturb the owning key.
						dels = []uint64{uint64(rnd.Intn(universe) + 1)}
					case 2: // replace: retire the current docID and add a fresh one
						if cur, ok := model[k]; ok {
							dels = []uint64{cur}
						}
						adds = []uint64{nextDoc}
						nextDoc++
					case 3: // add, but only where the key holds nothing — a key with two
						// live docIDs is refused at build time, so the index never
						// holds one and the merge is not defined for it
						if _, ok := model[k]; !ok {
							adds = []uint64{nextDoc}
							nextDoc++
						}
					}
					if len(adds) == 0 && len(dels) == 0 {
						continue
					}
					cursor.add([]byte(k), adds, dels)

					// same order the merge replays: deletions, then the addition
					for _, d := range dels {
						if cur, ok := model[k]; ok && cur == d {
							delete(model, k)
						}
					}
					for _, a := range adds {
						model[k] = a
					}
				}
				require.NoError(t, idx.AbsorbFlush(cursor))
			}

			state := idx.state.Load()
			merged := mergeTiers(state.base, state.runs)
			require.Equal(t, model, segmentPairs(merged),
				"merged base must equal the replayed net state")

			// keys must come out sorted, since everything downstream binary-searches
			for i := 1; i < merged.keys.len(); i++ {
				prev := string(merged.keys.appendKey(i-1, nil))
				cur := string(merged.keys.appendKey(i, nil))
				require.Less(t, prev, cur, "merged keys must be strictly ascending")
			}

			// and the merged base must answer queries identically to the tiers it replaced
			all := make([]string, 0, universe)
			for i := 0; i < universe; i++ {
				all = append(all, keyOf(i))
			}
			before := resolveSorted(idx, all...)
			idx.foldRunsIntoBase()
			require.Equal(t, before, resolveSorted(idx, all...),
				"folding must not change what the index resolves")
		})
	}
}

// TestMergeTiersEdgeCases covers the shapes the randomized run reaches rarely or
// never: nothing in the base, nothing in a run, and a key whose only mention is
// a deletion.
func TestMergeTiersEdgeCases(t *testing.T) {
	tests := []struct {
		name  string
		base  [][2]any // key, docID
		flush []struct {
			key  string
			adds []uint64
			dels []uint64
		}
		want map[string]uint64
	}{
		{
			name: "empty base, run adds",
			flush: []struct {
				key  string
				adds []uint64
				dels []uint64
			}{{key: "a", adds: []uint64{1}}},
			want: map[string]uint64{"a": 1},
		},
		{
			name: "deletion of a key the base never held",
			base: [][2]any{{"b", uint64(2)}},
			flush: []struct {
				key  string
				adds []uint64
				dels []uint64
			}{{key: "a", dels: []uint64{9}}},
			want: map[string]uint64{"b": 2},
		},
		{
			name: "delete then re-add in the same flush",
			base: [][2]any{{"a", uint64(1)}},
			flush: []struct {
				key  string
				adds []uint64
				dels []uint64
			}{{key: "a", adds: []uint64{5}, dels: []uint64{1}}},
			want: map[string]uint64{"a": 5},
		},
		{
			name: "every base key deleted",
			base: [][2]any{{"a", uint64(1)}, {"b", uint64(2)}},
			flush: []struct {
				key  string
				adds []uint64
				dels []uint64
			}{{key: "a", dels: []uint64{1}}, {key: "b", dels: []uint64{2}}},
			want: map[string]uint64{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var keys [][]byte
			var docs []uint64
			for _, p := range tt.base {
				keys = append(keys, []byte(p[0].(string)))
				docs = append(docs, p[1].(uint64))
			}
			idx := newTestIndex(segFromPairs(keys, docs))

			cursor := newMockCursor()
			for _, f := range tt.flush {
				cursor.add([]byte(f.key), f.adds, f.dels)
			}
			require.NoError(t, idx.AbsorbFlush(cursor))

			state := idx.state.Load()
			require.Equal(t, tt.want, segmentPairs(mergeTiers(state.base, state.runs)))
		})
	}
}

// TestMergeTiersAcrossKeyBackings runs the merge over each key backing, since
// the merge compares keys through appendKey and the prefix backing reconstructs
// them from an elided prefix.
func TestMergeTiersAcrossKeyBackings(t *testing.T) {
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
			model := map[string]uint64{}
			names := make([]string, 0, 40)
			for i := 0; i < 40; i++ {
				names = append(names, tt.keyOf(i))
			}
			sort.Strings(names)
			for i, n := range names {
				keys = append(keys, []byte(n))
				docs = append(docs, uint64(i+1))
				model[n] = uint64(i + 1)
			}
			idx := newTestIndex(segFromPairs(keys, docs))
			require.IsType(t, tt.backing, idx.state.Load().base.keys)

			cursor := newMockCursor()
			for i, n := range names {
				if i%3 != 0 {
					continue
				}
				cursor.add([]byte(n), []uint64{uint64(1000 + i)}, []uint64{uint64(i + 1)})
				model[n] = uint64(1000 + i)
			}
			require.NoError(t, idx.AbsorbFlush(cursor))

			state := idx.state.Load()
			require.Equal(t, model, segmentPairs(mergeTiers(state.base, state.runs)))
		})
	}
}
