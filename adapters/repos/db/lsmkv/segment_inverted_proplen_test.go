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

package lsmkv

import (
	"math"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSortPropLenPairs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ids  []uint64
	}{
		{"empty", nil},
		{"single", []uint64{42}},
		{"two_sorted", []uint64{1, 2}},
		{"two_reversed", []uint64{2, 1}},
		{"already_sorted", []uint64{1, 2, 3, 4, 5, 6, 7, 8}},
		{"reverse_sorted", []uint64{8, 7, 6, 5, 4, 3, 2, 1}},
		{"single_radix_pass", []uint64{200, 3, 100, 7}},
		{"multi_radix_pass", []uint64{1 << 40, 1, 1 << 24, 1 << 8, 1 << 16, 0}},
		{"max_uint64", []uint64{math.MaxUint64, 0, math.MaxUint64 - 1, 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ids := append([]uint64(nil), tc.ids...)
			lens := make([]uint32, len(ids))
			want := make(map[uint64]uint32, len(ids))
			for i, id := range ids {
				lens[i] = uint32(i + 1)
				want[id] = lens[i]
			}

			sortPropLenPairs(ids, lens)

			require.True(t, sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }))
			for i, id := range ids {
				assert.Equal(t, want[id], lens[i], "length must move together with id %d", id)
			}
		})
	}

	t.Run("random_large", func(t *testing.T) {
		t.Parallel()

		rnd := rand.New(rand.NewSource(7))
		const n = 200_000
		ids := make([]uint64, n)
		lens := make([]uint32, n)
		want := make(map[uint64]uint32, n)
		for i := range ids {
			// unique ids: random high bits + index low bits
			ids[i] = rnd.Uint64()<<20 | uint64(i)
			lens[i] = rnd.Uint32()
			want[ids[i]] = lens[i]
		}

		sortPropLenPairs(ids, lens)

		require.True(t, sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }))
		for i, id := range ids {
			require.Equal(t, want[id], lens[i])
		}
	})
}

// buildView constructs a view over the given docID->length set in the requested
// representation, mirroring loadPropertyLengths' two layouts.
func buildView(t *testing.T, m map[uint64]uint32, dense bool) propLengthsView {
	t.Helper()
	ids := make([]uint64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	if dense {
		require.NotEmpty(t, m, "dense layout needs at least one entry")
		minID, maxID := ids[0], ids[len(ids)-1]
		arr := make([]uint32, maxID-minID+1)
		for id, l := range m {
			arr[id-minID] = l
		}
		return propLengthsView{dense: arr, min: minID}
	}

	lens := make([]uint32, len(ids))
	for i, id := range ids {
		lens[i] = m[id]
	}
	return propLengthsView{ids: ids, lens: lens}
}

func TestPropLengthsViewGet(t *testing.T) {
	t.Parallel()

	// every stored length is >= 1, mirroring production data: 0 means absent
	sparse := map[uint64]uint32{}
	rnd := rand.New(rand.NewSource(11))
	next := uint64(100)
	for i := 0; i < 5000; i++ {
		next += uint64(rnd.Intn(1000) + 1)
		sparse[next] = uint32(rnd.Intn(200) + 1)
	}

	for _, layout := range []struct {
		name  string
		dense bool
	}{{"pairs", false}, {"dense", true}} {
		t.Run(layout.name, func(t *testing.T) {
			t.Parallel()

			t.Run("ascending_scan_hits_everything", func(t *testing.T) {
				v := buildView(t, sparse, layout.dense)
				ids := make([]uint64, 0, len(sparse))
				for id := range sparse {
					ids = append(ids, id)
				}
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				for _, id := range ids {
					require.Equal(t, sparse[id], v.get(id), "docID %d", id)
				}
			})

			t.Run("ascending_scan_with_misses", func(t *testing.T) {
				v := buildView(t, sparse, layout.dense)
				ids := make([]uint64, 0, len(sparse))
				for id := range sparse {
					ids = append(ids, id)
				}
				sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
				for _, id := range ids {
					if _, present := sparse[id-1]; !present {
						require.Equal(t, uint32(0), v.get(id-1), "absent docID %d", id-1)
					}
					require.Equal(t, sparse[id], v.get(id), "docID %d", id)
				}
			})

			t.Run("random_access_restarts_cursor", func(t *testing.T) {
				v := buildView(t, sparse, layout.dense)
				ids := make([]uint64, 0, len(sparse))
				for id := range sparse {
					ids = append(ids, id) // map order = random jumps incl. backward
				}
				for _, id := range ids {
					require.Equal(t, sparse[id], v.get(id), "docID %d", id)
				}
			})

			t.Run("out_of_range", func(t *testing.T) {
				v := buildView(t, sparse, layout.dense)
				assert.Equal(t, uint32(0), v.get(0))
				assert.Equal(t, uint32(0), v.get(99))
				assert.Equal(t, uint32(0), v.get(math.MaxUint64))
				// cursor exhausted by the MaxUint64 probe; earlier ids must still work
				for id, l := range sparse {
					assert.Equal(t, l, v.get(id))
					break
				}
			})

			t.Run("repeated_lookup_same_doc", func(t *testing.T) {
				v := buildView(t, sparse, layout.dense)
				for id, l := range sparse {
					require.Equal(t, l, v.get(id))
					require.Equal(t, l, v.get(id))
					require.Equal(t, l, v.get(id))
					break
				}
			})
		})
	}

	t.Run("empty_view", func(t *testing.T) {
		t.Parallel()
		var v propLengthsView
		assert.Equal(t, uint32(0), v.get(0))
		assert.Equal(t, uint32(0), v.get(42))
	})

	t.Run("pairs_adjacent_ids_use_linear_fast_path", func(t *testing.T) {
		t.Parallel()
		m := map[uint64]uint32{}
		for i := uint64(1000); i < 2000; i++ {
			m[i] = uint32(i % 97 * 2)
		}
		// lengths of 0 don't occur in production; rebuild with >=1
		for k := range m {
			m[k] = uint32(k%97 + 1)
		}
		v := buildView(t, m, false)
		for i := uint64(1000); i < 2000; i++ {
			require.Equal(t, m[i], v.get(i))
		}
	})
}

func TestMergePropLenPairs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		ids1, ids2 []uint64
		lens1      []uint32
		lens2      []uint32
		wantIDs    []uint64
		wantLens   []uint32
	}{
		{"both_empty", nil, nil, nil, nil, nil, nil},
		{
			"left_empty",
			nil,
			[]uint64{2, 5},
			nil,
			[]uint32{20, 50},
			[]uint64{2, 5},
			[]uint32{20, 50},
		},
		{
			"right_empty",
			[]uint64{1, 3},
			nil,
			[]uint32{10, 30},
			nil,
			[]uint64{1, 3},
			[]uint32{10, 30},
		},
		{
			"disjoint_interleaved",
			[]uint64{1, 4, 6},
			[]uint64{2, 3, 7},
			[]uint32{10, 40, 60},
			[]uint32{20, 30, 70},
			[]uint64{1, 2, 3, 4, 6, 7},
			[]uint32{10, 20, 30, 40, 60, 70},
		},
		{
			// duplicate docIDs: c2 (second/newer) wins, matching the compactor's
			// prior maps.Copy(toWrite<-toClean) precedence
			"duplicates_c2_wins",
			[]uint64{1, 5, 9},
			[]uint64{5, 9, 12},
			[]uint32{10, 50, 90},
			[]uint32{555, 999, 120},
			[]uint64{1, 5, 9, 12},
			[]uint32{10, 555, 999, 120},
		},
		{
			"all_duplicates",
			[]uint64{1, 2},
			[]uint64{1, 2},
			[]uint32{10, 20},
			[]uint32{11, 22},
			[]uint64{1, 2},
			[]uint32{11, 22},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ids, lens := mergePropLenPairs(tc.ids1, tc.lens1, tc.ids2, tc.lens2)
			assert.Equal(t, tc.wantIDs, nilIfEmpty(ids))
			assert.Equal(t, tc.wantLens, nilIfEmptyU32(lens))
			require.True(t, sort.SliceIsSorted(ids, func(i, j int) bool { return ids[i] < ids[j] }))
		})
	}
}

func nilIfEmpty(s []uint64) []uint64 {
	if len(s) == 0 {
		return nil
	}
	return s
}

func nilIfEmptyU32(s []uint32) []uint32 {
	if len(s) == 0 {
		return nil
	}
	return s
}
