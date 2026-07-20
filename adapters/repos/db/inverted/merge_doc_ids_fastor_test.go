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
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/sroar"
	"github.com/weaviate/weaviate/entities/filters"
)

func allowListDBM(ids ...uint64) *docBitmap {
	bm := sroar.NewBitmap()
	bm.SetMany(ids)
	return &docBitmap{docIDs: bm, isDenyList: false, release: func() {}}
}

func denyListDBM(ids ...uint64) *docBitmap {
	bm := sroar.NewBitmap()
	bm.SetMany(ids)
	return &docBitmap{docIDs: bm, isDenyList: true, release: func() {}}
}

// pairwiseOrReference reimplements the pre-FastOr mergeDocIDs algorithm as
// an independent oracle for the FastOr path.
func pairwiseOrReference(dbms []*docBitmap, maxConc int) []uint64 {
	cp := make([]*docBitmap, len(dbms))
	for i, dbm := range dbms {
		bm := sroar.NewBitmap()
		bm.SetMany(dbm.docIDs.ToArray())
		cp[i] = &docBitmap{docIDs: bm, isDenyList: dbm.isDenyList, release: func() {}}
	}
	for i := 0; i < len(cp)-1; i++ {
		cp[0] = mergeBitmapsAndOrWithDenyList(cp[0], cp[i+1], filters.OperatorOr, maxConc)
	}
	out := cp[0].docIDs.ToArray()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestMergeDocIDs_FastOrPath_MatchesPairwiseReference checks the FastOr
// path's output, key for key, against an independent reimplementation of
// the pairwise algorithm it replaces.
func TestMergeDocIDs_FastOrPath_MatchesPairwiseReference(t *testing.T) {
	rng := rand.New(rand.NewSource(42))

	for _, numPartials := range []int{2, 3, 16, 32} {
		t.Run(fmt.Sprintf("numPartials=%d", numPartials), func(t *testing.T) {
			var fastInput []*docBitmap
			var refInput []*docBitmap
			for i := 0; i < numPartials; i++ {
				ids := make([]uint64, 0, 50)
				for j := 0; j < 50; j++ {
					ids = append(ids, uint64(rng.Intn(500)))
				}
				fastInput = append(fastInput, allowListDBM(ids...))
				refInput = append(refInput, allowListDBM(ids...))
			}

			want := pairwiseOrReference(refInput, 4)

			merged := mergeDocIDs(filters.OperatorOr, fastInput, 4)
			require.Len(t, merged, 1)
			got := merged[0].docIDs.ToArray()
			sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })

			require.Equal(t, want, got,
				"FastOr-merged allowlist union must exactly match the pairwise-OrConc "+
					"reference algorithm it replaces")
		})
	}
}

// TestMergeDocIDs_DenyListPresent_SkipsFastOrPath proves the FastOr path is
// never taken when any input is a deny-list.
func TestMergeDocIDs_DenyListPresent_SkipsFastOrPath(t *testing.T) {
	deny := denyListDBM(1, 2, 3, 4, 5) // excludes [1,5]
	allow1 := allowListDBM(2)          // reclaims doc 2
	allow2 := allowListDBM(4)          // reclaims doc 4

	merged := mergeDocIDs(filters.OperatorOr, []*docBitmap{deny, allow1, allow2}, 4)
	require.Len(t, merged, 1)
	require.True(t, merged[0].IsDenyList(),
		"a batch containing a deny-list input must still produce a deny-list result -- "+
			"if FastOr's plain-union path were mistakenly taken here, the result would "+
			"come back as an (incorrect) allowlist instead")

	excluded := merged[0].docIDs.ToArray()
	sort.Slice(excluded, func(i, j int) bool { return excluded[i] < excluded[j] })
	require.Equal(t, []uint64{1, 3, 5}, excluded,
		"the merged deny-list must still exclude exactly {1,3,5} -- docs 2 and 4 were "+
			"reclaimed by the allowlist operands and must no longer be excluded")
}

// TestMergeDocIDs_AndOperator_SkipsFastOrPath proves OperatorAnd never
// routes through fastOrMerge, since FastOr is a union-only primitive with
// no intersection semantics.
func TestMergeDocIDs_AndOperator_SkipsFastOrPath(t *testing.T) {
	a := allowListDBM(1, 2, 3, 4)
	b := allowListDBM(2, 3, 4, 5)
	c := allowListDBM(3, 4, 5, 6)

	merged := mergeDocIDs(filters.OperatorAnd, []*docBitmap{a, b, c}, 4)
	require.Len(t, merged, 1)
	got := merged[0].docIDs.ToArray()
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	require.Equal(t, []uint64{3, 4}, got, "AND of the three sets must be exactly their intersection")
}

// BenchmarkMergeDocIDs_FastOrVsPairwise compares FastOr against the pairwise
// reduce at the partial counts (16, 32) typical post-chunking.
func BenchmarkMergeDocIDs_FastOrVsPairwise(b *testing.B) {
	rng := rand.New(rand.NewSource(7))

	for _, numPartials := range []int{16, 32} {
		partials := make([][]uint64, numPartials)
		for i := range partials {
			ids := make([]uint64, 0, 3000)
			for j := 0; j < 3000; j++ {
				ids = append(ids, uint64(rng.Intn(100000)))
			}
			partials[i] = ids
		}

		b.Run(fmt.Sprintf("FastOr/numPartials=%d", numPartials), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dbms := make([]*docBitmap, numPartials)
				for j, ids := range partials {
					dbms[j] = allowListDBM(ids...)
				}
				b.StartTimer()
				_ = mergeDocIDs(filters.OperatorOr, dbms, 4)
			}
		})

		b.Run(fmt.Sprintf("Pairwise/numPartials=%d", numPartials), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				dbms := make([]*docBitmap, numPartials)
				for j, ids := range partials {
					dbms[j] = allowListDBM(ids...)
				}
				b.StartTimer()
				for k := 0; k < len(dbms)-1; k++ {
					dbms[0] = mergeBitmapsAndOrWithDenyList(dbms[0], dbms[k+1], filters.OperatorOr, 4)
				}
			}
		})
	}
}
