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

package postingedge

import (
	"context"
	"math"
	"slices"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// The primary posting of a vector must be its exact nearest centroid, and
// its probes the exact nearest centroids: an approximate primary would hide
// the vector from the one posting a search expects it in.
func TestAssignIsExact(t *testing.T) {
	logger, _ := test.NewNullLogger()
	l2 := distancer.NewL2SquaredProvider()

	tests := []struct {
		name      string
		centroids int
		probe     int
		wantWidth int
	}{
		{name: "probe narrower than the centroids", centroids: 40, probe: 7, wantWidth: 7},
		{name: "probe of one", centroids: 40, probe: 1, wantWidth: 1},
		{name: "probe wider than the centroids", centroids: 5, probe: 64, wantWidth: 5},
		{name: "a single centroid", centroids: 1, probe: 3, wantWidth: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vectors := clusteredVectors(600, 12, 15, 21)
			centroids := clusteredVectors(tt.centroids, 12, 15, 22)

			a, err := assign(context.Background(), logger, vectors, centroids, tt.probe)
			require.NoError(t, err)
			require.Equal(t, tt.wantWidth, a.width)
			require.Len(t, a.primary, len(vectors))

			for i, vec := range vectors {
				want := exactNearest(t, centroids, vec, tt.wantWidth, -1, l2)
				require.Equalf(t, want[0].id, uint64(a.primary[i]), "primary of vector %d", i)

				wantIDs := make([]uint32, len(want))
				for j, w := range want {
					wantIDs[j] = uint32(w.id)
				}
				got := slices.Clone(a.probesOf(i))
				slices.Sort(got)
				slices.Sort(wantIDs)
				require.Equalf(t, wantIDs, got, "probes of vector %d", i)
				require.Containsf(t, got, a.primary[i], "the primary is always a probe (vector %d)", i)
			}
		})
	}
}

func TestAssignRejectsUnusableInput(t *testing.T) {
	logger, _ := test.NewNullLogger()
	nan := float32(math.NaN())

	t.Run("no centroids", func(t *testing.T) {
		_, err := assign(context.Background(), logger, [][]float32{{1, 2}}, nil, 4)
		require.Error(t, err)
	})

	t.Run("a vector without a finite distance to any centroid", func(t *testing.T) {
		_, err := assign(context.Background(), logger,
			[][]float32{{1, 2}, {nan, nan}}, [][]float32{{0, 0}, {5, 5}}, 2)
		require.ErrorContains(t, err, "no finite distance")
	})

	t.Run("mismatching dimensions", func(t *testing.T) {
		_, err := assign(context.Background(), logger,
			[][]float32{{1, 2, 3}}, [][]float32{{0, 0}}, 1)
		require.Error(t, err)
	})
}

func TestDropEmptyRenumbersTheAssignment(t *testing.T) {
	tests := []struct {
		name          string
		centroids     [][]float32
		primary       []uint32
		probes        []uint32
		width         int
		wantCentroids [][]float32
		wantPrimary   []uint32
		wantProbes    []uint32
	}{
		{
			name:          "nothing empty leaves everything as is",
			centroids:     [][]float32{{0}, {1}, {2}},
			primary:       []uint32{0, 1, 2, 1},
			probes:        []uint32{0, 1, 1, 2, 2, 1, 1, 0},
			width:         2,
			wantCentroids: [][]float32{{0}, {1}, {2}},
			wantPrimary:   []uint32{0, 1, 2, 1},
			wantProbes:    []uint32{0, 1, 1, 2, 2, 1, 1, 0},
		},
		{
			name:          "an empty centroid in the middle",
			centroids:     [][]float32{{0}, {1}, {2}},
			primary:       []uint32{0, 2, 2},
			probes:        []uint32{0, 1, 2, 1, 2, 0},
			width:         2,
			wantCentroids: [][]float32{{0}, {2}},
			wantPrimary:   []uint32{0, 1, 1},
			wantProbes:    []uint32{0, noCentroid, 1, noCentroid, 1, 0},
		},
		{
			name:          "empty centroids at both ends",
			centroids:     [][]float32{{0}, {1}, {2}, {3}},
			primary:       []uint32{1, 2, 1},
			probes:        []uint32{1, 0, 2, 3, 1, 2},
			width:         2,
			wantCentroids: [][]float32{{1}, {2}},
			wantPrimary:   []uint32{0, 1, 0},
			wantProbes:    []uint32{0, noCentroid, 1, noCentroid, 0, 1},
		},
		{
			name:          "an already dropped probe stays dropped",
			centroids:     [][]float32{{0}, {1}},
			primary:       []uint32{1},
			probes:        []uint32{1, noCentroid},
			width:         2,
			wantCentroids: [][]float32{{1}},
			wantPrimary:   []uint32{0},
			wantProbes:    []uint32{0, noCentroid},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &assignment{primary: tt.primary, probes: tt.probes, width: tt.width}
			got := dropEmpty(tt.centroids, a)
			assert.Equal(t, tt.wantCentroids, got)
			assert.Equal(t, tt.wantPrimary, a.primary)
			assert.Equal(t, tt.wantProbes, a.probes)
		})
	}
}
