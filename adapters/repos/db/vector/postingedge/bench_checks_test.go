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

//go:build postingedgebench

package postingedge

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMedian(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		want   float64
	}{
		{name: "one value", values: []float64{7}, want: 7},
		{name: "odd count, unsorted", values: []float64{9, 1, 5}, want: 5},
		{name: "even count is the mean of the two in the middle", values: []float64{10, 1, 3, 4}, want: 3.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			values := append([]float64(nil), tt.values...)
			require.InDelta(t, tt.want, median(values), 1e-12)
			require.Equal(t, tt.values, values, "the values are left in their order")
		})
	}
}

func TestOutworked(t *testing.T) {
	timed := func(threads, rescore, ef int, margin, qps float64) benchPoint {
		return benchPoint{Threads: threads, RescoreLimit: rescore, LoadBatch: DefaultLoadBatch, SourceMargin: margin, EF: ef, QPS: qps}
	}
	otherBatch := timed(8, 800, 640, 0.08, 900)
	otherBatch.LoadBatch = 1
	untimed := timed(8, 800, 640, 0.08, 900)
	untimed.Untimed = true

	// each wanted pair is the QPS of the slow point and of the busier one
	tests := []struct {
		name   string
		points []benchPoint
		want   [][2]float64
	}{
		{
			name:   "more rescoring gives less QPS",
			points: []benchPoint{timed(8, 400, 640, 0.08, 990), timed(8, 800, 640, 0.08, 914)},
		},
		{
			name:   "slower than the point that rescores more",
			points: []benchPoint{timed(8, 400, 640, 0.08, 894), timed(8, 800, 640, 0.08, 914)},
			want:   [][2]float64{{894, 914}},
		},
		{
			name:   "behind by less than the gap",
			points: []benchPoint{timed(8, 400, 640, 0.08, 900), timed(8, 800, 640, 0.08, 914)},
		},
		{
			name:   "set against the fastest of the busier points",
			points: []benchPoint{timed(1, 200, 80, 0.06, 600), timed(1, 400, 80, 0.06, 650), timed(1, 800, 80, 0.06, 700)},
			want:   [][2]float64{{600, 700}, {650, 700}},
		},
		{
			name: "another ef, margin, thread count or load batch is another search",
			points: []benchPoint{
				timed(8, 400, 640, 0.08, 800),
				timed(8, 800, 1024, 0.08, 900), timed(8, 800, 640, 0.06, 900), timed(1, 800, 640, 0.08, 900), otherBatch,
			},
		},
		{
			name:   "an untimed point has no QPS to compare",
			points: []benchPoint{timed(8, 400, 640, 0.08, 800), untimed},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got [][2]float64
			for _, o := range outworked(tt.points, 0.02) {
				require.Greater(t, o.busier.RescoreLimit, o.slow.RescoreLimit)
				got = append(got, [2]float64{o.slow.QPS, o.busier.QPS})
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestRepeatedPoints(t *testing.T) {
	efCurve := func(qpsByEF ...float64) []benchPoint {
		var points []benchPoint
		for i := 0; i < len(qpsByEF); i += 2 {
			points = append(points, benchPoint{EF: int(qpsByEF[i]), QPS: qpsByEF[i+1]})
		}
		return points
	}

	tests := []struct {
		name         string
		curve, other []benchPoint
		wantEFs      []int
		wantChanges  []float64
	}{
		{
			name:        "the same efs in both runs",
			curve:       efCurve(10, 1100, 20, 900),
			other:       efCurve(10, 1000, 20, 1000),
			wantEFs:     []int{10, 20},
			wantChanges: []float64{0.10, -0.10},
		},
		{
			name:        "an ef only one of the runs has is left out",
			curve:       efCurve(10, 1000, 20, 900, 2048, 50),
			other:       efCurve(20, 1000, 40, 700),
			wantEFs:     []int{20},
			wantChanges: []float64{-0.10},
		},
		{
			name:  "no other run of the curve",
			curve: efCurve(10, 1000),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pairs := repeatedPoints(tt.curve, tt.other)
			require.Len(t, pairs, len(tt.wantEFs))
			for i, pair := range pairs {
				require.Equal(t, tt.wantEFs[i], pair.point.EF)
				require.Equal(t, tt.wantEFs[i], pair.other.EF)
				require.InDelta(t, tt.wantChanges[i], pair.change(), 1e-9)
			}
		})
	}
}

func TestCoverageOf(t *testing.T) {
	tests := []struct {
		name     string
		start    []uint32
		docIDs   []uint64
		added    int
		want     coverage
		complete bool
	}{
		{
			name:  "every vector in one posting, one of the postings empty",
			start: []uint32{0, 2, 2, 5}, docIDs: []uint64{3, 0, 4, 1, 2}, added: 5,
			want: coverage{inPostings: 5}, complete: true,
		},
		{
			name:  "a vector that was added and is in no posting",
			start: []uint32{0, 2, 4}, docIDs: []uint64{0, 1, 2, 4}, added: 5,
			want: coverage{inPostings: 4, missing: 1},
		},
		{
			name:  "a slot that no posting owns",
			start: []uint32{0, 2, 3}, docIDs: []uint64{0, 1, 2, 3}, added: 4,
			want: coverage{inPostings: 3, missing: 1},
		},
		{
			name:  "a vector in two postings",
			start: []uint32{0, 2, 5}, docIDs: []uint64{0, 1, 2, 3, 1}, added: 4,
			want: coverage{inPostings: 5, repeated: 1},
		},
		{
			name:  "an id that was never added",
			start: []uint32{0, 3}, docIDs: []uint64{0, 1, 9}, added: 3,
			want: coverage{inPostings: 3, missing: 1, unknown: 1},
		},
		{
			// the sizes add up, so only the ids tell
			name:  "a repeat in the place of a missing vector",
			start: []uint32{0, 3}, docIDs: []uint64{0, 0, 1}, added: 3,
			want: coverage{inPostings: 3, missing: 1, repeated: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coverageOf(&postingStore{start: tt.start, docIDs: tt.docIDs}, tt.added)
			require.Equal(t, tt.want, got)
			require.Equal(t, tt.complete, got.complete(tt.added))
		})
	}
}

func TestBuildShareSurvivesTheCache(t *testing.T) {
	share := &buildShare{
		vectors: 5, postings: 3, probe: 2,
		centroids: [][]float32{{0.5, -1, 2}, {3, 4.25, -0}},
		assigned: &assignment{
			primary: []uint32{0, 1, 1, 0, 1},
			probes:  []uint32{0, 1, 1, 0, 1, 0, 0, 1, 1, noCentroid},
			width:   2,
		},
	}

	t.Run("an empty directory gives a share to fill in", func(t *testing.T) {
		got, err := loadBuildShare(t.TempDir(), "train A")
		require.NoError(t, err)
		require.False(t, got.filled())
	})

	t.Run("what was saved is what is loaded", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "nested", "cache")
		require.NoError(t, saveBuildShare(dir, "train A", share))
		got, err := loadBuildShare(dir, "train A")
		require.NoError(t, err)
		require.Equal(t, share, got)
		require.NoError(t, got.fits(5, 3, 2))
	})

	broken := []struct {
		name  string
		train string
		spoil func(t *testing.T, dir string)
	}{
		{name: "computed over another train set", train: "train B"},
		{
			name: "a probes file cut short", train: "train A",
			spoil: func(t *testing.T, dir string) {
				require.NoError(t, os.Truncate(filepath.Join(dir, "probes.u32"), 36))
			},
		},
		{
			name: "a missing centroid file", train: "train A",
			spoil: func(t *testing.T, dir string) {
				require.NoError(t, os.Remove(filepath.Join(dir, "centroids.f32")))
			},
		},
		{
			name: "a vector assigned to a centroid that is not there", train: "train A",
			spoil: func(t *testing.T, dir string) {
				require.NoError(t, os.WriteFile(filepath.Join(dir, "primary.u32"),
					[]byte{0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0}, 0o644))
			},
		},
	}
	for _, tt := range broken {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			require.NoError(t, saveBuildShare(dir, "train A", share))
			if tt.spoil != nil {
				tt.spoil(t, dir)
			}
			_, err := loadBuildShare(dir, tt.train)
			require.Error(t, err)
		})
	}
}

// The edge recall of a cap is against the k nearest true edges of each
// sampled vector: the edges a cap is meant to keep, and not the ones it drops
// by design.
func TestEdgeRecallOfNearest(t *testing.T) {
	// Two sampled slots, their true edges nearest first.
	truth := &edgeTruth{
		slots:  []uint32{0, 1},
		wanted: [][]uint32{{10, 11, 12, 13}, {20, 21}},
	}
	tests := []struct {
		name  string
		edges map[uint32][]uint32
		k     int
		want  float64
	}{
		{name: "every true edge, no cap", edges: map[uint32][]uint32{0: {10, 11, 12, 13}, 1: {20, 21}}, k: 0, want: 1},
		{name: "a cap of two holding the two nearest", edges: map[uint32][]uint32{0: {10, 11}, 1: {20, 21}}, k: 2, want: 1},
		{name: "the same edges against every true edge", edges: map[uint32][]uint32{0: {10, 11}, 1: {20, 21}}, k: 0, want: 4.0 / 6},
		{name: "a cap of two holding a farther edge", edges: map[uint32][]uint32{0: {10, 13}, 1: {20, 21}}, k: 2, want: 3.0 / 4},
		{name: "a cap above the true edges of a slot", edges: map[uint32][]uint32{0: {10, 11, 12}, 1: {21}}, k: 3, want: 4.0 / 5},
		{name: "edges that are not true edges", edges: map[uint32][]uint32{0: {30, 31}, 1: {40}}, k: 2, want: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truth.recallOfNearest(func(slot uint32) []uint32 { return tt.edges[slot] }, tt.k)
			require.InDelta(t, tt.want, got, 1e-12)
		})
	}
	require.Equal(t, truth.recallOfNearest(func(slot uint32) []uint32 { return []uint32{10, 20} }, 0),
		truth.recall(func(slot uint32) []uint32 { return []uint32{10, 20} }), "recall is the uncapped recall")
}
