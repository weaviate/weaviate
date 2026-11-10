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
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPartitionClustersCoverEveryVectorOnce(t *testing.T) {
	logger, _ := test.NewNullLogger()
	identical := make([][]float32, 97)
	for i := range identical {
		identical[i] = []float32{1, 2, 3, 4}
	}

	tests := []struct {
		name    string
		vectors [][]float32
		k       int
		want    int
	}{
		{name: "one cluster", vectors: clusteredVectors(200, 8, 5, 1), k: 1, want: 1},
		{name: "two clusters", vectors: clusteredVectors(200, 8, 5, 1), k: 2, want: 2},
		{name: "a power of two", vectors: clusteredVectors(1000, 8, 12, 2), k: 16, want: 16},
		{name: "not a power of two", vectors: clusteredVectors(1000, 8, 12, 3), k: 37, want: 37},
		{name: "one vector per cluster", vectors: clusteredVectors(64, 8, 4, 4), k: 64, want: 64},
		{name: "more clusters than vectors", vectors: clusteredVectors(10, 8, 2, 5), k: 50, want: 10},
		{name: "zero clusters means one", vectors: clusteredVectors(10, 8, 2, 5), k: 0, want: 1},
		{name: "a single vector", vectors: clusteredVectors(1, 8, 1, 6), k: 3, want: 1},
		{name: "identical vectors still split", vectors: identical, k: 9, want: 9},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := len(tt.vectors)
			clusters, err := partitionClusters(context.Background(), logger, tt.vectors, len(tt.vectors[0]), tt.k, 7)
			require.NoError(t, err)
			require.Len(t, clusters, tt.want)

			seen := make([]int, n)
			largest := 0
			for _, c := range clusters {
				require.NotEmpty(t, c.members, "no cluster may be empty")
				largest = max(largest, len(c.members))
				for _, m := range c.members {
					seen[m]++
				}
			}
			for i, count := range seen {
				require.Equalf(t, 1, count, "vector %d must be in exactly one cluster", i)
			}
			// Balanced splits keep the sizes near n/k: no cluster runs
			// away with the corpus.
			assert.LessOrEqual(t, largest, 4*((n+tt.want-1)/tt.want))

			centroids, err := partition(context.Background(), logger, tt.vectors, len(tt.vectors[0]), tt.k, 7)
			require.NoError(t, err)
			require.Len(t, centroids, tt.want)
			for _, centroid := range centroids {
				require.True(t, isFinite(centroid))
			}
		})
	}
}

func TestPartitionIsDeterministicForASeed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	vectors := clusteredVectors(2000, 16, 20, 9)

	first, err := partition(context.Background(), logger, vectors, 16, 40, 11)
	require.NoError(t, err)
	second, err := partition(context.Background(), logger, vectors, 16, 40, 11)
	require.NoError(t, err)
	assert.Equal(t, first, second, "the same seed must give the same centroids")

	other, err := partition(context.Background(), logger, vectors, 16, 40, 12)
	require.NoError(t, err)
	assert.NotEqual(t, first, other, "the seed must actually drive the splits")
}

func TestPartitionCentroidIsTheClusterMean(t *testing.T) {
	logger, _ := test.NewNullLogger()
	vectors := [][]float32{{0, 0}, {2, 4}, {4, 2}}

	centroids, err := partition(context.Background(), logger, vectors, 2, 1, 1)
	require.NoError(t, err)
	require.Len(t, centroids, 1)
	assert.InDeltaSlice(t, []float32{2, 2}, centroids[0], 1e-6)
}

func TestPartitionRejectsEmptyInput(t *testing.T) {
	logger, _ := test.NewNullLogger()
	_, err := partition(context.Background(), logger, nil, 4, 3, 1)
	require.Error(t, err)
}

func TestPartitionStopsOnCancelledContext(t *testing.T) {
	logger, _ := test.NewNullLogger()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := partition(ctx, logger, clusteredVectors(500, 8, 5, 1), 8, 10, 1)
	require.ErrorIs(t, err, context.Canceled)
}

// splitLeaves must hand both children at least one leaf and never more
// leaves than they have vectors, for every split bisect can produce.
func TestSplitLeavesKeepsBothSidesFeasible(t *testing.T) {
	for n := 2; n <= 60; n++ {
		for nLeft := 1; nLeft < n; nLeft++ {
			nRight := n - nLeft
			for leaves := 2; leaves <= n; leaves++ {
				kLeft := splitLeaves(leaves, nLeft, nRight)
				kRight := leaves - kLeft
				if kLeft < 1 || kRight < 1 || kLeft > nLeft || kRight > nRight {
					t.Fatalf("splitLeaves(%d, %d, %d) = %d leaves left, %d right",
						leaves, nLeft, nRight, kLeft, kRight)
				}
			}
		}
	}
}
