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
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

// sharingIndex is an unbuilt index over the vectors that shares its partition
// and assignment through share.
func sharingIndex(t *testing.T, vectors [][]float32, uc UserConfig, share *buildShare) *Index {
	t.Helper()
	index, err := New(testConfig(t, vectors, 0, distancer.NewCosineDistanceProvider()), uc)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, index.Shutdown(context.Background()))
	})
	index.shared = share
	require.NoError(t, index.AddBatch(context.Background(), sequentialIDs(len(vectors), 0), vectors))
	return index
}

// A build that takes the partition and the assignment of another must end in
// the index it would have built by itself.
func TestBuildTakesThePartitionOfAnotherBuild(t *testing.T) {
	ctx := context.Background()
	vectors := clusteredVectors(3000, 128, 30, 77)
	uc := smallConfig()

	alone := buildTestIndex(t, vectors, distancer.NewCosineDistanceProvider(), uc)

	share := &buildShare{}
	first := sharingIndex(t, vectors, uc, share)
	require.False(t, share.filled())
	require.NoError(t, first.Build(ctx))
	require.True(t, share.filled(), "a build that finds nothing shared leaves its own")
	require.Equal(t, len(vectors), share.vectors)
	require.Equal(t, uc.BuildProbe, share.probe)
	primary := append([]uint32(nil), share.assigned.primary...)
	probes := append([]uint32(nil), share.assigned.probes...)

	// The second index differs in its codes, which neither the partition nor
	// the assignment depend on.
	capped := uc
	capped.CodeBits = 64
	second := sharingIndex(t, vectors, capped, share)
	require.NoError(t, second.Build(ctx))
	require.Equal(t, primary, share.assigned.primary, "taking what is shared must not change it")
	require.Equal(t, probes, share.assigned.probes, "taking what is shared must not change it")
	stats, _ := second.BuildStats()
	require.Zero(t, stats.PartitionTime)
	require.Zero(t, stats.AssignTime)

	cappedAlone := buildTestIndex(t, vectors, distancer.NewCosineDistanceProvider(), capped)

	// A third one probes far fewer centroids: it reads the head of the
	// probes that were computed for the wider build.
	narrow := uc
	narrow.BuildProbe = 2
	third := sharingIndex(t, vectors, narrow, share)
	require.NoError(t, third.Build(ctx))
	narrowAlone := buildTestIndex(t, vectors, distancer.NewCosineDistanceProvider(), narrow)
	require.False(t, slices.Equal(alone.state.Load().postings.edges, narrowAlone.state.Load().postings.edges),
		"the fixture must tell the two build probes apart")

	tests := []struct {
		name          string
		alone, shared *Index
	}{
		{name: "the build that left its partition", alone: alone, shared: first},
		{name: "the build that took it", alone: cappedAlone, shared: second},
		{name: "a build with a narrower probe", alone: narrowAlone, shared: third},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			want, got := tt.alone.state.Load(), tt.shared.state.Load()
			require.Equal(t, want.centroids.vectors, got.centroids.vectors)
			require.Equal(t, want.postings.start, got.postings.start)
			require.Equal(t, want.postings.docIDs, got.postings.docIDs)
			require.Equal(t, want.postings.codes, got.postings.codes)
			require.Equal(t, want.postings.edgeStart, got.postings.edgeStart)
			require.Equal(t, want.postings.edges, got.postings.edges)
			require.Equal(t, want.side.postingOf, got.side.postingOf)
		})
	}
}

// What is shared is of one corpus size and one number of postings, and has
// probes of one width. A build of another shape, or one that probes wider,
// must fail rather than build on it.
func TestBuildRejectsAPartitionOfAnotherShape(t *testing.T) {
	ctx := context.Background()
	vectors := clusteredVectors(1200, 64, 12, 78)
	uc := smallConfig()

	share := &buildShare{}
	require.NoError(t, sharingIndex(t, vectors, uc, share).Build(ctx))

	tests := []struct {
		name    string
		vectors [][]float32
		change  func(uc *UserConfig)
		spoil   func(share *buildShare)
	}{
		{name: "fewer vectors", vectors: vectors[:1100]},
		{name: "another posting size", vectors: vectors, change: func(uc *UserConfig) { uc.TargetPostingSize = 60 }},
		{name: "a wider build probe", vectors: vectors, change: func(uc *UserConfig) { uc.BuildProbe = 2 * uc.BuildProbe }},
		{
			name: "an assignment cut short", vectors: vectors,
			spoil: func(share *buildShare) {
				cut := *share.assigned
				cut.probes = cut.probes[:len(cut.probes)-1]
				share.assigned = &cut
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			other, shared := uc, *share
			if tt.change != nil {
				tt.change(&other)
			}
			if tt.spoil != nil {
				tt.spoil(&shared)
			}
			err := sharingIndex(t, tt.vectors, other, &shared).Build(ctx)
			require.ErrorContains(t, err, "shared partition and assignment")
		})
	}
}
