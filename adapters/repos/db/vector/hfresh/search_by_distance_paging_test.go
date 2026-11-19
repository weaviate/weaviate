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

package hfresh

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enterrors "github.com/weaviate/weaviate/entities/errors"
)

// A search by distance asks for a first page of results and, while the page
// comes back full and within the distance, for a larger one. The loop that
// asked for the larger page never searched again: a search by distance
// returned its first page (100 results) and no more, and with no maximum (the
// -1 an aggregation without an object limit passes) it never returned.
//
// The index holds fewer documents than the rescore limit, so every search
// ranks all of them by their exact distance and two searches of the same
// index agree: the search by distance is checked against a search by vector
// that asks for everything at once.
func TestSearchByDistanceGoesPastItsFirstPage(t *testing.T) {
	const (
		docs = 300
		dim  = 32
		page = common.DefaultSearchByDistInitialLimit
	)

	logger, _ := test.NewNullLogger()

	type search func(distance float32, maxLimit int64) ([]uint64, []float32, error)

	// run checks a search by distance against the ids and distances of the
	// search that asks for everything.
	run := func(t *testing.T, byDistance search, all []uint64, allDists []float32) {
		t.Helper()
		require.Greater(t, len(all), 2*page-20, "the index has to answer with more than a page")
		// between is a distance that takes in the first n results and no
		// other, as long as the two around it are not the same distance.
		between := func(n int) float32 {
			require.Less(t, allDists[n-1], allDists[n], "results %d and %d are at one distance", n-1, n)
			return (allDists[n-1] + allDists[n]) / 2
		}

		tests := []struct {
			name     string
			distance float32
			maxLimit int64
			want     int
		}{
			{name: "less than a page", distance: between(50), maxLimit: 10_000, want: 50},
			{name: "the distance ends in the second page", distance: between(180), maxLimit: 10_000, want: 180},
			{name: "every document, with a maximum", distance: math.MaxFloat32, maxLimit: 10_000, want: len(all)},
			{name: "every document, without a maximum", distance: math.MaxFloat32, maxLimit: -1, want: len(all)},
			{name: "the distance ends in the second page, without a maximum", distance: between(180), maxLimit: -1, want: 180},
			// The maximum is checked before a larger page is asked for: the
			// second page would be of 1100, over the maximum, so the first
			// is what comes back. HNSW does the same.
			{name: "the next page is over the maximum", distance: math.MaxFloat32, maxLimit: 150, want: page},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				var (
					ids   []uint64
					dists []float32
					err   error
				)
				done := make(chan struct{})
				enterrors.GoWrapper(func() {
					defer close(done)
					ids, dists, err = byDistance(tt.distance, tt.maxLimit)
				}, logger)
				select {
				case <-done:
				case <-time.After(30 * time.Second):
					t.Fatal("the search by distance did not return")
				}

				require.NoError(t, err)
				require.Equal(t, all[:tt.want], ids)
				require.Len(t, dists, tt.want)
			})
		}
	}

	t.Run("single vector", func(t *testing.T) {
		tf, probe := populateRandomSingleVectorIndex(t, docs, dim, 42)
		tf.Index.waitForMaintenance(t)

		all, allDists, err := tf.Index.SearchByVector(t.Context(), probe, docs+page, nil)
		require.NoError(t, err)
		run(t, func(distance float32, maxLimit int64) ([]uint64, []float32, error) {
			return tf.Index.SearchByVectorDistance(t.Context(), probe, distance, maxLimit, nil)
		}, all, allDists)
	})

	t.Run("multi vector", func(t *testing.T) {
		tf := createMuveraHFreshIndex(t, withDistanceProvider(distancer.NewCosineDistanceProvider()))
		rng := rand.New(rand.NewSource(42))
		for i := range docs {
			addMultiVectorToIndex(t, &tf, uint64(i), randomMultiVector(rng, 2, dim))
		}
		tf.Index.waitForMaintenance(t)
		probe := randomMultiVector(rng, 2, dim)

		all, allDists, err := tf.Index.SearchByMultiVector(t.Context(), probe, docs+page, nil)
		require.NoError(t, err)
		run(t, func(distance float32, maxLimit int64) ([]uint64, []float32, error) {
			return tf.Index.SearchByMultiVectorDistance(t.Context(), probe, distance, maxLimit, nil)
		}, all, allDists)
	})
}
