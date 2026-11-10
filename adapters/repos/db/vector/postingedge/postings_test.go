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
	"errors"
	"math"
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
)

func TestSlotLayoutGroupsVectorsByPosting(t *testing.T) {
	tests := []struct {
		name       string
		primary    []uint32
		postings   int
		wantStart  []uint32
		wantOrder  []uint32
		wantSlotOf []uint32
	}{
		{
			name:       "already grouped",
			primary:    []uint32{0, 0, 1, 2},
			postings:   3,
			wantStart:  []uint32{0, 2, 3, 4},
			wantOrder:  []uint32{0, 1, 2, 3},
			wantSlotOf: []uint32{0, 1, 2, 3},
		},
		{
			name:       "interleaved, input order kept within a posting",
			primary:    []uint32{2, 0, 2, 1, 0},
			postings:   3,
			wantStart:  []uint32{0, 2, 3, 5},
			wantOrder:  []uint32{1, 4, 3, 0, 2},
			wantSlotOf: []uint32{3, 0, 4, 2, 1},
		},
		{
			name:       "a posting without vectors owns an empty range",
			primary:    []uint32{2, 0},
			postings:   3,
			wantStart:  []uint32{0, 1, 1, 2},
			wantOrder:  []uint32{1, 0},
			wantSlotOf: []uint32{1, 0},
		},
		{
			name:       "a single posting",
			primary:    []uint32{0, 0, 0},
			postings:   1,
			wantStart:  []uint32{0, 3},
			wantOrder:  []uint32{0, 1, 2},
			wantSlotOf: []uint32{0, 1, 2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			layout := newSlotLayout(tt.primary, tt.postings)
			assert.Equal(t, tt.wantStart, layout.start)
			assert.Equal(t, tt.wantOrder, layout.order)
			assert.Equal(t, tt.wantSlotOf, layout.slotOf)

			postingOf := postingOfSlots(layout.start)
			require.Len(t, postingOf, len(tt.primary))
			for input, p := range tt.primary {
				assert.Equalf(t, p, postingOf[layout.slotOf[input]],
					"the slot of input %d must fall in its posting", input)
			}
		})
	}
}

func TestPostingStoreLoadsOnePostingAtATime(t *testing.T) {
	// 3 postings of 2, 0 and 3 members, codes of 2 words.
	store := &postingStore{
		codeWords: 2,
		start:     []uint32{0, 2, 2, 5},
		docIDs:    []uint64{10, 11, 12, 13, 14},
		codes:     []uint64{100, 101, 110, 111, 120, 121, 130, 131, 140, 141},
		edgeStart: []uint32{0, 1, 3, 3, 4, 6},
		edges:     []uint32{2, 3, 4, 0, 0, 1},
	}
	side := &sidecar{codeWords: 2, codes: store.codes, postingOf: postingOfSlots(store.start)}

	require.Equal(t, 3, store.postings())
	require.Equal(t, 5, store.vectors())

	first := store.load(0)
	assert.Equal(t, uint32(0), first.first)
	assert.Equal(t, []uint64{10, 11}, first.docIDs)
	assert.Equal(t, []uint64{100, 101, 110, 111}, first.codes)
	assert.Equal(t, []uint32{2, 3, 4}, first.edges)

	empty := store.load(1)
	assert.Empty(t, empty.docIDs)
	assert.Empty(t, empty.codes)
	assert.Empty(t, empty.edges)

	last := store.load(2)
	assert.Equal(t, uint32(2), last.first)
	assert.Equal(t, []uint64{12, 13, 14}, last.docIDs)
	assert.Equal(t, []uint64{120, 121, 130, 131, 140, 141}, last.codes)
	assert.Equal(t, []uint32{0, 0, 1}, last.edges)

	assert.Equal(t, []uint32{3, 4}, store.edgesOf(1))
	assert.Empty(t, store.edgesOf(2))

	// A loaded posting hands out the edges of each of its members, wherever
	// the posting sits in the store.
	assert.Equal(t, []uint32{2}, first.edgesOf(0))
	assert.Equal(t, []uint32{3, 4}, first.edgesOf(1))
	assert.Empty(t, last.edgesOf(0))
	assert.Equal(t, []uint32{0}, last.edgesOf(1))
	assert.Equal(t, []uint32{0, 1}, last.edgesOf(2))

	// The sidecar serves the very same codes by slot, and cannot be
	// appended through into the next code.
	assert.Equal(t, []uint64{130, 131}, side.code(3))
	assert.Equal(t, 2, cap(side.code(3)))
	assert.Equal(t, []uint32{0, 0, 2, 2, 2}, side.postingOf)
}

// Scoring a batch of slots must give, bit for bit, what scoring them one by
// one gives: the batch only changes when the codes are fetched.
func TestSidecarScoreMatchesScoringOneByOne(t *testing.T) {
	tests := []struct {
		name     string
		dims     int
		provider distancer.Provider
	}{
		{name: "1536 dimensions, cosine", dims: 1536, provider: distancer.NewCosineDistanceProvider()},
		{name: "1536 dimensions, l2", dims: 1536, provider: distancer.NewL2SquaredProvider()},
		{name: "a code of one cache line", dims: 384, provider: distancer.NewCosineDistanceProvider()},
		{name: "dimensions padded to the minimum code", dims: 24, provider: distancer.NewL2SquaredProvider()},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const n = 300
			vectors := clusteredVectors(n+1, tt.dims, 3, 401)
			query, vectors := vectors[n], vectors[:n]
			if tt.provider.Type() == "cosine-dot" {
				query, vectors = distancer.Normalize(query), normalizedCopy(vectors)
			}
			cfg := testConfig(t, vectors, 0, tt.provider)
			quantizer, err := trainQuantizer(vectors, tt.dims, 100, &cfg)
			require.NoError(t, err)

			side := &sidecar{codeWords: len(quantizer.Encode(vectors[0]))}
			for _, vec := range vectors {
				side.codes = append(side.codes, quantizer.Encode(vec)...)
			}
			estimator := quantizer.NewDistancer(query)

			rng := rand.New(rand.NewPCG(5, 9))
			random := func(count int) []uint32 {
				slots := make([]uint32, count)
				for i := range slots {
					slots[i] = uint32(rng.IntN(n))
				}
				return slots
			}
			batches := map[string][]uint32{
				"empty":                          {},
				"a single slot":                  {7},
				"fewer slots than the lookahead": random(scoreAhead - 1),
				"exactly the lookahead":          random(scoreAhead),
				"one more than the lookahead":    random(scoreAhead + 1),
				"many slots":                     random(1000),
				"the first and the last slot":    {0, n - 1, 0, n - 1},
				"the same slot over and over":    {42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42},
			}
			for name, slots := range batches {
				dists := make([]float32, len(slots))
				require.NoError(t, side.score(estimator, slots, dists), name)
				for i, slot := range slots {
					want, err := estimator.Distance(side.code(slot))
					require.NoError(t, err)
					require.Equalf(t, math.Float32bits(want), math.Float32bits(dists[i]),
						"%s: slot %d at position %d: %v != %v", name, slot, i, dists[i], want)
				}
			}
		})
	}
}

// failingEstimator fails on the code whose first word it was told to.
type failingEstimator struct {
	failOn uint64
}

func (f failingEstimator) Distance(code []uint64) (float32, error) {
	if code[0] == f.failOn {
		return 0, errors.New("broken code")
	}
	return float32(code[0]), nil
}

func TestSidecarScoreReportsTheSlotThatFailed(t *testing.T) {
	side := &sidecar{codeWords: 2, codes: []uint64{10, 0, 11, 0, 12, 0, 13, 0}}
	dists := make([]float32, 3)

	require.NoError(t, side.score(failingEstimator{failOn: 99}, []uint32{3, 0, 2}, dists))
	assert.Equal(t, []float32{13, 10, 12}, dists)

	err := side.score(failingEstimator{failOn: 12}, []uint32{3, 2, 0}, dists)
	require.ErrorContains(t, err, "slot 2")
}

// BenchmarkSidecarScore scores random slots of a sidecar that does not fit
// the CPU caches, the situation of a search on a large index: one code at a
// time, and as a batch.
func BenchmarkSidecarScore(b *testing.B) {
	const dims, n, batch = 1536, 400_000, 1024
	provider := distancer.NewCosineDistanceProvider()
	rng := rand.New(rand.NewPCG(1, 2))
	query := distancer.Normalize(clusteredVectors(1, dims, 1, 7)[0])
	quantizer, err := compressionhelpers.NewCenteredBinaryRotationalQuantizer(
		dims, compressionhelpers.DefaultFastRotationSeed, make([]float32, dims), provider)
	require.NoError(b, err)

	// Random bits behind a real header: what the estimator computes is
	// meaningless, what it costs is not.
	header := quantizer.Encode(query)[0]
	side := &sidecar{codeWords: 1 + dims/64}
	side.codes = make([]uint64, n*side.codeWords)
	for i := range side.codes {
		side.codes[i] = rng.Uint64()
		if i%side.codeWords == 0 {
			side.codes[i] = header
		}
	}
	estimator := quantizer.NewDistancer(query)
	slots := make([]uint32, batch)
	dists := make([]float32, batch)

	run := func(name string, score func()) {
		b.Run(name, func(b *testing.B) {
			b.StopTimer()
			for done := 0; done < b.N; done += batch {
				for i := range slots {
					slots[i] = uint32(rng.IntN(n))
				}
				b.StartTimer()
				score()
				b.StopTimer()
			}
		})
	}
	run("one by one", func() {
		for i, slot := range slots {
			dists[i], _ = estimator.Distance(side.code(slot))
		}
	})
	run("batch", func() {
		_ = side.score(estimator, slots, dists)
	})
}

func TestTargetPostingSize(t *testing.T) {
	tests := []struct {
		name       string
		configured int
		codeBytes  int
		want       int
	}{
		{name: "an explicit size wins", configured: 50, codeBytes: 200, want: 50},
		{name: "1536 dimensions fill the budget with ~236 vectors", configured: 0, codeBytes: 200, want: 236},
		{name: "768 dimensions", configured: 0, codeBytes: 104, want: 438},
		{name: "a code larger than the budget still yields a posting", configured: 0, codeBytes: 1 << 20, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, targetPostingSize(tt.configured, tt.codeBytes))
		})
	}
}
