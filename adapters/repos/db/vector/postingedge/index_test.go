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
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/common"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestNewValidatesItsConfig(t *testing.T) {
	vectors := clusteredVectors(10, 4, 1, 1)
	valid := func() Config { return testConfig(t, vectors, 0, distancer.NewL2SquaredProvider()) }

	tests := []struct {
		name    string
		cfg     func() Config
		uc      func() UserConfig
		wantErr string
	}{
		{name: "defaults", cfg: valid, uc: NewDefaultUserConfig},
		{
			name: "cosine",
			cfg: func() Config {
				return testConfig(t, vectors, 0, distancer.NewCosineDistanceProvider())
			},
			uc: NewDefaultUserConfig,
		},
		{
			name:    "missing id",
			cfg:     func() Config { c := valid(); c.ID = ""; return c },
			uc:      NewDefaultUserConfig,
			wantErr: "id cannot be empty",
		},
		{
			name:    "missing root path",
			cfg:     func() Config { c := valid(); c.RootPath = ""; return c },
			uc:      NewDefaultUserConfig,
			wantErr: "rootPath cannot be empty",
		},
		{
			name:    "missing distance provider",
			cfg:     func() Config { c := valid(); c.DistanceProvider = nil; return c },
			uc:      NewDefaultUserConfig,
			wantErr: "distanceProvider cannot be nil",
		},
		{
			name:    "missing vector thunk",
			cfg:     func() Config { c := valid(); c.TempVectorForIDWithViewThunk = nil; return c },
			uc:      NewDefaultUserConfig,
			wantErr: "tempVectorForIDWithViewThunk cannot be nil",
		},
		{
			name:    "missing view thunk",
			cfg:     func() Config { c := valid(); c.GetViewThunk = nil; return c },
			uc:      NewDefaultUserConfig,
			wantErr: "getViewThunk cannot be nil",
		},
		{
			name: "a distance the partitioning does not agree with",
			cfg: func() Config {
				c := valid()
				c.DistanceProvider = distancer.NewDotProductProvider()
				return c
			},
			uc:      NewDefaultUserConfig,
			wantErr: "is not supported",
		},
		{
			name:    "no edges",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.Edges = 0; return u },
			wantErr: "edges must be at least 1",
		},
		{
			name:    "a rescue limit below the edges",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.RescueLimit = 4; return u },
			wantErr: "rescueLimit",
		},
		{
			name:    "no entry postings",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.EntryPostings = 0; return u },
			wantErr: "entryPostings",
		},
		{
			name:    "an empty load batch",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.LoadBatch = 0; return u },
			wantErr: "loadBatch",
		},
		{
			name:    "a negative ef",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.EF = -1; return u },
			wantErr: "ef must not be negative",
		},
		{
			name:    "a negative rescore limit",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.RescoreLimit = -1; return u },
			wantErr: "rescoreLimit",
		},
		{
			name:    "a negative source margin",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.SourceMargin = -0.1; return u },
			wantErr: "sourceMargin",
		},
		{
			name: "a source margin that is not a number",
			cfg:  valid,
			uc: func() UserConfig {
				u := NewDefaultUserConfig()
				u.SourceMargin = float32(math.NaN())
				return u
			},
			wantErr: "sourceMargin",
		},
		{
			name:    "a negative posting size",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.TargetPostingSize = -5; return u },
			wantErr: "targetPostingSize",
		},
		{
			name:    "no build probe",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.BuildProbe = 0; return u },
			wantErr: "buildProbe",
		},
		{
			name:    "no training sample",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.TrainingLimit = 0; return u },
			wantErr: "trainingLimit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index, err := New(tt.cfg(), tt.uc())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			require.NoError(t, index.Shutdown(context.Background()))
		})
	}
}

// The defaults are measured choices, and the default source margin follows
// the distance: a margin is in the units of the distance, so the one
// calibrated for cosine must not leak into an l2 index.
func TestDefaults(t *testing.T) {
	uc := NewDefaultUserConfig()
	require.NoError(t, uc.Validate())
	assert.Equal(t, 20, uc.EF)
	assert.Equal(t, 64, uc.RescoreLimit)
	assert.Equal(t, 256, uc.BuildProbe)
	assert.Equal(t, float32(SourceMarginAuto), uc.SourceMargin)

	vectors := clusteredVectors(10, 4, 1, 1)
	tests := []struct {
		name       string
		provider   distancer.Provider
		margin     float32
		wantMargin float32
	}{
		{name: "cosine takes its calibrated margin", provider: distancer.NewCosineDistanceProvider(), margin: SourceMarginAuto, wantMargin: 0.06},
		{name: "l2 has none", provider: distancer.NewL2SquaredProvider(), margin: SourceMarginAuto, wantMargin: 0},
		{name: "an explicit zero switches the cut off for cosine", provider: distancer.NewCosineDistanceProvider(), margin: 0, wantMargin: 0},
		{name: "an explicit margin is kept for cosine", provider: distancer.NewCosineDistanceProvider(), margin: 0.1, wantMargin: 0.1},
		{name: "an explicit margin is kept for l2", provider: distancer.NewL2SquaredProvider(), margin: 2.5, wantMargin: 2.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := NewDefaultUserConfig()
			uc.SourceMargin = tt.margin
			index, err := New(testConfig(t, vectors, 0, tt.provider), uc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
			assert.Equal(t, tt.wantMargin, math.Float32frombits(index.sourceMargin.Load()))

			// And an update back to the default resolves the same way.
			explicit := uc
			explicit.SourceMargin = 0.5
			require.NoError(t, index.UpdateUserConfig(explicit, func() {}))
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			assert.Equal(t, tt.wantMargin, math.Float32frombits(index.sourceMargin.Load()))
		})
	}
}

func TestIndexDescribesItself(t *testing.T) {
	uc := NewDefaultUserConfig()
	assert.Equal(t, "postingedge", uc.IndexType())
	assert.Equal(t, "cosine", uc.DistanceName())
	assert.False(t, uc.IsMultiVector())

	vectors := clusteredVectors(200, 8, 3, 1)
	luc := smallConfig()
	luc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), luc)

	assert.Equal(t, common.IndexType("postingedge"), index.Type())
	assert.True(t, index.Compressed())
	assert.False(t, index.Multivector())
	assert.Equal(t, "rq", index.CompressionStats().CompressionType())
}

// The write path of this version: batches in, one build, then read-only.
func TestIndexLifecycle(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(600, 8, 6, 151)
	uc := smallConfig()
	uc.Distance = "l2-squared"

	index, err := New(testConfig(t, vectors, 1000, provider), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })

	_, ok := index.BuildStats()
	require.False(t, ok)
	_, ok = index.MemoryStats()
	require.False(t, ok)
	assert.Equal(t, "none", index.CompressionStats().CompressionType())

	// An empty index has nothing to build or to search.
	require.Error(t, index.Build(ctx))
	_, _, err = index.SearchByVector(ctx, vectors[0], 5, nil)
	require.ErrorIs(t, err, ErrNotBuilt)

	// Ids need not start at zero.
	ids := sequentialIDs(len(vectors), 1000)
	require.NoError(t, index.AddBatch(ctx, ids[:400], vectors[:400]))
	require.NoError(t, index.Add(ctx, ids[400], vectors[400]))
	require.NoError(t, index.AddBatch(ctx, ids[401:], vectors[401:]))

	assert.True(t, index.ContainsDoc(1000))
	assert.True(t, index.ContainsDoc(1599))
	assert.False(t, index.ContainsDoc(999))
	assert.False(t, index.ContainsDoc(1600))

	// Added but not built: still not searchable.
	_, _, err = index.SearchByVector(ctx, vectors[0], 5, nil)
	require.ErrorIs(t, err, ErrNotBuilt)

	require.NoError(t, index.Build(ctx))

	stats, ok := index.BuildStats()
	require.True(t, ok)
	assert.Equal(t, len(vectors), stats.Vectors)
	assert.Positive(t, stats.Postings)
	assert.Positive(t, stats.TotalTime)

	got, _, err := index.SearchByVector(ctx, vectors[7], 5, nil)
	require.NoError(t, err)
	require.Len(t, got, 5)
	assert.Equal(t, uint64(1007), got[0], "results carry the doc ids the vectors were added under")

	// Built: no further writes, no second build.
	require.ErrorIs(t, index.AddBatch(ctx, []uint64{5000}, vectors[:1]), ErrBuilt)
	require.ErrorIs(t, index.Add(ctx, 5000, vectors[0]), ErrBuilt)
	require.ErrorIs(t, index.Build(ctx), ErrBuilt)
	assert.False(t, index.ContainsDoc(5000), "a rejected write must leave no trace")

	var iterated []uint64
	index.Iterate(func(id uint64) bool {
		iterated = append(iterated, id)
		return true
	})
	slices.Sort(iterated)
	assert.Equal(t, ids, iterated)

	visited := 0
	index.Iterate(func(uint64) bool {
		visited++
		return visited < 3
	})
	assert.Equal(t, 3, visited, "iteration stops when the callback says so")
}

func TestAddBatchRejectsWhatItCannotStore(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(20, 8, 2, 161)
	uc := smallConfig()
	uc.Distance = "l2-squared"

	newIndex := func(t *testing.T) *Index {
		index, err := New(testConfig(t, vectors, 0, provider), uc)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })
		require.NoError(t, index.AddBatch(ctx, []uint64{0, 1, 2}, vectors[:3]))
		return index
	}

	tests := []struct {
		name    string
		ids     []uint64
		vectors [][]float32
		wantIs  error
	}{
		{name: "an empty batch", ids: nil, vectors: nil},
		{name: "ids and vectors of different lengths", ids: []uint64{7}, vectors: vectors[7:9]},
		{
			name: "a vector of another length", ids: []uint64{7, 8},
			vectors: [][]float32{vectors[7], {1, 2, 3}}, wantIs: common.ErrWrongDimensions,
		},
		{
			name: "an id that was already added", ids: []uint64{7, 1},
			vectors: vectors[7:9], wantIs: ErrNotSupported,
		},
		{
			name: "an id repeated within the batch", ids: []uint64{7, 7},
			vectors: vectors[7:9], wantIs: ErrNotSupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			index := newIndex(t)
			err := index.AddBatch(ctx, tt.ids, tt.vectors)
			require.Error(t, err)
			if tt.wantIs != nil {
				require.ErrorIs(t, err, tt.wantIs)
			}

			// A rejected batch is rejected as a whole.
			assert.False(t, index.ContainsDoc(7))
			assert.False(t, index.ContainsDoc(8))
			require.NoError(t, index.Build(ctx))
			memory, ok := index.MemoryStats()
			require.True(t, ok)
			assert.Equal(t, 3, memory.Vectors)
		})
	}

	t.Run("a cancelled context", func(t *testing.T) {
		index := newIndex(t)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		require.ErrorIs(t, index.AddBatch(cancelled, []uint64{7}, vectors[7:8]), context.Canceled)
		assert.False(t, index.ContainsDoc(7))
	})

	// A vector without a nearest centroid would fail the build of the whole
	// index, so it must not get in, and must not take its batch with it.
	nan, inf := float32(math.NaN()), float32(math.Inf(1))
	unusable := map[string][]float32{
		"a NaN value":               {1, 2, nan, 4, 5, 6, 7, 8},
		"an infinite value":         {1, 2, 3, 4, inf, 6, 7, 8},
		"a negative infinite value": {1, 2, 3, 4, 5, -inf, 7, 8},
		"values that overflow":      {3e19, 3e19, 0, 0, 0, 0, 0, 0},
	}
	for name, bad := range unusable {
		t.Run(name, func(t *testing.T) {
			index := newIndex(t)
			require.Error(t, index.ValidateBeforeInsert(bad))
			require.Error(t, index.AddBatch(ctx, []uint64{7, 8}, [][]float32{vectors[7], bad}))
			assert.False(t, index.ContainsDoc(7))
			assert.False(t, index.ContainsDoc(8))

			require.NoError(t, index.Build(ctx), "the vectors that did get in must still build")
			ids, _, err := index.SearchByVector(ctx, vectors[0], 3, nil)
			require.NoError(t, err)
			assert.Len(t, ids, 3)
		})
	}

	t.Run("large but usable values are accepted", func(t *testing.T) {
		// The store has to hold the large vector too: the search rescores
		// against what the thunk returns for the id.
		large := []float32{1e18, -1e18, 1e18, 0, 0, 0, 0, 0}
		stored := slices.Clone(vectors)
		stored[7] = large
		index, err := New(testConfig(t, stored, 0, provider), uc)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })

		require.NoError(t, index.ValidateBeforeInsert(large))
		require.NoError(t, index.AddBatch(ctx, sequentialIDs(8, 0), stored[:8]))
		require.NoError(t, index.Build(ctx))
		ids, dists, err := index.SearchByVector(ctx, large, 1, nil)
		require.NoError(t, err)
		assert.Equal(t, []uint64{7}, ids)
		assert.Equal(t, []float32{0}, dists)
	})

	t.Run("a vector without dimensions as the first vector", func(t *testing.T) {
		index, err := New(testConfig(t, vectors, 0, provider), uc)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })
		require.Error(t, index.AddBatch(ctx, []uint64{0}, [][]float32{{}}))
		assert.False(t, index.ContainsDoc(0))
	})
}

func TestValidateBeforeInsert(t *testing.T) {
	ctx := context.Background()
	vectors := clusteredVectors(5, 8, 1, 171)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index, err := New(testConfig(t, vectors, 0, distancer.NewL2SquaredProvider()), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })

	require.NoError(t, index.ValidateBeforeInsert(make([]float32, 3)), "an empty index takes any length")
	require.NoError(t, index.AddBatch(ctx, sequentialIDs(5, 0), vectors))
	require.NoError(t, index.ValidateBeforeInsert(make([]float32, 8)))
	require.ErrorIs(t, index.ValidateBeforeInsert(make([]float32, 3)), common.ErrWrongDimensions)
}

// Cosine normalizes what it indexes, but must leave the caller's vectors
// alone, and must rank by angle whatever their lengths.
func TestCosineIndexNormalizesACopy(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewCosineDistanceProvider()
	vectors := clusteredVectors(800, 16, 10, 181)
	for i, vec := range vectors {
		scale := float32(1 + i%7)
		for j := range vec {
			vec[j] *= scale
		}
	}
	original := make([][]float32, len(vectors))
	for i, vec := range vectors {
		original[i] = slices.Clone(vec)
	}

	uc := smallConfig()
	uc.EntryPostings = 1000
	uc.RescoreLimit = 1000
	index := buildTestIndex(t, vectors, provider, uc)
	require.Equal(t, original, vectors, "the caller's vectors must not be modified")

	truth := normalizedCopy(vectors)
	for _, i := range []int{0, 13, 400, 799} {
		// A scaled copy of a stored vector is at angle zero from it.
		query := slices.Clone(vectors[i])
		for j := range query {
			query[j] *= 3
		}
		ids, dists, err := index.SearchByVector(ctx, query, 5, nil)
		require.NoError(t, err)
		want := exactNearest(t, truth, distancer.Normalize(query), 5, -1, provider)
		for j, w := range want {
			require.Equal(t, w.id, ids[j])
			require.InDelta(t, w.dist, dists[j], 1e-5)
		}
		require.Equal(t, uint64(i), ids[0])
	}
}

func TestOperationsThisVersionLeavesOutFailLoudly(t *testing.T) {
	ctx := context.Background()
	vectors := clusteredVectors(300, 8, 4, 191)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), uc)

	require.ErrorIs(t, index.Delete(3), ErrNotSupported)
	assert.True(t, index.ContainsDoc(3), "a refused delete must not look like it worked")

	_, _, err := index.SearchByVector(ctx, vectors[0], 5, helpers.NewAllowList(1, 2, 3))
	require.ErrorIs(t, err, ErrNotSupported)

	require.ErrorIs(t, index.PrepareForBackup(ctx), ErrNotSupported)
	_, err = index.SnapshotMutableFiles(ctx, t.TempDir(), t.TempDir())
	require.ErrorIs(t, err, ErrNotSupported)

	// Nothing is persisted, so there is nothing to flush or to list.
	require.NoError(t, index.Flush())
	require.NoError(t, index.ResumeAfterBackup(ctx))
	files, err := index.ListFiles(ctx, t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, files)
	index.PostStartup(ctx)
	index.Preload(1, vectors[1])
}

func TestUpdateUserConfig(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(300, 8, 4, 201)
	base := smallConfig()
	base.Distance = "l2-squared"

	tests := []struct {
		name       string
		built      bool
		updated    func() schemaConfig.VectorIndexConfig
		wantErr    string
		wantEF     int64
		wantLoad   int64
		wantMargin float32
	}{
		{
			name:  "search tunables change on a built index",
			built: true,
			updated: func() schemaConfig.VectorIndexConfig {
				u := base
				u.EF, u.RescoreLimit, u.EntryPostings, u.LoadBatch = 77, 33, 2, 9
				u.SourceMargin = 0.25
				return u
			},
			wantEF: 77, wantLoad: 9, wantMargin: 0.25,
		},
		{
			name:    "build parameters cannot change on a built index",
			built:   true,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.Edges = 4; u.RescueLimit = 8; return u },
			wantErr: "cannot change once the index is built",
		},
		{
			name:    "build parameters can change before the build",
			built:   false,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.Edges = 4; u.EF = 12; return u },
			wantEF:  12, wantLoad: int64(base.LoadBatch),
		},
		{
			name:    "the distance never changes",
			built:   false,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.Distance = "cosine"; return u },
			wantErr: "distance is immutable",
		},
		{
			name:    "an invalid config",
			built:   true,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.LoadBatch = 0; return u },
			wantErr: "loadBatch",
		},
		{
			name:    "a config of another index",
			built:   true,
			updated: func() schemaConfig.VectorIndexConfig { return enthnsw.UserConfig{} },
			wantErr: "not a posting-edge UserConfig",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			index, err := New(testConfig(t, vectors, 0, provider), base)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })
			require.NoError(t, index.AddBatch(ctx, sequentialIDs(len(vectors), 0), vectors))
			if tt.built {
				require.NoError(t, index.Build(ctx))
			}

			called := 0
			updateErr := index.UpdateUserConfig(tt.updated(), func() { called++ })
			assert.Equal(t, 1, called, "the callback runs exactly once, whatever the outcome")

			if tt.wantErr != "" {
				require.ErrorContains(t, updateErr, tt.wantErr)
				assert.Equal(t, int64(base.EF), index.ef.Load(), "a rejected update changes nothing")
				assert.Equal(t, int64(base.LoadBatch), index.loadBatch.Load())
				return
			}
			require.NoError(t, updateErr)
			assert.Equal(t, tt.wantEF, index.ef.Load())
			assert.Equal(t, tt.wantLoad, index.loadBatch.Load())
			assert.Equal(t, tt.wantMargin, math.Float32frombits(index.sourceMargin.Load()))
		})
	}
}

func TestQueryVectorDistancerIsExact(t *testing.T) {
	tests := []struct {
		name     string
		provider distancer.Provider
		distance string
	}{
		{name: "l2", provider: distancer.NewL2SquaredProvider(), distance: "l2-squared"},
		{name: "cosine", provider: distancer.NewCosineDistanceProvider(), distance: "cosine"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vectors := clusteredVectors(300, 8, 4, 211)
			uc := smallConfig()
			uc.Distance = tt.distance
			index := buildTestIndex(t, vectors, tt.provider, uc)

			query := vectors[5]
			qd := index.QueryVectorDistancer(query)
			defer qd.Close()

			for _, id := range []uint64{0, 5, 299} {
				got, err := qd.DistanceToNode(id)
				require.NoError(t, err)

				a, b := query, vectors[id]
				if tt.distance == "cosine" {
					a, b = distancer.Normalize(a), distancer.Normalize(b)
				}
				want, err := tt.provider.SingleDist(a, b)
				require.NoError(t, err)
				assert.InDelta(t, want, got, 1e-5)
			}

			_, err := qd.DistanceToNode(100000)
			require.Error(t, err, "a doc the store does not hold")
		})
	}
}

func TestMemoryStatsAddUp(t *testing.T) {
	vectors := clusteredVectors(2000, 64, 20, 221)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, distancer.NewL2SquaredProvider(), uc)

	memory, ok := index.MemoryStats()
	require.True(t, ok)
	build, ok := index.BuildStats()
	require.True(t, ok)

	assert.Equal(t, 2000, memory.Vectors)
	assert.Equal(t, build.Postings, memory.Postings)
	assert.Equal(t, build.Edges, memory.Edges)

	// 64 dimensions are padded to the 256 bit minimum: 4 words of bits and
	// one of header.
	assert.Equal(t, int64(2000*5*8), memory.CodeBytes)
	assert.Equal(t, int64(2000*8+(memory.Postings+1)*4+2000*4), memory.PostingBytes)
	assert.Equal(t, int64(memory.Edges*4+2001*4), memory.EdgeBytes)
	assert.Equal(t, int64(memory.Postings*64*4), memory.CentroidVectorBytes)
	assert.Equal(t, memory.PostingBytes+memory.CodeBytes+memory.EdgeBytes+memory.CentroidVectorBytes,
		memory.Total())
}

// Every vector must end up in exactly one posting, in the posting of its
// nearest centroid, with the code of its own vector.
func TestBuiltStateInvariants(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(3000, 24, 30, 231)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)
	st := index.state.Load()
	store, side := st.postings, st.side

	require.Equal(t, len(vectors), store.vectors())
	require.Len(t, side.postingOf, len(vectors))
	require.Same(t, &store.codes[0], &side.codes[0], "postings and sidecar share one code array")

	seen := make([]int, len(vectors))
	for p := 0; p < store.postings(); p++ {
		post := store.load(uint32(p))
		require.NotEmptyf(t, post.docIDs, "posting %d must not be empty", p)
		for i, docID := range post.docIDs {
			seen[docID]++
			slot := store.start[p] + uint32(i)
			require.Equal(t, uint32(p), side.postingOf[slot])

			// docID is the input position in this fixture.
			vec := vectors[docID]
			nearest := exactNearest(t, st.centroids.vectors, vec, 1, -1, provider)
			require.Equalf(t, uint64(p), nearest[0].id, "doc %d is not in the posting of its nearest centroid", docID)
			require.Equal(t, st.quantizer.Encode(vec), post.codes[i*store.codeWords:(i+1)*store.codeWords])
		}
	}
	for docID, count := range seen {
		require.Equalf(t, 1, count, "doc %d must be in exactly one posting", docID)
	}
}

func TestShutdownAndDrop(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(300, 8, 4, 241)
	uc := smallConfig()
	uc.Distance = "l2-squared"

	build := func(t *testing.T) (*Index, string) {
		cfg := testConfig(t, vectors, 0, provider)
		index, err := New(cfg, uc)
		require.NoError(t, err)
		require.NoError(t, index.AddBatch(ctx, sequentialIDs(len(vectors), 0), vectors))
		require.NoError(t, index.Build(ctx))
		return index, cfg.RootPath
	}

	t.Run("shutdown", func(t *testing.T) {
		index, _ := build(t)
		require.NoError(t, index.Shutdown(ctx))
		require.NoError(t, index.Shutdown(ctx), "shutting down twice is fine")

		_, _, err := index.SearchByVector(ctx, vectors[0], 5, nil)
		require.ErrorIs(t, err, ErrNotBuilt)
		require.Error(t, index.AddBatch(ctx, []uint64{900}, vectors[:1]))
		require.Error(t, index.Build(ctx))
	})

	t.Run("drop removes what the centroid graph left on disk", func(t *testing.T) {
		index, root := build(t)
		entries, err := os.ReadDir(root)
		require.NoError(t, err)
		require.NotEmpty(t, entries, "the centroid graph creates its directory")

		require.NoError(t, index.Drop(ctx, false))
		matches, err := filepath.Glob(filepath.Join(root, "*"))
		require.NoError(t, err)
		assert.Empty(t, matches)
	})

	t.Run("an index that was never built", func(t *testing.T) {
		index, err := New(testConfig(t, vectors, 0, provider), uc)
		require.NoError(t, err)
		require.NoError(t, index.AddBatch(ctx, []uint64{1}, vectors[:1]))
		require.NoError(t, index.Drop(ctx, false))
	})
}

func TestBuildStopsOnCancelledContext(t *testing.T) {
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(2000, 16, 10, 251)
	uc := smallConfig()
	uc.Distance = "l2-squared"

	index, err := New(testConfig(t, vectors, 0, provider), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(context.Background())) })
	require.NoError(t, index.AddBatch(context.Background(), sequentialIDs(len(vectors), 0), vectors))

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, index.Build(cancelled), context.Canceled)

	// A failed build leaves the index as it was: it can be built again.
	_, _, err = index.SearchByVector(context.Background(), vectors[0], 5, nil)
	require.ErrorIs(t, err, ErrNotBuilt)
	require.NoError(t, index.Build(context.Background()))
	ids, _, err := index.SearchByVector(context.Background(), vectors[0], 5, nil)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), ids[0])
}

// Searches may arrive while the single writer is still adding and building.
// They must fail cleanly until the build is published, never observe a
// half-built index. Run with -race.
func TestSearchDuringBuild(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(4000, 16, 30, 261)
	uc := smallConfig()
	uc.Distance = "l2-squared"

	index, err := New(testConfig(t, vectors, 0, provider), uc)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	unexpected := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				ids, _, err := index.SearchByVector(ctx, vectors[i%len(vectors)], 5, nil)
				index.ContainsDoc(uint64(i % len(vectors)))
				if err == nil && len(ids) == 5 {
					continue
				}
				if err != nil && errors.Is(err, ErrNotBuilt) {
					continue
				}
				unexpected <- err
				return
			}
		}()
	}

	ids := sequentialIDs(len(vectors), 0)
	for start := 0; start < len(vectors); start += 250 {
		require.NoError(t, index.AddBatch(ctx, ids[start:start+250], vectors[start:start+250]))
	}
	require.NoError(t, index.Build(ctx))
	// Let the searchers run against the built index for a moment.
	for i := 0; i < 200; i++ {
		_, _, err := index.SearchByVector(ctx, vectors[i], 5, nil)
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()
	close(unexpected)
	for err := range unexpected {
		t.Errorf("a search during the build returned neither a result nor ErrNotBuilt: %v", err)
	}
}
