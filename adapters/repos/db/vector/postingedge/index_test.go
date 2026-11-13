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
	"fmt"
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
			name:    "code bits that are not whole words",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.CodeBits = 100; return u },
			wantErr: "codeBits",
		},
		{
			name:    "negative code bits",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.CodeBits = -64; return u },
			wantErr: "codeBits",
		},
		{
			name: "scan codes shorter than full codes",
			cfg:  valid,
			uc:   func() UserConfig { u := NewDefaultUserConfig(); u.ScanCodeBits = 64; return u },
		},
		{
			name: "scan codes shorter than capped codes",
			cfg:  valid,
			uc: func() UserConfig {
				u := NewDefaultUserConfig()
				u.CodeBits, u.ScanCodeBits = 192, 128
				return u
			},
		},
		{
			name:    "scan code bits that are not whole words",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.ScanCodeBits = 100; return u },
			wantErr: "scanCodeBits",
		},
		{
			name:    "negative scan code bits",
			cfg:     valid,
			uc:      func() UserConfig { u := NewDefaultUserConfig(); u.ScanCodeBits = -64; return u },
			wantErr: "scanCodeBits",
		},
		{
			name: "scan codes as wide as the capped codes",
			cfg:  valid,
			uc: func() UserConfig {
				u := NewDefaultUserConfig()
				u.CodeBits, u.ScanCodeBits = 128, 128
				return u
			},
			wantErr: "scanCodeBits (128) must be smaller than codeBits (128)",
		},
		{
			name: "scan codes wider than the capped codes",
			cfg:  valid,
			uc: func() UserConfig {
				u := NewDefaultUserConfig()
				u.CodeBits, u.ScanCodeBits = 128, 256
				return u
			},
			wantErr: "scanCodeBits (256) must be smaller than codeBits (128)",
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
			name:    "the code width cannot change on a built index",
			built:   true,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.CodeBits = 64; return u },
			wantErr: "cannot change once the index is built",
		},
		{
			name:    "the scan code width cannot change on a built index",
			built:   true,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.ScanCodeBits = 64; return u },
			wantErr: "cannot change once the index is built",
		},
		{
			name:    "the scan code width can change before the build",
			built:   false,
			updated: func() schemaConfig.VectorIndexConfig { u := base; u.ScanCodeBits = 64; return u },
			wantEF:  int64(base.EF), wantLoad: int64(base.LoadBatch),
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
	assert.Zero(t, memory.ScanCodeBytes, "the scan reads the codes of the sidecar")
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

// Capped codes shrink the index and blur its estimates, nothing else: the
// structure is the same, and with everything loaded and rescored the result
// is still exact.
func TestCappedCodes(t *testing.T) {
	const dims, n, k = 512, 1500, 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(n+30, dims, 1, 501)
	vectors, queries := all[:n], all[n:]
	truth := normalizedCopy(vectors)

	tests := []struct {
		name      string
		codeBits  int
		wantWords int
	}{
		{name: "full codes", codeBits: 0, wantWords: 9},
		{name: "capped to 256 bits", codeBits: 256, wantWords: 5},
		{name: "capped to 64 bits", codeBits: 64, wantWords: 2},
		{name: "capped at the full width", codeBits: 512, wantWords: 9},
	}
	recalls := map[int]float64{}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			uc.CodeBits = tt.codeBits
			uc.EF = 4 * k
			index := buildTestIndex(t, vectors, provider, uc)
			st := index.state.Load()

			require.Equal(t, tt.wantWords, st.postings.codeWords)
			memory, ok := index.MemoryStats()
			require.True(t, ok)
			assert.Equal(t, int64(n*tt.wantWords*8), memory.CodeBytes)
			for slot, docID := range st.postings.docIDs {
				require.Equal(t, st.quantizer.Encode(truth[docID]), st.side.code(uint32(slot)))
			}

			// Through the edges, with the estimates the codes give.
			recall := recallAt(t, index, truth, normalizedCopy(queries), k, provider)
			t.Logf("%s: recall@%d %.3f", tt.name, k, recall)
			recalls[tt.codeBits] = recall

			// With every posting loaded and every vector rescored, the
			// codes decide nothing.
			exhaustive := uc
			exhaustive.EntryPostings, exhaustive.RescoreLimit = n, n
			require.NoError(t, index.UpdateUserConfig(exhaustive, func() {}))
			for _, query := range queries {
				ids, _, err := index.SearchByVector(context.Background(), query, k, nil)
				require.NoError(t, err)
				want := exactNearest(t, truth, distancer.Normalize(query), k, -1, provider)
				for i, w := range want {
					require.Equal(t, w.id, ids[i])
				}
			}
		})
	}

	// Coarser codes cost recall, in order, and must not break the search:
	// picking at random would find 0.007.
	assert.InDelta(t, recalls[0], recalls[dims], 1e-9, "capped at the full width is the full code")
	assert.Greater(t, recalls[0], recalls[256])
	assert.Greater(t, recalls[256], recalls[64])
	assert.Greater(t, recalls[64], 0.15)
}

func TestCodeBitsWiderThanTheCodeFailTheBuild(t *testing.T) {
	// 24 dimensions are padded to the minimum code of 256 bits.
	vectors := clusteredVectors(200, 24, 3, 511)

	tests := []struct {
		name          string
		tooWide       func(*UserConfig)
		fits          func(*UserConfig)
		wantErr       string
		wantScanWords int
		wantSideWords int
	}{
		{
			name:          "the codes of the index",
			tooWide:       func(u *UserConfig) { u.CodeBits = 512 },
			fits:          func(u *UserConfig) { u.CodeBits = 128 },
			wantErr:       "cap the codes",
			wantScanWords: 3, wantSideWords: 3,
		},
		{
			name:          "the scan codes",
			tooWide:       func(u *UserConfig) { u.ScanCodeBits = 512 },
			fits:          func(u *UserConfig) { u.ScanCodeBits = 64 },
			wantErr:       "scan codes: cap the codes",
			wantScanWords: 2, wantSideWords: 5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			uc := smallConfig()
			uc.Distance = "l2-squared"
			tt.tooWide(&uc)

			index, err := New(testConfig(t, vectors, 0, distancer.NewL2SquaredProvider()), uc)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, index.Shutdown(ctx)) })
			require.NoError(t, index.AddBatch(ctx, sequentialIDs(len(vectors), 0), vectors))
			require.ErrorContains(t, index.Build(ctx), tt.wantErr)

			// The failed build left the vectors in place: with a width that
			// fits, it builds.
			tt.fits(&uc)
			require.NoError(t, index.UpdateUserConfig(uc, func() {}))
			require.NoError(t, index.Build(ctx))
			st := index.state.Load()
			assert.Equal(t, tt.wantScanWords, st.postings.codeWords)
			assert.Equal(t, tt.wantSideWords, st.side.codeWords)
		})
	}
}

// Scan codes of their own split the code array in two: short codes in the
// postings, the codes of CodeBits in the sidecar, each from its own codec over
// one quantizer. Nothing else about the structure changes, and with
// everything loaded and rescored the result is still exact.
func TestScanCodes(t *testing.T) {
	const dims, n, k = 512, 1500, 10
	provider := distancer.NewCosineDistanceProvider()
	all := clusteredVectors(n+30, dims, 1, 501)
	vectors, queries := all[:n], all[n:]
	truth := normalizedCopy(vectors)

	tests := []struct {
		name          string
		codeBits      int
		scanCodeBits  int
		wantScanWords int
		wantSideWords int
	}{
		{name: "short scan codes, full sidecar codes", scanCodeBits: 128, wantScanWords: 3, wantSideWords: 9},
		{name: "short scan codes, capped sidecar codes", codeBits: 256, scanCodeBits: 64, wantScanWords: 2, wantSideWords: 5},
		{name: "scan codes at the full width", scanCodeBits: dims, wantScanWords: 9, wantSideWords: 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uc := smallConfig()
			uc.CodeBits, uc.ScanCodeBits = tt.codeBits, tt.scanCodeBits
			uc.EF = 4 * k
			index := buildTestIndex(t, vectors, provider, uc)
			st := index.state.Load()
			store, side := st.postings, st.side

			require.Equal(t, tt.wantScanWords, store.codeWords)
			require.Equal(t, tt.wantSideWords, side.codeWords)
			require.Len(t, store.codes, n*tt.wantScanWords)
			require.Len(t, side.codes, n*tt.wantSideWords)
			require.NotSame(t, &store.codes[0], &side.codes[0], "the postings have a code array of their own")

			memory, ok := index.MemoryStats()
			require.True(t, ok)
			assert.Equal(t, int64(n*tt.wantSideWords*8), memory.CodeBytes)
			assert.Equal(t, int64(n*tt.wantScanWords*8), memory.ScanCodeBytes)
			assert.Equal(t, memory.PostingBytes+memory.CodeBytes+memory.ScanCodeBytes+memory.EdgeBytes+
				memory.CentroidVectorBytes, memory.Total())

			for slot, docID := range store.docIDs {
				require.Equal(t, st.quantizer.Encode(truth[docID]), side.code(uint32(slot)))
				require.Equal(t, st.scanQuantizer.Encode(truth[docID]),
					store.codes[slot*store.codeWords:(slot+1)*store.codeWords])
			}

			// Through the edges, scanning with one code and ranking the
			// endpoints with the other: picking at random would find 0.007.
			recall := recallAt(t, index, truth, normalizedCopy(queries), k, provider)
			t.Logf("%s: recall@%d %.3f", tt.name, k, recall)
			assert.Greater(t, recall, 0.15)

			// With every posting loaded and every vector rescored, the
			// codes decide nothing.
			exhaustive := uc
			exhaustive.EntryPostings, exhaustive.RescoreLimit = n, n
			require.NoError(t, index.UpdateUserConfig(exhaustive, func() {}))
			for _, query := range queries {
				ids, _, err := index.SearchByVector(context.Background(), query, k, nil)
				require.NoError(t, err)
				want := exactNearest(t, truth, distancer.Normalize(query), k, -1, provider)
				for i, w := range want {
					require.Equal(t, w.id, ids[i])
				}
			}
		})
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

// replaceEdges republishes one build with other edges: the memory accounting
// follows them, a search reads them, and putting the full edges back gives
// back the results of the build.
func TestReplaceEdges(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(3000, 24, 30, 271)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	uc.EF = 8
	index := buildTestIndex(t, vectors, provider, uc)

	full := index.state.Load().postings
	fullStart, fullEdges := full.edgeStart, full.edges
	queries := vectors[:200]
	search := func() [][]uint64 {
		out := make([][]uint64, len(queries))
		for i, q := range queries {
			ids, _, err := index.SearchByVector(ctx, q, 10, nil)
			require.NoError(t, err)
			out[i] = ids
		}
		return out
	}
	before := search()

	capped := func(k int) func() ([]uint32, []uint32) {
		return func() ([]uint32, []uint32) { return capDegree(fullStart, fullEdges, k) }
	}
	tests := []struct {
		name  string
		edges func() ([]uint32, []uint32)
		full  bool
	}{
		{name: "one edge per vector", edges: capped(1)},
		{name: "three edges per vector", edges: capped(3)},
		{name: "one edge per target posting", edges: func() ([]uint32, []uint32) {
			return onePerTarget(fullStart, fullEdges, index.state.Load().side.postingOf)
		}},
		{name: "no edges at all", edges: func() ([]uint32, []uint32) {
			return make([]uint32, len(fullStart)), []uint32{}
		}},
		{name: "the full edges again", edges: capped(0), full: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, edges := tt.edges()
			require.NoError(t, index.replaceEdges(start, edges))

			memory, ok := index.MemoryStats()
			require.True(t, ok)
			assert.Equal(t, len(edges), memory.Edges)
			assert.Equal(t, int64(len(edges)*4+len(start)*4), memory.EdgeBytes)

			st := index.state.Load()
			for slot := uint32(0); slot < uint32(st.postings.vectors()); slot++ {
				require.Equal(t, edges[start[slot]:start[slot+1]], st.postings.edgesOf(slot))
			}
			// Everything else is the build as it was.
			assert.Equal(t, full.docIDs, st.postings.docIDs)
			assert.Equal(t, full.start, st.postings.start)

			got := search()
			if tt.full {
				assert.Equal(t, before, got, "the full edges give back the results of the build")
			}
			// Every query is a stored vector, found through its own posting
			// with or without edges.
			for i, ids := range got {
				assert.Containsf(t, ids, uint64(i), "query %d", i)
			}
		})
	}
}

func TestReplaceEdgesRejectsWhatIsNotAnEdgeSet(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(1000, 16, 10, 281)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)
	st := index.state.Load()
	n := st.postings.vectors()
	good := func() ([]uint32, []uint32) {
		return slices.Clone(st.postings.edgeStart), slices.Clone(st.postings.edges)
	}

	tests := []struct {
		name  string
		edges func() ([]uint32, []uint32)
		want  string
	}{
		{name: "an offset short", want: "edge offsets", edges: func() ([]uint32, []uint32) {
			start, edges := good()
			return start[:n], edges
		}},
		{name: "offsets that do not start at zero", want: "span", edges: func() ([]uint32, []uint32) {
			start, edges := good()
			start[0] = 1
			return start, edges
		}},
		{name: "offsets that do not reach the last edge", want: "span", edges: func() ([]uint32, []uint32) {
			start, edges := good()
			return start, append(edges, 0)
		}},
		{name: "decreasing offsets", want: "decrease", edges: func() ([]uint32, []uint32) {
			start, edges := good()
			start[1] = start[2] + 1
			return start, edges
		}},
		{name: "an endpoint beyond the last slot", want: "ends at slot", edges: func() ([]uint32, []uint32) {
			start, edges := good()
			edges[len(edges)/2] = uint32(n)
			return start, edges
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start, edges := tt.edges()
			err := index.replaceEdges(start, edges)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
			assert.Same(t, st, index.state.Load(), "a rejected edge set publishes nothing")
		})
	}

	t.Run("before the build", func(t *testing.T) {
		empty, err := New(testConfig(t, vectors, 0, provider), uc)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, empty.Shutdown(ctx)) })
		require.ErrorContains(t, empty.replaceEdges([]uint32{0}, nil), "not built")
	})

	t.Run("after shutdown", func(t *testing.T) {
		closed := buildTestIndex(t, vectors, provider, uc)
		start, edges := good()
		require.NoError(t, closed.Shutdown(ctx))
		require.ErrorContains(t, closed.replaceEdges(start, edges), "closed")
	})
}

// Searches running while the edges are replaced read one edge set or the
// other, never a mix: under -race this is also the check that the state a
// search holds is not written.
func TestReplaceEdgesDuringSearches(t *testing.T) {
	ctx := context.Background()
	provider := distancer.NewL2SquaredProvider()
	vectors := clusteredVectors(3000, 16, 30, 291)
	uc := smallConfig()
	uc.Distance = "l2-squared"
	index := buildTestIndex(t, vectors, provider, uc)
	full := index.state.Load().postings
	fullStart, fullEdges := full.edgeStart, full.edges

	stop := make(chan struct{})
	var wg sync.WaitGroup
	failures := make(chan error, 4)
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := worker; ; i += 4 {
				select {
				case <-stop:
					return
				default:
				}
				doc := i % len(vectors)
				ids, _, err := index.SearchByVector(ctx, vectors[doc], 5, nil)
				if err == nil && !slices.Contains(ids, uint64(doc)) {
					err = fmt.Errorf("vector %d not found with itself as the query: %v", doc, ids)
				}
				if err != nil {
					failures <- err
					return
				}
			}
		}()
	}
	for round := 0; round < 50; round++ {
		start, edges := capDegree(fullStart, fullEdges, round%4)
		require.NoError(t, index.replaceEdges(start, edges))
	}
	close(stop)
	wg.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}
