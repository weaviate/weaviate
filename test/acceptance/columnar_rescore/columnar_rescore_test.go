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

// Package columnar_rescore_test verifies the user-visible behavior of the
// hnsw `columnarRescore` vectorIndexConfig flag end to end:
//
//   - schema create echoes columnarRescore: true (and the RQ-8 config),
//   - GraphQL nearVector search with RQ-8 + columnarRescore returns the
//     correct nearest neighbors for probe vectors constructed from known
//     objects (the rescore pass reads candidate vectors from the per-target
//     vector column),
//   - the flag is runtime-mutable: a class created with columnarRescore
//     false accepts an update flipping it to true and echoes it,
//   - a graceful restart preserves results: the column bucket reloads from
//     its sentinel, and post-restart searches serve segment-resident rows
//     via the zero-copy rescore path.
//
// CI wiring: this package needs no explicit registration. test/run.sh's
// get_fast_acceptance_packages globs `go list ./... | grep test/acceptance`
// minus an explicit exclusion list; columnar_rescore is neither excluded nor
// assigned to fast groups 1-4, so it lands automatically in the catch-all
// fast group 5.
package columnar_rescore_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	graphqlhelper "github.com/weaviate/weaviate/test/helper/graphql"
)

const (
	className     = "ColumnarRescoreSongs"
	twinClassName = "ColumnarRescoreTwin"
	numObjects    = 500
	dims          = 32
	k             = 10
	batchSize     = 100
)

func boolPtr(b bool) *bool { return &b }

func uuidFor(i int) strfmt.UUID {
	return strfmt.UUID(fmt.Sprintf("%08x-0000-4000-8000-%012x", i, i))
}

// vectorFor returns the deterministic vector of object i: reproducible
// across the import, the probe construction and the post-restart
// assertions without storing anything.
func vectorFor(i int) []float32 {
	r := rand.New(rand.NewSource(int64(i) + 1))
	vec := make([]float32, dims)
	for d := range vec {
		vec[d] = r.Float32()*2 - 1
	}
	return vec
}

// makeClass builds a class with RQ-8 compression (rescoreLimit raised so the
// rescore fetch phase actually runs over a wide candidate set) and the given
// columnarRescore setting. The default cosine distance is kept deliberately:
// it exercises the out-of-place normalization on the zero-copy rescore path.
func makeClass(name string, columnarRescore bool) *models.Class {
	return &models.Class{
		Class:      name,
		Vectorizer: "none",
		Properties: []*models.Property{
			{
				Name:            "idx",
				DataType:        schema.DataTypeInt.PropString(),
				IndexFilterable: boolPtr(true),
			},
		},
		VectorIndexType: "hnsw",
		VectorIndexConfig: map[string]interface{}{
			"rq": map[string]interface{}{
				"enabled":      true,
				"bits":         8,
				"rescoreLimit": 100,
			},
			"columnarRescore": columnarRescore,
		},
	}
}

// vectorIndexConfigOf fetches the class and returns its vectorIndexConfig
// as a map for echo assertions.
func vectorIndexConfigOf(t *testing.T, class string) map[string]interface{} {
	t.Helper()
	cls := helper.GetClass(t, class)
	require.NotNil(t, cls)
	cfg, ok := cls.VectorIndexConfig.(map[string]interface{})
	require.True(t, ok, "vectorIndexConfig is not a map: %T", cls.VectorIndexConfig)
	return cfg
}

type searchHit struct {
	idx      int
	distance float64
}

// searchNearVector runs a GraphQL nearVector query and returns the hits in
// result order.
func searchNearVector(t *testing.T, class string, vec []float32) []searchHit {
	t.Helper()
	vecJSON, err := json.Marshal(vec)
	require.NoError(t, err)
	query := fmt.Sprintf(`{
		Get {
			%s(nearVector: {vector: %s}, limit: %d) {
				idx
				_additional { distance }
			}
		}
	}`, class, vecJSON, k)
	result := graphqlhelper.AssertGraphQL(t, helper.RootAuth, query)
	rows := result.Get("Get", class).AsSlice()
	hits := make([]searchHit, 0, len(rows))
	for _, raw := range rows {
		row, ok := raw.(map[string]interface{})
		require.True(t, ok)
		idxNum, ok := row["idx"].(json.Number)
		require.True(t, ok, "idx is not a number: %T %v", row["idx"], row["idx"])
		idx, err := idxNum.Int64()
		require.NoError(t, err)
		add, ok := row["_additional"].(map[string]interface{})
		require.True(t, ok)
		distNum, ok := add["distance"].(json.Number)
		require.True(t, ok, "distance is not a number: %T %v", add["distance"], add["distance"])
		dist, err := distNum.Float64()
		require.NoError(t, err)
		hits = append(hits, searchHit{idx: int(idx), distance: dist})
	}
	return hits
}

// assertProbeResults asserts the invariants every probe search must satisfy:
// k results, the probe's source object as the top hit at ~zero cosine
// distance, and non-decreasing distances.
func assertProbeResults(t *testing.T, hits []searchHit, wantTop int) {
	t.Helper()
	require.Len(t, hits, k)
	assert.Equal(t, wantTop, hits[0].idx,
		"probe vector built from object %d must return that object first", wantTop)
	assert.InDelta(t, 0.0, hits[0].distance, 0.01,
		"cosine distance of the probe's source object must be ~0")
	for i := 1; i < len(hits); i++ {
		assert.GreaterOrEqual(t, hits[i].distance, hits[i-1].distance,
			"distances must be non-decreasing")
	}
}

func importObjects(t *testing.T, class string) {
	t.Helper()
	for start := 0; start < numObjects; start += batchSize {
		end := start + batchSize
		if end > numObjects {
			end = numObjects
		}
		batch := make([]*models.Object, 0, end-start)
		for i := start; i < end; i++ {
			batch = append(batch, &models.Object{
				Class:      class,
				ID:         uuidFor(i),
				Properties: map[string]interface{}{"idx": i},
				Vector:     vectorFor(i),
			})
		}
		helper.CreateObjectsBatch(t, batch)
	}
}

func TestColumnarRescore(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviate().
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	probeIdxs := []int{0, 123, 250, 499}

	// ── 1. schema create echoes the flag ────────────────────────

	ok := t.Run("create class, schema echoes columnarRescore", func(t *testing.T) {
		helper.CreateClass(t, makeClass(className, true))

		cfg := vectorIndexConfigOf(t, className)
		assert.Equal(t, true, cfg["columnarRescore"],
			"schema GET must echo columnarRescore: true")
		rq, ok := cfg["rq"].(map[string]interface{})
		require.True(t, ok, "rq config missing from echo: %#v", cfg)
		assert.Equal(t, true, rq["enabled"])
	})
	require.True(t, ok, "schema setup failed, aborting")

	// ── 2. import deterministic vectors ─────────────────────────

	ok = t.Run("import deterministic vectors", func(t *testing.T) {
		importObjects(t, className)
	})
	require.True(t, ok, "import failed, aborting")

	// ── 3. nearVector search with RQ-8 + columnar rescore ───────

	t.Run("nearVector returns the constructed nearest neighbor", func(t *testing.T) {
		for _, probe := range probeIdxs {
			hits := searchNearVector(t, className, vectorFor(probe))
			assertProbeResults(t, hits, probe)
		}
	})

	// ── 4. columnarRescore is runtime-mutable ───────────────────

	t.Run("config update flips columnarRescore false to true", func(t *testing.T) {
		helper.CreateClass(t, makeClass(twinClassName, false))
		cfg := vectorIndexConfigOf(t, twinClassName)
		require.Equal(t, false, cfg["columnarRescore"])

		importObjects(t, twinClassName)

		updated := helper.GetClass(t, twinClassName)
		updatedCfg, ok := updated.VectorIndexConfig.(map[string]interface{})
		require.True(t, ok)
		updatedCfg["columnarRescore"] = true
		updated.VectorIndexConfig = updatedCfg
		helper.UpdateClass(t, updated)

		cfg = vectorIndexConfigOf(t, twinClassName)
		assert.Equal(t, true, cfg["columnarRescore"],
			"schema GET must echo the updated columnarRescore: true")

		// searches keep working through the flip (the column backfills in
		// the background; reads fall back per docID until it is servable)
		hits := searchNearVector(t, twinClassName, vectorFor(123))
		assertProbeResults(t, hits, 123)
	})

	// ── 5. restart: column reload (sentinel) + zero-copy reads ──

	t.Run("restart preserves search results", func(t *testing.T) {
		// record pre-restart results to compare against
		before := make(map[int][]searchHit, len(probeIdxs))
		for _, probe := range probeIdxs {
			before[probe] = searchNearVector(t, className, vectorFor(probe))
		}

		require.NoError(t, compose.StopAt(ctx, 0, nil)) // SIGTERM, graceful flush
		require.NoError(t, compose.StartAt(ctx, 0))
		// ports remap on restart: re-setup the REST client
		helper.SetupClient(compose.GetWeaviate().URI())

		cfg := vectorIndexConfigOf(t, className)
		require.Equal(t, true, cfg["columnarRescore"],
			"flag must survive the restart")

		// after the graceful flush every column row is segment-resident, so
		// these searches rescore through the zero-copy (mmap-aliasing) path
		for _, probe := range probeIdxs {
			hits := searchNearVector(t, className, vectorFor(probe))
			assertProbeResults(t, hits, probe)
			require.Len(t, hits, len(before[probe]))
			for i, hit := range hits {
				assert.Equal(t, before[probe][i].idx, hit.idx,
					"probe %d result %d: id differs after restart", probe, i)
				assert.InDelta(t, before[probe][i].distance, hit.distance, 1e-5,
					"probe %d result %d: distance differs after restart", probe, i)
			}
		}

		// the twin (flag flipped to true before the restart) now serves its
		// column-backed read path as well
		hits := searchNearVector(t, twinClassName, vectorFor(250))
		assertProbeResults(t, hits, 250)
	})
}
