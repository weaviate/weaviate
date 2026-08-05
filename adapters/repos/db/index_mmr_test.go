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

package db

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
	vectorIndexCommon "github.com/weaviate/weaviate/entities/vectorindex/common"
)

// stubVectorIndexConfig implements schemaConfig.VectorIndexConfig for tests.
type stubVectorIndexConfig struct {
	distance string
}

func (s stubVectorIndexConfig) IndexType() string    { return "hnsw" }
func (s stubVectorIndexConfig) DistanceName() string { return s.distance }
func (s stubVectorIndexConfig) IsMultiVector() bool  { return false }

func makeTestObject(vector []float32) *storobj.Object {
	return &storobj.Object{
		Object: models.Object{},
		Vector: vector,
	}
}

func makeTestObjectNamed(name string, vector []float32) *storobj.Object {
	return &storobj.Object{
		Object:  models.Object{Vectors: models.Vectors{name: vector}},
		Vectors: map[string][]float32{name: vector},
	}
}

func TestApplySelectionOnObjects_NilSelection(t *testing.T) {
	idx := &Index{}
	objs := []*storobj.Object{makeTestObject([]float32{1, 0})}
	dists := []float32{0.1}

	out, outDists, err := idx.applySelectionOnObjects(context.Background(), nil, []string{""}, objs, dists)
	require.NoError(t, err)
	assert.Equal(t, objs, out)
	assert.Equal(t, dists, outDists)
}

func TestApplySelectionOnObjects_EmptyObjects(t *testing.T) {
	idx := &Index{}
	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: 3, Balance: 0.5}}

	out, outDists, err := idx.applySelectionOnObjects(context.Background(), sel, []string{""}, nil, nil)
	require.NoError(t, err)
	assert.Nil(t, out)
	assert.Nil(t, outDists)
}

func TestApplySelectionOnObjects_DiversityWithDefaultVector(t *testing.T) {
	// Two clusters: close cluster near (1,0,0), far cluster near (0,1,0).
	// With balance=0 (pure diversity) and limit=4, MMR should alternate clusters.
	idx := &Index{
		vectorIndexUserConfig: stubVectorIndexConfig{distance: vectorIndexCommon.DistanceL2Squared},
	}

	objects := []*storobj.Object{
		makeTestObject([]float32{1, 0, 0}),    // close cluster
		makeTestObject([]float32{1.01, 0, 0}), // close cluster
		makeTestObject([]float32{0.99, 0, 0}), // close cluster
		makeTestObject([]float32{0, 1, 0}),    // far cluster
		makeTestObject([]float32{0, 1.01, 0}), // far cluster
		makeTestObject([]float32{0, 0.99, 0}), // far cluster
	}
	// Query distances: close cluster is nearer
	queryDists := []float32{0.01, 0.02, 0.03, 0.9, 0.91, 0.92}

	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: 4, Balance: 0}}

	out, outDists, err := idx.applySelectionOnObjects(
		context.Background(), sel, []string{""}, objects, queryDists)
	require.NoError(t, err)
	require.Len(t, out, 4)
	require.Len(t, outDists, 4)

	// First pick: most relevant (objects[0], close cluster)
	assert.Equal(t, objects[0], out[0])

	// Second pick: should be from far cluster (diverse from objects[0])
	assert.Contains(t, []*storobj.Object{objects[3], objects[4], objects[5]}, out[1],
		"second pick should be from the far cluster for diversity")

	// With 4 picks and balance=0, we should get objects from both clusters
	closeCount := 0
	farCount := 0
	for _, obj := range out {
		if obj.Vector[0] > 0.5 {
			closeCount++
		} else {
			farCount++
		}
	}
	assert.True(t, closeCount > 0 && farCount > 0,
		"MMR should select from both clusters, got close=%d far=%d", closeCount, farCount)
}

func TestApplySelectionOnObjects_PureRelevance(t *testing.T) {
	// With balance=1 (pure relevance), MMR should return in distance order.
	idx := &Index{
		vectorIndexUserConfig: stubVectorIndexConfig{distance: vectorIndexCommon.DistanceCosine},
	}

	objects := []*storobj.Object{
		makeTestObject([]float32{1, 0}),
		makeTestObject([]float32{0, 1}),
		makeTestObject([]float32{0.5, 0.5}),
	}
	queryDists := []float32{0.1, 0.9, 0.5}

	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: 3, Balance: 1}}

	out, outDists, err := idx.applySelectionOnObjects(
		context.Background(), sel, []string{""}, objects, queryDists)
	require.NoError(t, err)
	require.Len(t, out, 3)

	// Pure relevance: sorted by ascending query distance
	assert.Equal(t, objects[0], out[0])
	assert.Equal(t, objects[2], out[1])
	assert.Equal(t, objects[1], out[2])
	assert.Equal(t, []float32{0.1, 0.5, 0.9}, outDists)
}

func TestApplySelectionOnObjects_NamedVector(t *testing.T) {
	// Test with a named target vector instead of the default.
	idx := &Index{
		vectorIndexUserConfigs: map[string]schemaConfig.VectorIndexConfig{
			"title": stubVectorIndexConfig{distance: vectorIndexCommon.DistanceL2Squared},
		},
	}

	objects := []*storobj.Object{
		makeTestObjectNamed("title", []float32{1, 0}),
		makeTestObjectNamed("title", []float32{0, 1}),
	}
	queryDists := []float32{0.1, 0.8}

	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: 2, Balance: 0.5}}

	out, outDists, err := idx.applySelectionOnObjects(
		context.Background(), sel, []string{"title"}, objects, queryDists)
	require.NoError(t, err)
	require.Len(t, out, 2)
	require.Len(t, outDists, 2)
	assert.Equal(t, objects[0], out[0])
}

func TestApplySelectionOnObjects_LimitSmaller(t *testing.T) {
	// MMR limit smaller than input: should select a subset.
	idx := &Index{
		vectorIndexUserConfig: stubVectorIndexConfig{distance: vectorIndexCommon.DistanceCosine},
	}

	objects := []*storobj.Object{
		makeTestObject([]float32{1, 0}),
		makeTestObject([]float32{0.9, 0.1}),
		makeTestObject([]float32{0, 1}),
		makeTestObject([]float32{0.5, 0.5}),
	}
	queryDists := []float32{0.1, 0.15, 0.9, 0.5}

	sel := &searchparams.Selection{MMR: &searchparams.SelectionMMR{Limit: 2, Balance: 0}}

	out, _, err := idx.applySelectionOnObjects(
		context.Background(), sel, []string{""}, objects, queryDists)
	require.NoError(t, err)
	assert.Len(t, out, 2)

	// First: most relevant (objects[0])
	assert.Equal(t, objects[0], out[0])
	// Second: with pure diversity, should be the most different from objects[0]
	assert.Equal(t, objects[2], out[1])
}
