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

package traverser

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
)

// A hybrid nearVector search over several targets hands searchForTargets one
// query vector per target. The request's NearVector must not be rewritten on
// the way: an earlier loop set its TargetVectors to the last target through
// the shared pointer.
func Test_Explorer_HybridNearVectorMultiTarget(t *testing.T) {
	title, body := []float32{1, 0, 0}, []float32{0, 1, 0}
	searcher := &fakeVectorSearcher{}
	var gotParams dto.GetParams
	var gotVectors []models.Vector
	searcher.On("VectorSearch", mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			gotParams = args.Get(0).(dto.GetParams)
			gotVectors = args.Get(1).([]models.Vector)
		}).
		Return(makeHybridVectorResults(3), nil)
	explorer := newTestExplorer(searcher, getFakeModulesProvider())

	nearVector := &searchparams.NearVector{Vectors: []models.Vector{title, body}}
	res, err := explorer.Hybrid(context.Background(), dto.GetParams{
		ClassName:  "TestClass",
		Pagination: &filters.Pagination{Offset: 0, Limit: 10},
		HybridSearch: &searchparams.HybridSearch{
			Query:            "foo",
			Alpha:            1,
			TargetVectors:    []string{"title", "body"},
			NearVectorParams: nearVector,
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, res)

	searcher.AssertNumberOfCalls(t, "VectorSearch", 1)
	assert.Equal(t, []models.Vector{title, body}, gotVectors, "one query vector per target, in target order")
	assert.Equal(t, []string{"title", "body"}, gotParams.HybridSearch.TargetVectors)
	assert.Nil(t, nearVector.TargetVectors, "the request's nearVector must not be rewritten to a single target")
	assert.Equal(t, []models.Vector{title, body}, nearVector.Vectors)
}
