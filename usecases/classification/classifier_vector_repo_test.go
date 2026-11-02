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

package classification

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/modulecapabilities"
	"github.com/weaviate/weaviate/entities/search"
)

type capturingVectorRepo struct {
	vectorRepo
	params dto.GetParams
}

func (r *capturingVectorRepo) VectorSearch(ctx context.Context, params dto.GetParams,
	targetVectors []string, searchVectors []models.Vector,
) ([]search.Result, error) {
	r.params = params
	return nil, nil
}

// Module classifiers compare against the vectors of the found objects, objects
// of remote shards only carry them if the vector is requested.
func TestVectorClassSearchRequestsVector(t *testing.T) {
	repo := &capturingVectorRepo{}

	_, err := newVectorClassSearchRepo(repo).VectorClassSearch(context.Background(),
		modulecapabilities.VectorClassSearchParams{ClassName: "Category", Properties: []string{"id"}})
	require.NoError(t, err)

	assert.Equal(t, "Category", repo.params.ClassName)
	assert.True(t, repo.params.AdditionalProperties.Vector)
}
