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
	"fmt"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/dto"
	"github.com/weaviate/weaviate/entities/filters"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/search"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/usecases/queryadmission"
)

// TestExplorer_AdmissionShedIdentitySurvivesWrapping pins that Explorer's %w
// re-wraps preserve errors.Is(err, queryadmission.ErrOverloaded) at every entrypoint.
func TestExplorer_AdmissionShedIdentitySurvivesWrapping(t *testing.T) {
	// Wrapped, not the raw sentinel, to also exercise errors.Is traversal.
	shed := fmt.Errorf("shard search: %w", queryadmission.ErrOverloaded)

	newExplorer := func(searcher *fakeVectorSearcher) *Explorer {
		log, _ := test.NewNullLogger()
		e := NewExplorer(searcher, log, getFakeModulesProvider(), &fakeMetrics{}, defaultConfig)
		e.SetSchemaGetter(&fakeSchemaGetter{
			schema: schema.Schema{Objects: &models.Schema{Classes: []*models.Class{
				{Class: "BestClass"},
			}}},
		})
		return e
	}

	cases := []struct {
		name   string
		invoke func() error
	}{
		{
			name: "keyword (bm25) path via searcher.Search",
			invoke: func() error {
				searcher := &fakeVectorSearcher{}
				searcher.On("Search", mock.Anything).Return([]search.Result(nil), shed)
				_, err := newExplorer(searcher).GetClass(context.Background(), dto.GetParams{
					ClassName:      "BestClass",
					KeywordRanking: &searchparams.KeywordRanking{Query: "foo"},
					Pagination:     &filters.Pagination{Limit: 10},
				})
				return err
			},
		},
		{
			name: "vector search path via searcher.VectorSearch",
			invoke: func() error {
				searcher := &fakeVectorSearcher{}
				searcher.On("VectorSearch", mock.Anything, mock.Anything).Return([]search.Result(nil), shed)
				_, err := newExplorer(searcher).GetClass(context.Background(), dto.GetParams{
					ClassName:  "BestClass",
					NearVector: &searchparams.NearVector{Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}},
					Pagination: &filters.Pagination{Limit: 10},
				})
				return err
			},
		},
		{
			name: "cross-class path via searcher.CrossClassVectorSearch",
			invoke: func() error {
				searcher := &fakeVectorSearcher{crossClassErr: shed}
				_, err := newExplorer(searcher).CrossClassVectorSearch(context.Background(), ExploreParams{
					NearVector: &searchparams.NearVector{Vectors: []models.Vector{[]float32{0.1, 0.2, 0.3}}},
				})
				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.invoke()
			require.Error(t, err)
			require.ErrorIs(t, err, queryadmission.ErrOverloaded,
				"explorer must preserve the admission shed (ErrOverloaded) identity through error wrapping")
		})
	}
}
