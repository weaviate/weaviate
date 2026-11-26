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

package namespace

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/aggregate"
	"github.com/weaviate/weaviate/client/search"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_RESTSearchAndAggregate pins that POST /v1/search/{collection}/bm25
// and POST /v1/aggregate/{collection} resolve a reference-path where filter in
// the caller's namespace. Both namespaces hold the same IDs with swapped series
// titles, so a filter resolved in the other namespace matches different movies.
func TestNamespaces_RESTSearchAndAggregate(t *testing.T) {
	t.Parallel()
	ns1, ns2, user1Key, user2Key := twoNamespaces(t)
	const (
		class  = "RestSearchMovies"
		series = "RestSearchSeries"
	)
	setupClassInBothNamespaces(t, ns1, ns2, series, user1Key, user2Key)
	for _, key := range []string{user1Key, user2Key} {
		helper.CreateClassAuth(t, &models.Class{
			Class: class,
			Properties: []*models.Property{
				{Name: "title", DataType: []string{"text"}},
				{Name: "partOf", DataType: []string{series}},
			},
		}, key)
	}
	t.Cleanup(func() {
		helper.DeleteClassAuth(t, ns1+":"+class, adminKey)
		helper.DeleteClassAuth(t, ns2+":"+class, adminKey)
	})

	trilogyInNs1ID := strfmt.UUID("dddddddd-4444-4444-4444-000000000003")
	trilogyInNs2ID := strfmt.UUID("dddddddd-4444-4444-4444-000000000004")
	seedTwo(t, series, trilogyInNs1ID, "Trilogy", "Anthology", user1Key, user2Key)
	seedTwo(t, series, trilogyInNs2ID, "Anthology", "Trilogy", user1Key, user2Key)

	matrixID := strfmt.UUID("dddddddd-4444-4444-4444-000000000001")
	reloadedID := strfmt.UUID("dddddddd-4444-4444-4444-000000000002")
	seedTwo(t, class, matrixID, "The Matrix", "The Matrix Resurrections", user1Key, user2Key)
	_, err := helper.CreateObjectWithResponseAuth(t, &models.Object{
		ID: reloadedID, Class: class, Properties: map[string]any{"title": "Matrix Reloaded"},
	}, user1Key)
	require.NoError(t, err)

	link := func(key string, movieID, seriesID strfmt.UUID) {
		t.Helper()
		_, err := helper.AddReferenceReturn(t,
			&models.SingleRef{Beacon: strfmt.URI("weaviate://localhost/" + series + "/" + string(seriesID))},
			movieID, class, "partOf", "", helper.CreateAuth(key))
		require.NoError(t, err)
	}
	link(user1Key, matrixID, trilogyInNs1ID)
	link(user1Key, reloadedID, trilogyInNs2ID)
	link(user2Key, matrixID, trilogyInNs2ID)

	where := &models.WhereFilter{
		Operator:  models.WhereFilterOperatorEqual,
		Path:      []string{"partOf", series, "title"},
		ValueText: new("trilogy"),
	}

	cases := []struct {
		name       string
		key        string
		wantTitles map[strfmt.UUID]string
	}{
		{
			name:       "ns1 user",
			key:        user1Key,
			wantTitles: map[strfmt.UUID]string{matrixID: "The Matrix"},
		},
		{
			name:       "ns2 user",
			key:        user2Key,
			wantTitles: map[strfmt.UUID]string{matrixID: "The Matrix Resurrections"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name+" bm25", func(t *testing.T) {
			res, err := helper.Client(t).Search.SearchBm25(
				search.NewSearchBm25Params().WithCollection(class).WithBody(&models.SearchBm25Request{
					SearchCommon: models.SearchCommon{Where: where},
					Query:        new("matrix"),
				}),
				helper.CreateAuth(tc.key),
			)
			require.NoError(t, err)

			got := map[strfmt.UUID]string{}
			for _, hit := range res.Payload.Results {
				require.NotNil(t, hit.ID)
				got[*hit.ID], _ = hit.Properties["title"].(string)
			}
			assert.Equal(t, tc.wantTitles, got)
		})

		t.Run(tc.name+" aggregate", func(t *testing.T) {
			res, err := helper.Client(t).Aggregate.Aggregate(
				aggregate.NewAggregateParams().WithCollection(class).WithBody(&models.AggregateRequest{
					Where:         where,
					ReturnMetrics: []string{"count"},
				}),
				helper.CreateAuth(tc.key),
			)
			require.NoError(t, err)
			require.NotNil(t, res.Payload.Count)
			assert.Equal(t, int64(len(tc.wantTitles)), *res.Payload.Count)
		})
	}
}
