//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package batch_request_endpoints

import (
	"testing"

	"github.com/semi-technologies/weaviate/client/batch"
	"github.com/semi-technologies/weaviate/entities/models"
	"github.com/semi-technologies/weaviate/test/acceptance/helper"
	"github.com/stretchr/testify/require"
)

func batchDeleteJourney(t *testing.T) {
	sourcesSize := 20
	var sources []*models.Object
	equalThisName := "equal-this-name"

	getBatchDelete := func(dryRun bool) *batch.BatchObjectsDeleteParams {
		output := "verbose"
		params := batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{
			Match: &models.BatchDeleteMatch{
				Class: "BulkTestSource",
				Where: &models.WhereFilter{
					Operator:    "Equal",
					Path:        []string{"name"},
					ValueString: &equalThisName,
				},
			},
			DryRun: &dryRun,
			Output: &output,
		})
		return params
	}

	t.Run("create some data", func(t *testing.T) {
		sources = make([]*models.Object, sourcesSize)
		for i := range sources {
			sources[i] = &models.Object{
				Class: "BulkTestSource",
				ID:    mustNewUUID(),
				Properties: map[string]interface{}{
					"name": equalThisName,
				},
			}
		}
	})

	t.Run("import all data in batch", func(t *testing.T) {
		params := batch.NewBatchObjectsCreateParams().WithBody(
			batch.BatchObjectsCreateBody{
				Objects: sources,
			},
		)
		res, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		require.Nil(t, err)

		for _, elem := range res.Payload {
			require.Nil(t, elem.Result.Errors)
		}
	})

	t.Run("verify using GraphQL", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{  Get { BulkTestSource(where:{operator:Equal path:["name"] valueString:"equal-this-name"}) { name } } }
		`)
		items := result.Get("Get", "BulkTestSource").AsSlice()
		require.Len(t, items, sourcesSize)
	})

	t.Run("perform batch delete dry run", func(t *testing.T) {
		params := getBatchDelete(true)
		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(sourcesSize), response.Results.Matches)
		require.Equal(t, int64(0), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, sourcesSize, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("verify that batch delete dry run didn't delete data", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{  Get { BulkTestSource(where:{operator:Equal path:["name"] valueString:"equal-this-name"}) { name } } }
		`)
		items := result.Get("Get", "BulkTestSource").AsSlice()
		require.Len(t, items, sourcesSize)
	})

	t.Run("perform batch delete", func(t *testing.T) {
		params := getBatchDelete(false)
		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(sourcesSize), response.Results.Matches)
		require.Equal(t, int64(sourcesSize), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, sourcesSize, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("verify that batch delete deleted everything", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{  Get { BulkTestSource(where:{operator:Equal path:["name"] valueString:"equal-this-name"}) { name } } }
		`)
		items := result.Get("Get", "BulkTestSource").AsSlice()
		require.Len(t, items, 0)
	})
}
