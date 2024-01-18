//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package batch_request_endpoints

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func batchDeleteJourney(t *testing.T) {
	maxObjects := 20
	var sources []*models.Object
	equalThisName := "equal-this-name"

	getBatchDelete := func(className string, path []string, valueText string, dryRun bool) *batch.BatchObjectsDeleteParams {
		output := "verbose"
		params := batch.NewBatchObjectsDeleteParams().WithBody(&models.BatchDelete{
			Match: &models.BatchDeleteMatch{
				Class: className,
				Where: &models.WhereFilter{
					Operator:  "Equal",
					Path:      path,
					ValueText: &valueText,
				},
			},
			DryRun: &dryRun,
			Output: &output,
		})
		return params
	}

	sourceUUIDs := make([]strfmt.UUID, maxObjects)
	targetUUIDs := make([]strfmt.UUID, maxObjects)

	t.Run("create some data", func(t *testing.T) {
		sources = make([]*models.Object, maxObjects)
		for i := range sources {
			uuid := mustNewUUID()

			sources[i] = &models.Object{
				Class: "BulkTestSource",
				ID:    uuid,
				Properties: map[string]interface{}{
					"name": equalThisName,
				},
			}

			sourceUUIDs[i] = uuid
		}

		targets := make([]*models.Object, maxObjects)
		for i := range targets {
			uuid := mustNewUUID()

			targets[i] = &models.Object{
				Class: "BulkTestTarget",
				ID:    uuid,
				Properties: map[string]interface{}{
					"intProp": i,
				},
			}

			targetUUIDs[i] = uuid
		}
	})

	t.Run("import all batch objects", func(t *testing.T) {
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

	t.Run("import all batch refs", func(t *testing.T) {
		batchRefs := make([]*models.BatchReference, len(sources))

		for i := range batchRefs {
			batchRefs[i] = &models.BatchReference{
				From: strfmt.URI(fmt.Sprintf("weaviate://localhost/%s/%s/fromSource", "BulkTestTarget", targetUUIDs[i])),
				To:   strfmt.URI(fmt.Sprintf("weaviate://localhost/%s", sourceUUIDs[i])),
			}
		}

		params := batch.NewBatchReferencesCreateParams().WithBody(batchRefs)
		res, err := helper.Client(t).Batch.BatchReferencesCreate(params, nil)
		require.Nil(t, err)

		for _, elem := range res.Payload {
			require.Nil(t, elem.Result.Errors)
		}
	})

	t.Run("verify using GraphQL", func(t *testing.T) {
		// verify objects
		result := AssertGraphQL(t, helper.RootAuth, `
		{  Get { BulkTestSource(where:{operator:Equal path:["name"] valueText:"equal-this-name"}) { name } } }
		`)
		items := result.Get("Get", "BulkTestSource").AsSlice()
		require.Len(t, items, maxObjects)

		// verify refs
		result = AssertGraphQL(t, helper.RootAuth, `
		{
		  Get {
			BulkTestTarget
			(
			  where: {
				path: ["fromSource", "BulkTestSource", "name"]
				operator: Equal
				valueText: "equal-this-name"
			  }
			)
			{
			  fromSource {
				... on BulkTestSource {
				  _additional {
					id
				  }
				}
			  }
			}
		  }
		}
		`)
		items = result.Get("Get", "BulkTestTarget").AsSlice()
		for _, item := range items {
			fromSource := item.(map[string]interface{})["fromSource"]
			require.NotNil(t, fromSource)
		}
		require.Len(t, items, maxObjects)
	})

	t.Run("perform batch delete by refs dry run", func(t *testing.T) {
		params := getBatchDelete("BulkTestTarget", []string{"fromSource", "BulkTestSource", "name"}, equalThisName, true)
		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(maxObjects), response.Results.Matches)
		require.Equal(t, int64(0), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, maxObjects, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("[deprecated string] perform batch delete by refs dry run", func(t *testing.T) {
		params := getBatchDelete("BulkTestTarget", []string{"fromSource", "BulkTestSource", "name"}, equalThisName, true)
		params.Body.Match.Where.ValueText = nil
		params.Body.Match.Where.ValueString = &equalThisName

		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(maxObjects), response.Results.Matches)
		require.Equal(t, int64(0), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, maxObjects, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("verify that batch delete by refs dry run didn't delete data", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{
		  Get {
			BulkTestTarget
			(
			  where: {
				path: ["fromSource", "BulkTestSource", "name"]
				operator: Equal
				valueText: "equal-this-name"
			  }
			)
			{
			  fromSource {
				... on BulkTestSource {
				  _additional {
					id
				  }
				}
			  }
			}
		  }
		}
		`)
		items := result.Get("Get", "BulkTestTarget").AsSlice()
		require.Len(t, items, maxObjects)
	})

	t.Run("perform batch delete by prop dry run", func(t *testing.T) {
		params := getBatchDelete("BulkTestSource", []string{"name"}, equalThisName, true)
		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(maxObjects), response.Results.Matches)
		require.Equal(t, int64(0), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, maxObjects, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("verify that batch delete by prop dry run didn't delete data", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{  Get { BulkTestSource(where:{operator:Equal path:["name"] valueText:"equal-this-name"}) { name } } }
		`)
		items := result.Get("Get", "BulkTestSource").AsSlice()
		require.Len(t, items, maxObjects)
	})

	t.Run("perform batch delete by ref", func(t *testing.T) {
		params := getBatchDelete("BulkTestTarget", []string{"fromSource", "BulkTestSource", "name"}, equalThisName, false)
		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(maxObjects), response.Results.Matches)
		require.Equal(t, int64(maxObjects), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, maxObjects, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("verify that batch delete by ref deleted everything", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{
		  Get {
			BulkTestTarget
			(
			  where: {
				path: ["fromSource", "BulkTestSource", "name"]
				operator: Equal
				valueText: "equal-this-name"
			  }
			)
			{
			  fromSource {
				... on BulkTestSource {
				  _additional {
					id
				  }
				}
			  }
			}
		  }
		}
		`)
		items := result.Get("Get", "BulkTestTarget").AsSlice()
		require.Len(t, items, 0)
	})

	t.Run("perform batch delete by prop", func(t *testing.T) {
		params := getBatchDelete("BulkTestSource", []string{"name"}, equalThisName, false)
		res, err := helper.Client(t).Batch.BatchObjectsDelete(params, nil)
		require.Nil(t, err)

		response := res.Payload
		require.NotNil(t, response)
		require.NotNil(t, response.Match)
		require.NotNil(t, response.Results)
		require.Equal(t, int64(maxObjects), response.Results.Matches)
		require.Equal(t, int64(maxObjects), response.Results.Successful)
		require.Equal(t, int64(0), response.Results.Failed)
		require.Equal(t, maxObjects, len(response.Results.Objects))
		for _, elem := range response.Results.Objects {
			require.Nil(t, elem.Errors)
		}
	})

	t.Run("verify that batch delete by prop deleted everything", func(t *testing.T) {
		result := AssertGraphQL(t, helper.RootAuth, `
		{  Get { BulkTestSource(where:{operator:Equal path:["name"] valueText:"equal-this-name"}) { name } } }
		`)
		items := result.Get("Get", "BulkTestSource").AsSlice()
		require.Len(t, items, 0)
	})
}
