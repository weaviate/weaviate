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
	"math/rand"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func batchPatchJourney(t *testing.T) {
	t.Skip()
	var (
		numObjects  = 100
		testObjects = createBatchPatchTestObjects(numObjects)
	)

	t.Run("import objects", func(t *testing.T) {
		params := batch.NewBatchObjectsCreateParams().WithBody(
			batch.BatchObjectsCreateBody{Objects: testObjects})
		res, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		require.Nil(t, err)

		for _, elem := range res.Payload {
			assert.Nil(t, elem.Result.Errors)
		}
	})

	t.Run("successfully update random existing objects", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			toUpdate := make([]*models.Object, 20)
			for j, k := rand.Intn(numObjects), 0; j < (j + 20); j, k = j+1, k+1 {
				idx := j % numObjects
				obj := &models.Object{
					Class: testObjects[idx].Class,
					ID:    testObjects[idx].ID,
					Properties: map[string]interface{}{
						"name": "updated!",
					},
				}
				toUpdate[k] = obj
			}
			res, err := doBatchPatch(t, toUpdate)
			require.Nil(t, err)
			assert.Len(t, res.Payload, 20)
			for _, r := range res.Payload {
				assert.Nil(t, r.Result.Errors)
			}

			for _, upd := range toUpdate {
				obj, err := helper.GetObject(t, upd.Class, upd.ID)
				assert.Nil(t, err)
				if err != nil {
					continue
				}
				assert.NotNil(t, obj.Properties)
				assert.IsType(t, map[string]interface{}{}, obj.Properties)
				name := obj.Properties.(map[string]interface{})["name"]
				assert.Equal(t, "updated!", name)
			}
		}
	})

	t.Run("fail to update non-existing objects, but update the existing ones", func(t *testing.T) {
		var (
			existing         = testObjects[:10]
			nonexisting      = createBatchPatchTestObjects(10)
			existIDLookup    = make(map[strfmt.UUID]struct{})
			nonexistIDLookup = func() map[strfmt.UUID]bool {
				m := make(map[strfmt.UUID]bool, len(nonexisting))
				for _, obj := range nonexisting {
					m[obj.ID] = false
				}
				return m
			}()
		)

		toUpdate := append(existing, nonexisting...)
		res, err := doBatchPatch(t, toUpdate)
		require.Nil(t, err)
		require.Len(t, res.Payload, 20)

		for _, r := range res.Payload {
			if r.Result.Errors != nil {
				_, ok := nonexistIDLookup[r.ID]
				require.True(t, ok)
				nonexistIDLookup[r.ID] = true
			} else {
				existIDLookup[r.ID] = struct{}{}
			}
		}

		for _, found := range nonexistIDLookup {
			assert.True(t, found)
		}
		assert.Len(t, existIDLookup, 10)
	})

	t.Run("fail to update objects with non-existing properties", func(t *testing.T) {
		toUpdate := func() []*models.Object {
			res := make([]*models.Object, 5)
			for i, obj := range testObjects[50:55] {
				res[i] = &models.Object{
					Class: obj.Class,
					ID:    obj.ID,
					Properties: map[string]interface{}{
						"doesNotExist": "!@#$%",
					},
				}
			}
			return res
		}()

		resp, err := doBatchPatch(t, toUpdate)
		require.Nil(t, err)
		require.Len(t, resp.Payload, len(toUpdate))
		for i, r := range resp.Payload {
			assert.Equal(t, toUpdate[i].ID.String(), r.ID)
			if assert.NotNil(t, r) {
				require.NotNil(t, r.Result.Errors)
				require.Len(t, r.Result.Errors.Error, 1)
				msg := r.Result.Errors.Error[0].Message
				assert.Equal(t, "property \"doesNotExist\" does not exist on class \"BatchTest\"", msg)
			}
		}
	})
}

func createBatchPatchTestObjects(n int) []*models.Object {
	objs := make([]*models.Object, n)
	for i := 0; i < n; i++ {
		objs[i] = &models.Object{
			Class: "BulkTest",
			ID:    strfmt.UUID(uuid.NewString()),
			Properties: map[string]interface{}{
				"index": i,
				"name":  fmt.Sprintf("obj%d", i),
			},
		}
	}
	return objs
}

func doBatchPatch(t *testing.T, objects []*models.Object) (*batch.BatchObjectsMergeOK, error) {
	params := batch.NewBatchObjectsMergeParams().
		WithBody(batch.BatchObjectsMergeBody{Objects: objects})
	return helper.Client(t).Batch.BatchObjectsMerge(params, nil)
}
