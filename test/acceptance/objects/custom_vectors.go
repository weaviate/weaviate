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

package test

import (
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func customVectors(t *testing.T) {
	var id strfmt.UUID

	t.Run("create object", func(t *testing.T) {
		params := objects.NewObjectsCreateParams().WithBody(
			&models.Object{
				Class:      "TestObjectCustomVector",
				Properties: map[string]interface{}{"description": "foo"},
				Vector:     []float32{0.1, 0.2},
			})
		resp, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
		require.Nil(t, err, "creation should succeed")
		id = resp.Payload.ID
	})

	t.Run("check custom vector is set", func(t *testing.T) {
		include := "vector"
		params := objects.NewObjectsGetParams().WithID(id).WithInclude(&include)
		resp, err := helper.Client(t).Objects.ObjectsGet(params, nil)
		require.Nil(t, err, "get should succeed")
		assert.Equal(t, []float32{0.1, 0.2}, []float32(resp.Payload.Vector))
	})

	t.Run("replace object entirely (update)", func(t *testing.T) {
		params := objects.NewObjectsUpdateParams().WithID(id).WithBody(&models.Object{
			ID:         id,
			Class:      "TestObjectCustomVector",
			Properties: map[string]interface{}{"description": "foo updated"},
			Vector:     []float32{0.1, 0.3},
		})
		_, err := helper.Client(t).Objects.ObjectsUpdate(params, nil)
		require.Nil(t, err, "update should succeed")
	})

	t.Run("check custom vector is updated", func(t *testing.T) {
		include := "vector"
		params := objects.NewObjectsGetParams().WithID(id).WithInclude(&include)
		resp, err := helper.Client(t).Objects.ObjectsGet(params, nil)
		require.Nil(t, err, "get should succeed")
		assert.Equal(t, []float32{0.1, 0.3}, []float32(resp.Payload.Vector))
	})

	t.Run("replace only vector through merge", func(t *testing.T) {
		params := objects.NewObjectsPatchParams().WithID(id).WithBody(&models.Object{
			ID:         id,
			Class:      "TestObjectCustomVector",
			Properties: map[string]interface{}{},
			Vector:     []float32{0.4, 0.3},
		})
		_, err := helper.Client(t).Objects.ObjectsPatch(params, nil)
		if err != nil {
			spew.Dump(err.(*objects.ObjectsPatchInternalServerError).Payload.Error[0])
		}
		require.Nil(t, err, "patch should succeed")
	})

	t.Run("check custom vector is updated", func(t *testing.T) {
		include := "vector"
		params := objects.NewObjectsGetParams().WithID(id).WithInclude(&include)
		resp, err := helper.Client(t).Objects.ObjectsGet(params, nil)
		require.Nil(t, err, "get should succeed")
		assert.Equal(t, []float32{0.4, 0.3}, []float32(resp.Payload.Vector))
	})
}
