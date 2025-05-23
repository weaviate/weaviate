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

package recovery

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated"
	"github.com/weaviate/weaviate/test/acceptance_v2/dedicated/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestUpdatePropertyFieldFailureWithRestart(t *testing.T) {
	t.Parallel()

	compose := dedicated.SetupDedicated(t, func(t *testing.T) *docker.Compose {
		return docker.New().With1NodeCluster()
	})

	className := "C2"
	propName := "p1"
	nestedPropName := "np1"
	ctx := t.Context()
	client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
	delete := func() {
		client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
		params := schema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := client.Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
	}
	defer delete()

	params := schema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{
		Class: className,
		Properties: []*models.Property{
			{
				Name:     propName,
				DataType: []string{"object"},
				NestedProperties: []*models.NestedProperty{{
					Name:     nestedPropName,
					DataType: []string{"text"},
				}},
			},
		},
	})

	_, err := client.Schema.SchemaObjectsCreate(params, nil)
	assert.Nil(t, err)

	newDescription := "its updated description"

	t.Run("update property and nested property data type and shall fail", func(t *testing.T) {
		params := schema.NewSchemaObjectsGetParams().WithClassName(className)

		res, err := client.Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, "", res.Payload.Properties[0].Description)

		prop := res.Payload.Properties[0]
		prop.Description = newDescription
		prop.NestedProperties[0].Description = newDescription
		prop.NestedProperties[0].Name = "faulty-np2"
		prop.NestedProperties[0].DataType = []string{"boolean"}
		updateParams := schema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(&models.Class{
				Class:      className,
				Properties: []*models.Property{prop},
			})
		_, err = client.Schema.SchemaObjectsUpdate(updateParams, nil)
		require.Error(t, err)

		helper.AssertRequestFail(t, nil, err, func() {
			var errResponse *schema.SchemaObjectsUpdateUnprocessableEntity
			require.True(t, errors.As(err, &errResponse))
			require.Contains(t, errResponse.Payload.Error[0].Message, "property fields other than description cannot be updated through updating the class")
		})
	})

	t.Run("restart node", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 0)
		common.StartNodeAt(ctx, t, compose, 0)
	})

	t.Run("verify node is running after faulty schema update", func(t *testing.T) {
		client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
		require.NotNil(t, helper.GetClassWithClient(t, client, className))
	})

	t.Run("create new class to make sure schema updates work even with previous failure", func(t *testing.T) {
		client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
		newClass := &models.Class{
			Class: "NewClass",
			Properties: []*models.Property{
				{
					Name:     "p1",
					DataType: []string{"text"},
				},
			},
		}
		helper.CreateClassWithClient(t, client, newClass)
		returnedClass := helper.GetClassWithClient(t, client, newClass.Class)
		require.NotNil(t, returnedClass)
		require.Equal(t, newClass.Class, returnedClass.Class)
		require.Equal(t, newClass.Properties[0].Name, returnedClass.Properties[0].Name)
		require.Equal(t, newClass.Properties[0].DataType, returnedClass.Properties[0].DataType)
	})

	t.Run("restart node again", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 0)
		common.StartNodeAt(ctx, t, compose, 0)
	})

	t.Run("verify node is running after faulty and valid schema update", func(t *testing.T) {
		client := helper.ClientFromURI(t, compose.GetWeaviate().URI())
		require.NotNil(t, helper.GetClassWithClient(t, client, className))
		require.NotNil(t, helper.GetClassWithClient(t, client, "NewClass"))
	})
}
