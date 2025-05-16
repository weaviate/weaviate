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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestUpdatePropertyFieldFailureWithRestart(t *testing.T) {
	className := "C2"
	propName := "p1"
	nestedPropName := "np1"
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With1NodeCluster().
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	delete := func() {
		params := clschema.NewSchemaObjectsDeleteParams().WithClassName(className)
		_, err := helper.Client(t).Schema.SchemaObjectsDelete(params, nil)
		assert.Nil(t, err)
	}
	defer delete()

	params := clschema.NewSchemaObjectsCreateParams().WithObjectClass(&models.Class{
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

	_, err = helper.Client(t).Schema.SchemaObjectsCreate(params, nil)
	assert.Nil(t, err)

	newDescription := "its updated description"

	t.Run("update property and nested property data type and shall fail", func(t *testing.T) {
		params := clschema.NewSchemaObjectsGetParams().
			WithClassName(className)

		res, err := helper.Client(t).Schema.SchemaObjectsGet(params, nil)
		require.Nil(t, err)
		assert.Equal(t, "", res.Payload.Properties[0].Description)

		prop := res.Payload.Properties[0]
		prop.Description = newDescription
		prop.NestedProperties[0].Description = newDescription
		prop.NestedProperties[0].Name = "faulty-np2"
		prop.NestedProperties[0].DataType = []string{"boolean"}
		updateParams := clschema.NewSchemaObjectsUpdateParams().
			WithClassName(className).
			WithObjectClass(&models.Class{
				Class:      className,
				Properties: []*models.Property{prop},
			})
		_, err = helper.Client(t).Schema.SchemaObjectsUpdate(updateParams, nil)
		require.Error(t, err)

		helper.AssertRequestFail(t, nil, err, func() {
			errResponse, ok := err.(*schema.SchemaObjectsUpdateUnprocessableEntity)
			require.True(t, ok)
			require.Contains(t, errResponse.Payload.Error[0].Message, "property fields other than description cannot be updated through updating the class")
		})
	})

	t.Run("restart node", func(t *testing.T) {
		common.StopNodeAt(ctx, t, compose, 0)
		common.StartNodeAt(ctx, t, compose, 0)
	})

	t.Run("verify node is running after faulty schema update", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviate().URI())
		require.NotNil(t, helper.GetClass(t, className))
	})
}
