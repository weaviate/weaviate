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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/batch"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestClassMultiTenancyDisabled(t *testing.T) {
	testClass := models.Class{
		Class: "ClassDisableMultiTenancy",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: false,
		},
	}
	objUUID := strfmt.UUID("0927a1e0-398e-4e76-91fb-04a7a8f0405c")

	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	helper.CreateObjectsBatch(t, []*models.Object{{
		ID:    objUUID,
		Class: testClass.Class,
	}})

	object, err := helper.GetObject(t, testClass.Class, objUUID)
	require.Nil(t, err)
	require.NotNil(t, object)
	require.Equal(t, objUUID, object.ID)
}

func TestClassMultiTenancyDisabledSchemaPrint(t *testing.T) {
	testClass := models.Class{Class: "ClassDisableMultiTenancy"}
	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	classReturn := helper.GetClass(t, testClass.Class)
	require.NotNil(t, classReturn.MultiTenancyConfig)
}

func TestClassMultiTenancyToggleAutoTenant(t *testing.T) {
	createObjectToCheckAutoTenant := func(t *testing.T, object *models.Object) *models.ObjectsGetResponse {
		t.Helper()
		params := batch.NewBatchObjectsCreateParams().
			WithBody(batch.BatchObjectsCreateBody{
				Objects: []*models.Object{object},
			})
		resp, err := helper.Client(t).Batch.BatchObjectsCreate(params, nil)
		helper.AssertRequestOk(t, resp, err, nil)
		require.NotNil(t, resp)
		require.Len(t, resp.Payload, 1)
		return resp.Payload[0]
	}

	testClass := models.Class{
		Class: "AutoTenantToggle",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			AutoTenantCreation: false,
			Enabled:            true,
		},
	}
	helper.CreateClass(t, &testClass)
	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	t.Run("autotenant not set, fail to create object with nonexistent tenant", func(t *testing.T) {
		resp := createObjectToCheckAutoTenant(t,
			&models.Object{
				Class: "AutoTenantToggle",
				Properties: map[string]interface{}{
					"stringProp": "value",
				},
				Tenant: "non-existent",
			},
		)

		require.NotNil(t, resp.Result)
		require.NotNil(t, resp.Result.Errors)
		require.Len(t, resp.Result.Errors.Error, 1)
		assert.Equal(t, resp.Result.Errors.Error[0].Message, `tenant not found: "non-existent"`)
	})

	t.Run("autotenant toggled on, successfully create object", func(t *testing.T) {
		fetched := helper.GetClass(t, testClass.Class)
		fetched.MultiTenancyConfig.AutoTenantCreation = true
		helper.UpdateClass(t, fetched)

		resp := createObjectToCheckAutoTenant(t,
			&models.Object{
				Class: "AutoTenantToggle",
				Properties: map[string]interface{}{
					"stringProp": "value",
				},
				Tenant: "now-exists",
			},
		)

		require.NotNil(t, resp.Result)
		require.Nil(t, resp.Result.Errors)
		success := "SUCCESS"
		assert.EqualValues(t, resp.Result.Status, &success)
	})

	t.Run("autotenant toggled back off, fail to create object with nonexistent tenant", func(t *testing.T) {
		fetched := helper.GetClass(t, testClass.Class)
		fetched.MultiTenancyConfig.AutoTenantCreation = false
		helper.UpdateClass(t, fetched)

		resp := createObjectToCheckAutoTenant(t,
			&models.Object{
				Class: "AutoTenantToggle",
				Properties: map[string]interface{}{
					"stringProp": "value",
				},
				Tenant: "still-nonexistent",
			},
		)

		require.NotNil(t, resp.Result)
		require.NotNil(t, resp.Result.Errors)
		require.Len(t, resp.Result.Errors.Error, 1)
		assert.Equal(t, resp.Result.Errors.Error[0].Message, `tenant not found: "still-nonexistent"`)
	})
}
