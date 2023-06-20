//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/entities/schema/crossref"
	"github.com/weaviate/weaviate/test/helper"
)

func TestBatchAddTenantReferences(t *testing.T) {
	className1 := "MultiTenantClass1"
	className2 := "MultiTenantClass2"
	className3 := "SingleTenantClass1"
	className4 := "SingleTenantClass2"
	tenantKey := "tenantName"
	tenantName1 := "Tenant1"
	tenantName2 := "Tenant2"
	mtRefProp1 := "relatedToMT1"
	mtRefProp2 := "relatedToMT2"
	stRefProp := "relatedToST"
	mtClass1 := models.Class{
		Class: className1,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			},
			{
				Name:     mtRefProp1,
				DataType: []string{className1},
			},
			{
				Name:     mtRefProp2,
				DataType: []string{className2},
			},
			{
				Name:     stRefProp,
				DataType: []string{className3},
			},
		},
	}
	mtClass2 := models.Class{
		Class: className2,
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled:   true,
			TenantKey: tenantKey,
		},
		Properties: []*models.Property{
			{
				Name:     tenantKey,
				DataType: []string{"string"},
			},
		},
	}
	stClass1 := models.Class{
		Class: className3,
		Properties: []*models.Property{
			{
				Name:     "stringProp",
				DataType: []string{"string"},
			},
		},
	}
	stClass2 := models.Class{
		Class: className4,
		Properties: []*models.Property{
			{
				Name:     mtRefProp1,
				DataType: []string{className1},
			},
		},
	}
	mtObject1 := &models.Object{
		ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
		Class: className1,
		Properties: map[string]interface{}{
			tenantKey: tenantName1,
		},
	}
	mtObject2DiffTenant := &models.Object{
		ID:    "af90a7e3-53b3-4eb0-b395-10a04d217263",
		Class: className2,
		Properties: map[string]interface{}{
			tenantKey: tenantName2,
		},
	}
	mtObject2SameTenant := &models.Object{
		ID:    "4076df6b-0767-43a9-a0a4-2ec153bf262e",
		Class: className2,
		Properties: map[string]interface{}{
			tenantKey: tenantName1,
		},
	}
	stObject1 := &models.Object{
		ID:    "bea841c7-d689-4526-8af3-56c44b44274a",
		Class: className3,
		Properties: map[string]interface{}{
			"stringProp": "123",
		},
	}
	stObject2 := &models.Object{
		ID:    "744f869a-7dcb-4fb5-8b0a-73075da1e116",
		Class: className4,
	}

	defer func() {
		helper.DeleteClass(t, className1)
		helper.DeleteClass(t, className2)
		helper.DeleteClass(t, className3)
		helper.DeleteClass(t, className4)
	}()

	t.Run("create classes", func(t *testing.T) {
		helper.CreateClass(t, &stClass1)
		helper.CreateClass(t, &mtClass2)
		helper.CreateClass(t, &mtClass1)
		helper.CreateClass(t, &stClass2)
	})

	t.Run("create tenants", func(t *testing.T) {
		helper.CreateTenants(t, className1, []*models.Tenant{{Name: tenantName1}})
		helper.CreateTenants(t, className2, []*models.Tenant{{Name: tenantName1}})
		helper.CreateTenants(t, className2, []*models.Tenant{{Name: tenantName2}})
	})

	t.Run("add tenant objects", func(t *testing.T) {
		helper.CreateTenantObject(t, mtObject1, tenantName1)
		helper.CreateTenantObject(t, mtObject2DiffTenant, tenantName2)
		helper.CreateTenantObject(t, mtObject2SameTenant, tenantName1)
		helper.CreateObject(t, stObject1)
		helper.CreateObject(t, stObject2)

		t.Run("verify objects creation", func(t *testing.T) {
			resp, err := helper.TenantObject(t, mtObject1.Class, mtObject1.ID, tenantName1)
			require.Nil(t, err)
			require.Equal(t, mtObject1.Class, resp.Class)
			require.Equal(t, mtObject1.Properties, resp.Properties)

			resp, err = helper.TenantObject(t, mtObject2DiffTenant.Class, mtObject2DiffTenant.ID, tenantName2)
			require.Nil(t, err)
			require.Equal(t, mtObject2DiffTenant.Class, resp.Class)
			require.Equal(t, mtObject2DiffTenant.Properties, resp.Properties)

			resp, err = helper.TenantObject(t, mtObject2SameTenant.Class, mtObject2SameTenant.ID, tenantName1)
			require.Nil(t, err)
			require.Equal(t, mtObject2SameTenant.Class, resp.Class)
			require.Equal(t, mtObject2SameTenant.Properties, resp.Properties)

			resp, err = helper.GetObject(t, stObject1.Class, stObject1.ID)
			require.Nil(t, err)
			require.Equal(t, stObject1.Class, resp.Class)
		})
	})

	t.Run("add tenant reference - same class and tenant", func(t *testing.T) {
		refs := []*models.BatchReference{
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className1),
					schema.PropertyName(mtRefProp1), mtObject1.ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className1, mtObject1.ID).String()),
			},
		}
		resp, err := helper.AddTenantReferences(t, refs, tenantName1)
		helper.CheckReferencesBatchResponse(t, resp, err)

		t.Run("verify object references", func(t *testing.T) {
			resp, err := helper.TenantObject(t, mtObject1.Class, mtObject1.ID, tenantName1)
			require.Nil(t, err)
			require.Equal(t, mtObject1.Class, resp.Class)
			require.Equal(t, mtObject1.ID, resp.ID)
			relatedTo := resp.Properties.(map[string]interface{})[mtRefProp1].([]interface{})
			require.Len(t, relatedTo, 1)
			beacon := relatedTo[0].(map[string]interface{})["beacon"].(string)
			assert.Equal(t, helper.NewBeacon(className1, mtObject1.ID), strfmt.URI(beacon))
		})
	})

	t.Run("add tenant reference - different MT class same tenant", func(t *testing.T) {
		refs := []*models.BatchReference{
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className1),
					schema.PropertyName(mtRefProp2), mtObject1.ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className2, mtObject2SameTenant.ID).String()),
			},
		}
		resp, err := helper.AddTenantReferences(t, refs, tenantName1)
		helper.CheckReferencesBatchResponse(t, resp, err)

		t.Run("verify object references", func(t *testing.T) {
			resp, err := helper.TenantObject(t, mtObject1.Class, mtObject1.ID, tenantName1)
			require.Nil(t, err)
			require.Equal(t, mtObject1.Class, resp.Class)
			require.Equal(t, mtObject1.ID, resp.ID)
			relatedTo := resp.Properties.(map[string]interface{})[mtRefProp2].([]interface{})
			require.Len(t, relatedTo, 1)
			beacon := relatedTo[0].(map[string]interface{})["beacon"].(string)
			assert.Equal(t, helper.NewBeacon(className2, mtObject2SameTenant.ID), strfmt.URI(beacon))
		})
	})

	t.Run("add tenant reference - different MT class and tenant", func(t *testing.T) {
		refs := []*models.BatchReference{
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className1),
					schema.PropertyName(mtRefProp2), mtObject1.ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className2, mtObject2DiffTenant.ID).String()),
			},
		}

		resp, err := helper.AddTenantReferences(t, refs, tenantName1)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Empty(t, resp[0].To)
		require.Empty(t, resp[0].From)
		require.NotNil(t, resp[0].Result)
		require.NotNil(t, resp[0].Result.Errors)
		require.Len(t, resp[0].Result.Errors.Error, 1)
		require.NotNil(t, resp[0].Result.Errors.Error[0])
		expectedMsg := fmt.Sprintf(`target object %s/%s not found for tenant %q`,
			className2, mtObject2DiffTenant.ID, tenantName1)
		assert.Equal(t, expectedMsg, resp[0].Result.Errors.Error[0].Message)
	})

	t.Run("add tenant reference - from MT class to single tenant class", func(t *testing.T) {
		refs := []*models.BatchReference{
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className1),
					schema.PropertyName(stRefProp), mtObject1.ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className3, stObject1.ID).String()),
			},
		}
		resp, err := helper.AddTenantReferences(t, refs, tenantName1)
		helper.CheckReferencesBatchResponse(t, resp, err)

		t.Run("verify object references", func(t *testing.T) {
			resp, err := helper.TenantObject(t, mtObject1.Class, mtObject1.ID, tenantName1)
			require.Nil(t, err)
			require.Equal(t, mtObject1.Class, resp.Class)
			require.Equal(t, mtObject1.ID, resp.ID)
			relatedTo := resp.Properties.(map[string]interface{})[stRefProp].([]interface{})
			require.Len(t, relatedTo, 1)
			beacon := relatedTo[0].(map[string]interface{})["beacon"].(string)
			assert.Equal(t, helper.NewBeacon(className3, stObject1.ID), strfmt.URI(beacon))
		})
	})

	t.Run("add tenant reference - from single tenant class to MT class", func(t *testing.T) {
		refs := []*models.BatchReference{
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className4),
					schema.PropertyName(mtRefProp1), stObject2.ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className1, mtObject1.ID).String()),
			},
		}

		resp, err := helper.AddReferences(t, refs)
		require.Nil(t, err)
		require.NotNil(t, resp)
		require.Len(t, resp, 1)
		require.Empty(t, resp[0].To)
		require.Empty(t, resp[0].From)
		require.NotNil(t, resp[0].Result)
		require.NotNil(t, resp[0].Result.Errors)
		require.Len(t, resp[0].Result.Errors.Error, 1)
		require.NotNil(t, resp[0].Result.Errors.Error[0])
		expectedMsg := "invalid reference: cannot reference a multi-tenant enabled class from a non multi-tenant enabled class"
		assert.Equal(t, expectedMsg, resp[0].Result.Errors.Error[0].Message)
	})
}
