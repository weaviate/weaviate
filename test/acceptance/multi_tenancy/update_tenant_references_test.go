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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestTenantObjectsReferenceUpdates(t *testing.T) {
	className := "MultiTenantClass"
	refProp := "refProp1"

	class := models.Class{
		Class:              className,
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties: []*models.Property{
			{Name: refProp, DataType: []string{className}},
		},
	}
	defer func() {
		helper.DeleteClass(t, className)
	}()
	helper.CreateClass(t, &class)

	tenantNames := []string{"Tenant1", "Tenant2", "Tenant3"}
	tenants := make([]*models.Tenant, len(tenantNames))
	for i := range tenants {
		tenants[i] = &models.Tenant{Name: tenantNames[i]}
	}
	helper.CreateTenants(t, className, tenants)

	objs := make([]*models.Object, 0)
	for i := 0; i < len(tenantNames)*4; i++ {
		obj := &models.Object{
			ID:     strfmt.UUID(uuid.New().String()),
			Class:  className,
			Tenant: tenantNames[i%len(tenantNames)],
		}
		objs = append(objs, obj)
		helper.CreateObject(t, obj)
	}

	for _, obj := range objs {
		ref := &models.SingleRef{Beacon: helper.NewBeacon(className, obj.ID)}
		helper.AddReferenceTenant(t, obj, ref, refProp, obj.Tenant)
		helper.AddReferenceTenant(t, obj, ref, refProp, obj.Tenant)
	}

	for _, obj := range objs {
		resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
		require.Nil(t, err)
		require.Equal(t, obj.ID, resp.ID)
		require.Equal(t, obj.Class, resp.Class)
		refs := resp.Properties.(map[string]interface{})[refProp].([]interface{})
		require.Len(t, refs, 2)
		expectedBeacon := helper.NewBeacon(className, obj.ID).String()
		assert.Equal(t, expectedBeacon, refs[0].(map[string]interface{})["beacon"])
	}

	t.Run("Update reference - single", func(t *testing.T) {
		for i, obj := range objs {
			j := (i + len(tenantNames)) % len(objs) // always reference the next object with the same tenant
			ref := models.MultipleRef{{Beacon: helper.NewBeacon(className, objs[j].ID)}}
			helper.UpdateReferenceTenant(t, obj, ref, refProp, obj.Tenant)
		}

		for i, obj := range objs {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
			require.Nil(t, err)
			require.Equal(t, obj.ID, resp.ID)
			require.Equal(t, obj.Class, resp.Class)
			refs := resp.Properties.(map[string]interface{})[refProp].([]interface{})
			require.Len(t, refs, 1)
			j := (i + len(tenantNames)) % len(objs) // always reference the next object with the same tenant
			expectedBeacon := helper.NewBeacon(className, objs[j].ID).String()
			assert.Equal(t, expectedBeacon, refs[0].(map[string]interface{})["beacon"])
		}
	})

	t.Run("Update reference - multiple", func(t *testing.T) {
		for i, obj := range objs {
			ref := models.MultipleRef{}
			j := (i + 2*len(tenantNames)) % len(objs)
			ref = append(ref, &models.SingleRef{Beacon: helper.NewBeacon(className, objs[j].ID)})
			ref = append(ref, &models.SingleRef{Beacon: helper.NewBeacon(className, objs[j].ID)})
			ref = append(ref, &models.SingleRef{Beacon: helper.NewBeacon(className, objs[j].ID)})
			helper.UpdateReferenceTenant(t, obj, ref, refProp, obj.Tenant)
		}

		for i, obj := range objs {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
			require.Nil(t, err)
			require.Equal(t, obj.ID, resp.ID)
			require.Equal(t, obj.Class, resp.Class)
			refs := resp.Properties.(map[string]interface{})[refProp].([]interface{})
			require.Len(t, refs, 3)
			j := (i + 2*len(tenantNames)) % len(objs) // always reference the next object with the same tenant
			expectedBeacon := helper.NewBeacon(className, objs[j].ID).String()
			assert.Equal(t, expectedBeacon, refs[0].(map[string]interface{})["beacon"])
		}
	})

	t.Run("Update reference - empty", func(t *testing.T) {
		for _, obj := range objs {
			ref := models.MultipleRef{}
			helper.UpdateReferenceTenant(t, obj, ref, refProp, obj.Tenant)
		}

		for _, obj := range objs {
			resp, err := helper.TenantObject(t, obj.Class, obj.ID, obj.Tenant)
			require.Nil(t, err)
			require.Equal(t, obj.ID, resp.ID)
			require.Equal(t, obj.Class, resp.Class)
			refs := resp.Properties.(map[string]interface{})[refProp].([]interface{})
			require.Len(t, refs, 0)
		}
	})
}
