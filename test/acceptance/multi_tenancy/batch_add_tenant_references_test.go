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
	className := "MultiTenantClass"
	tenantKey := "tenantName"
	tenantName := "Tenant1"
	refProp := "relatedTo"
	testClass := models.Class{
		Class: className,
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
				Name:     refProp,
				DataType: []string{className},
			},
		},
	}
	tenantObjects := []*models.Object{
		{
			ID:    "0927a1e0-398e-4e76-91fb-04a7a8f0405c",
			Class: className,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
		{
			ID:    "831ae1d0-f441-44b1-bb2a-46548048e26f",
			Class: className,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
		{
			ID:    "6f3363e0-c0a0-4618-bf1f-b6cad9cdff59",
			Class: className,
			Properties: map[string]interface{}{
				tenantKey: tenantName,
			},
		},
	}

	defer func() {
		helper.DeleteClass(t, className)
	}()

	t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
		helper.CreateClass(t, &testClass)
	})

	t.Run("create tenants", func(t *testing.T) {
		tenants := []*models.Tenant{{Name: tenantName}}
		helper.CreateTenants(t, className, tenants)
	})

	t.Run("add tenant objects", func(t *testing.T) {
		for _, obj := range tenantObjects {
			helper.CreateTenantObject(t, obj, tenantName)
		}

		t.Run("verify object creation", func(t *testing.T) {
			for _, obj := range tenantObjects {
				resp, err := helper.TenantObject(t, obj.Class, obj.ID, tenantName)
				require.Nil(t, err)
				require.Equal(t, obj.Class, resp.Class)
				require.Equal(t, obj.Properties, resp.Properties)
			}
		})
	})

	t.Run("add tenant references", func(t *testing.T) {
		refs := []*models.BatchReference{
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className),
					schema.PropertyName(refProp), tenantObjects[0].ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className, tenantObjects[0].ID).String()),
			},
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className),
					schema.PropertyName(refProp), tenantObjects[0].ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className, tenantObjects[1].ID).String()),
			},
			{
				From: strfmt.URI(crossref.NewSource(schema.ClassName(className),
					schema.PropertyName(refProp), tenantObjects[0].ID).String()),
				To: strfmt.URI(crossref.NewLocalhost(className, tenantObjects[2].ID).String()),
			},
		}
		helper.AddTenantReferences(t, refs, tenantName)
	})

	t.Run("verify object references", func(t *testing.T) {
		resp, err := helper.TenantObject(t, tenantObjects[0].Class, tenantObjects[0].ID, tenantName)
		require.Nil(t, err)
		require.Equal(t, tenantObjects[0].Class, resp.Class)
		require.Equal(t, tenantObjects[0].ID, resp.ID)
		relatedTo := resp.Properties.(map[string]interface{})[refProp].([]interface{})
		require.Len(t, relatedTo, 3)
		for i, rel := range relatedTo {
			beacon := rel.(map[string]interface{})["beacon"].(string)
			assert.Equal(t, helper.NewBeacon(className, tenantObjects[i].ID), strfmt.URI(beacon))
		}
	})
}
