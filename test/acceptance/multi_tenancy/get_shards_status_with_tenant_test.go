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

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestGetShardsStatusWithTenant(t *testing.T) {
	testClass := models.Class{
		Class: "ClassGetShardsStatusWithTenant",
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
	}

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()

	helper.CreateClass(t, &testClass)
	helper.CreateTenants(t, testClass.Class, []*models.Tenant{
		{
			Name: "tenant1",
		},
		{
			Name: "tenant2",
		},
	})

	t.Run("get shards status with tenant string", func(t *testing.T) {
		tenant := "tenant1"
		client := helper.Client(t)
		res, err := client.Schema.SchemaObjectsShardsGet(
			schema.
				NewSchemaObjectsShardsGetParams().
				WithClassName(testClass.Class).
				WithTenant(&tenant),
			nil,
		)
		helper.AssertRequestOk(t, res, err, nil)
	})

	t.Run("get shards status with empty tenant string", func(t *testing.T) {
		tenant := ""
		client := helper.Client(t)
		res, err := client.Schema.SchemaObjectsShardsGet(
			schema.
				NewSchemaObjectsShardsGetParams().
				WithClassName(testClass.Class).
				WithTenant(&tenant),
			nil,
		)
		helper.AssertRequestOk(t, res, err, nil)
	})

	t.Run("get shards status with nil pointer", func(t *testing.T) {
		client := helper.Client(t)
		res, err := client.Schema.SchemaObjectsShardsGet(
			schema.
				NewSchemaObjectsShardsGetParams().
				WithClassName(testClass.Class).
				WithTenant(nil),
			nil,
		)
		helper.AssertRequestOk(t, res, err, nil)
	})
}
