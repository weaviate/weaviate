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

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/helper"
)

func TestCreateTenants(t *testing.T) {
	tenantKey := "tenantName"
	testClass := models.Class{
		Class: "MultiTenantClass",
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

	defer func() {
		helper.DeleteClass(t, testClass.Class)
	}()
	helper.CreateClass(t, &testClass)

	t.Run("create tenants", func(t *testing.T) {
		tenants := []*models.Tenant{
			{Name: "Tenant1"},
			{Name: "Tenant2"},
			{Name: "Tenant3"},
		}
		helper.CreateTenants(t, testClass.Class, tenants)
	})
	// TODO: Get tenants to verify creation, when endpoint available.
	//       Tenant creation has been manually verified for now
}
