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
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	client "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func Test_Offload_When_Not_Enabled(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	compose, err := docker.New().
		WithText2VecContextionary().
		With3NodeCluster().
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	className := "MultiTenantClass"
	testClass := models.Class{
		Class:             className,
		ReplicationConfig: &models.ReplicationConfig{Factor: 2},
		MultiTenancyConfig: &models.MultiTenancyConfig{
			Enabled: true,
		},
		Properties: []*models.Property{
			{
				Name:     "name",
				DataType: schema.DataTypeText.PropString(),
			},
		},
	}
	t.Run("create class with multi-tenancy enabled", func(t *testing.T) {
		helper.CreateClass(t, &testClass)
	})

	tenantNames := []string{
		"Tenant1", "Tenant2", "Tenant3",
	}
	tenants := make([]*models.Tenant, len(tenantNames))

	t.Run("create tenants", func(t *testing.T) {
		for i := range tenants {
			tenants[i] = &models.Tenant{Name: tenantNames[i]}
		}
		helper.CreateTenants(t, className, tenants)
	})

	tenantObjects := make([]*models.Object, len(tenantNames))
	for i := range tenantObjects {
		tenantObjects[i] = &models.Object{
			Class: className,
			Properties: map[string]interface{}{
				"name": tenantNames[0],
			},
			Tenant: tenantNames[0],
		}
	}

	defer func() {
		helper.DeleteClass(t, className)
	}()

	t.Run("add tenant objects", func(t *testing.T) {
		for _, obj := range tenantObjects {
			assert.Nil(t, helper.CreateObject(t, obj))
		}
	})

	t.Run("updating tenant status", func(t *testing.T) {
		t.Helper()
		for i := range tenants {
			tenants[i].ActivityStatus = "FROZEN"
		}
		params := client.NewTenantsUpdateParams().
			WithClassName(className).WithBody(tenants)
		_, err := helper.Client(t).Schema.TenantsUpdate(params, nil)
		require.NotNil(t, err)

		tenantErr := &client.TenantsUpdateUnprocessableEntity{}
		if !errors.As(err, &tenantErr) {
			t.Fatalf("expected %T, got: %T", tenantErr, err)
		}
		require.NotNil(t, tenantErr.Payload)
		require.Len(t, tenantErr.Payload.Error, 1)
		msg := tenantErr.Payload.Error[0].Message
		assert.Equal(t, "can't offload tenants, because offload-s3 module is not enabled", msg)
	})
}
