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

package multi_node

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func TestCreateClassWhileOneNodeIsDown(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	t.Run("class with MT Enabled", func(t *testing.T) {
		testClass := articles.ParagraphsClass()
		testClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		testClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("stop 3rd node", func(t *testing.T) {
			require.Nil(t, compose.StopAt(context.Background(), 2, nil))
		})

		t.Run("create class", func(t *testing.T) {
			helper.CreateClass(t, testClass)
		})

		t.Run("bring 3rd node back up", func(t *testing.T) {
			require.Nil(t, compose.StartAt(context.Background(), 2))
		})

		t.Run("verify class exists on the 3rd node", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviateNode3().URI())
			require.NotNil(t, helper.GetClass(t, testClass.Class))
		})

		t.Run("delete create class", func(t *testing.T) {
			helper.DeleteClass(t, testClass.Class)
		})
	})

	t.Run("class with MT disabled", func(t *testing.T) {
		testClass := articles.ParagraphsClass()
		testClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: false}
		testClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}

		helper.SetupClient(compose.GetWeaviate().URI())

		t.Run("stop 3rd node", func(t *testing.T) {
			require.Nil(t, compose.StopAt(context.Background(), 2, nil))
		})

		t.Run("create class", func(t *testing.T) {
			helper.CreateClass(t, testClass)
		})

		t.Run("bring 3rd node back up", func(t *testing.T) {
			require.Nil(t, compose.StartAt(context.Background(), 2))
		})

		t.Run("verify class exists on the 3rd node", func(t *testing.T) {
			helper.SetupClient(compose.GetWeaviateNode3().URI())
			require.NotNil(t, helper.GetClass(t, testClass.Class))
		})
	})
}

func TestAddTenantWhileOneNodeIsDown(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	testClass := articles.ParagraphsClass()
	testClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	testClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	tenants := []*models.Tenant{
		{Name: "Tenant_1", ActivityStatus: models.TenantActivityStatusHOT},
		{Name: "Tenant_2", ActivityStatus: models.TenantActivityStatusCOLD},
	}

	helper.SetupClient(compose.GetWeaviateNode2().URI())

	t.Run("create class", func(t *testing.T) {
		helper.CreateClass(t, testClass)
	})

	t.Run("stop 3rd node", func(t *testing.T) {
		require.Nil(t, compose.StopAt(context.Background(), 2, nil))
	})

	t.Run("Add Tenant", func(t *testing.T) {
		helper.CreateTenants(t, testClass.Class, tenants)
	})

	t.Run("bring 3rd node back up", func(t *testing.T) {
		require.Nil(t, compose.StartAt(context.Background(), 2))
	})

	t.Run("verify tenant exists on the 3rd node", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviateNode3().URI())
		helper.TenantExists(t, testClass.Class, tenants[0].Name)
	})
}

func TestAddObjectsWhileOneNodeIsDown(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	testClass := articles.ParagraphsClass()
	testClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	testClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	tenants := []*models.Tenant{
		{Name: "Tenant_1", ActivityStatus: models.TenantActivityStatusHOT},
		{Name: "Tenant_2", ActivityStatus: models.TenantActivityStatusCOLD},
	}

	helper.SetupClient(compose.GetWeaviateNode2().URI())

	t.Run("create class", func(t *testing.T) {
		helper.CreateClass(t, testClass)
	})

	t.Run("stop 3rd node", func(t *testing.T) {
		require.Nil(t, compose.StopAt(context.Background(), 2, nil))
	})

	t.Run("Add Tenant", func(t *testing.T) {
		helper.CreateTenants(t, testClass.Class, tenants)
	})

	paragraphIDs := []strfmt.UUID{
		strfmt.UUID("3bf331ac-8c86-4f95-b127-2f8f96bbc093"),
		strfmt.UUID("47b26ba1-6bc9-41f8-a655-8b9a5b60e1a3"),
	}

	t.Run("Add objects", func(t *testing.T) {
		batch := make([]*models.Object, len(paragraphIDs))
		for idx, id := range paragraphIDs {
			batch[idx] = articles.NewParagraph().
				WithID(id).
				WithContents(fmt.Sprintf("paragraph#%d", idx)).
				WithTenant(tenants[0].Name).
				Object()
		}
		helper.CreateObjectsBatch(t, batch)
	})

	t.Run("bring 3rd node back up", func(t *testing.T) {
		require.Nil(t, compose.StartAt(context.Background(), 2))
	})

	t.Run("verify object created the 3rd node", func(t *testing.T) {
		helper.SetupClient(compose.GetWeaviateNode3().URI())
		for idx, id := range paragraphIDs {
			res, err := helper.TenantObject(t, testClass.Class, id, tenants[0].Name)
			require.Nil(t, err)
			require.Equal(t, id, res.ID)
			require.Equal(t, helper.ObjectContentsProp(fmt.Sprintf("paragraph#%d", idx)), res.Properties)
		}
	})
}
