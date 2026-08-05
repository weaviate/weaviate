//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func Test_AutoTenantActivation_SingleNode(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateWithGRPC().
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true, AutoTenantActivation: true, AutoTenantCreation: true}
	helper.CreateClass(t, cls)

	tenant := "tenant"
	paragraph := articles.NewParagraph().WithTenant(tenant)
	helper.CreateObject(t, paragraph.Object())

	t.Run("perform meta count aggregation on deactivated tenant", func(t *testing.T) {
		helper.DeactivateTenants(t, cls.Class, []string{tenant})
		res, err := helper.ClientGRPC(t).Aggregate(ctx, &protocol.AggregateRequest{Collection: cls.Class, ObjectsCount: true, Tenant: tenant})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, int(*res.GetSingleResult().ObjectsCount), 1)
	})

	t.Run("perform fetch objects search on deactivated tenant", func(t *testing.T) {
		helper.DeactivateTenants(t, cls.Class, []string{tenant})
		res, err := helper.ClientGRPC(t).Search(ctx, &protocol.SearchRequest{Collection: cls.Class, Tenant: tenant})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, len(res.GetResults()), 1)
	})
}

func Test_AutoTenantActivation_Cluster(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	require.Nil(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	cls := articles.ParagraphsClass()
	cls.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true, AutoTenantActivation: true, AutoTenantCreation: true}
	cls.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	helper.CreateClass(t, cls)

	tenant := "tenant"
	paragraph := articles.NewParagraph().WithTenant(tenant)
	helper.CreateObject(t, paragraph.Object())

	t.Run("perform meta count aggregation on deactivated tenant", func(t *testing.T) {
		helper.DeactivateTenants(t, cls.Class, []string{tenant})
		res, err := helper.ClientGRPC(t).Aggregate(ctx, &protocol.AggregateRequest{Collection: cls.Class, ObjectsCount: true, Tenant: tenant})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, int(*res.GetSingleResult().ObjectsCount), 1)
	})

	t.Run("perform fetch objects search on deactivated tenant", func(t *testing.T) {
		helper.DeactivateTenants(t, cls.Class, []string{tenant})
		res, err := helper.ClientGRPC(t).Search(ctx, &protocol.SearchRequest{Collection: cls.Class, Tenant: tenant})
		require.NoError(t, err)
		require.NotNil(t, res)
		require.Equal(t, len(res.GetResults()), 1)
	})
}
