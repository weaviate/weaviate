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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/cluster"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func TestAuthzNodes(t *testing.T) {
	adminUser := "existing-user"
	adminKey := "existing-key"
	adminRole := "admin"

	customUser := "custom-user"
	customKey := "custom-key"
	customRole := "custom"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.
		New().
		WithWeaviate().
		WithRBAC().
		WithRbacUser(adminUser, adminKey, adminRole).
		WithRbacUser(customUser, customKey, customRole).
		Start(ctx)

	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	clsA := articles.ArticlesClass()

	t.Run("setup", func(t *testing.T) {
		helper.CreateClassAuth(t, articles.ParagraphsClass(), adminKey)
		helper.CreateClassAuth(t, clsA, adminKey)
		helper.CreateObjectsBatchAuth(t, []*models.Object{articles.NewArticle().WithTitle("article1").Object()}, adminKey)
	})

	t.Run("fail to get node status without read_cluster", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*nodes.NodesGetForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("fail to get cluster stats without read_cluster", func(t *testing.T) {
		_, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*cluster.ClusterGetStatisticsForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add read_cluster to custom role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, customRole, helper.NewClusterPermission().WithAction(authorization.ReadCluster).Permission())
	})

	t.Run("get minimal node status with read_cluster", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Nodes, 1)
	})

	t.Run("get cluster stats with read_cluster", func(t *testing.T) {
		resp, err := helper.Client(t).Cluster.ClusterGetStatistics(cluster.NewClusterGetStatisticsParams(), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Statistics, 1)
	})

	t.Run("fail to get verbose node status by class without read_data on class", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(clsA.Class).WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*nodes.NodesGetClassForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add read_data on class to custom role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, customRole, helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection(clsA.Class).Permission())
	})

	t.Run("get verbose node status by class with read_data on class", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithClassName(clsA.Class).WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Nodes, 1)
	})

	t.Run("fail to get verbose node status on all classes without read_data on *", func(t *testing.T) {
		_, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.NotNil(t, err)
		parsed, forbidden := err.(*nodes.NodesGetForbidden)
		require.True(t, forbidden)
		require.Contains(t, parsed.Payload.Error[0].Message, "forbidden")
	})

	t.Run("add read_data on * to custom role", func(t *testing.T) {
		helper.AddPermissions(t, adminKey, customRole, helper.NewDataPermission().WithAction(authorization.ReadData).WithCollection("*").Permission())
	})

	t.Run("get verbose node status on all classes with read_data on *", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithOutput(String("verbose")), helper.CreateAuth(customKey))
		require.Nil(t, err)
		require.Len(t, resp.Payload.Nodes, 1)
	})
}
