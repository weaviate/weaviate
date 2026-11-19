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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiclient "github.com/weaviate/weaviate/client"
	"github.com/weaviate/weaviate/client/objects"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/storagestate"
	"github.com/weaviate/weaviate/test/docker"
)

func TestShardStatusReportsConditions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		Start(ctx)
	require.NoError(t, err)
	defer func() { _ = compose.Terminate(context.Background()) }()

	const className = "ShardConditions"
	client := newNodeClient(compose.GetWeaviate().URI())
	_, err = client.Schema.SchemaObjectsCreate(clschema.NewSchemaObjectsCreateParams().WithContext(ctx).
		WithObjectClass(&models.Class{
			Class:             className,
			Vectorizer:        "none",
			ReplicationConfig: &models.ReplicationConfig{Factor: 3},
			ShardingConfig:    map[string]interface{}{"desiredCount": 1},
		}), nil)
	require.NoError(t, err)

	shards := func(t require.TestingT) map[string]*models.ShardStatusGetResponse {
		resp, err := client.Schema.SchemaObjectsShardsGet(
			clschema.NewSchemaObjectsShardsGetParams().WithContext(ctx).WithClassName(className), nil)
		require.NoError(t, err)
		byName := make(map[string]*models.ShardStatusGetResponse, len(resp.Payload))
		for _, shard := range resp.Payload {
			byName[shard.Name] = shard
		}
		return byName
	}
	write := func(node *apiclient.Weaviate, level string) error {
		_, err := node.Objects.ObjectsCreate(objects.NewObjectsCreateParams().WithContext(ctx).
			WithBody(&models.Object{Class: className}).WithConsistencyLevel(&level), nil)
		return err
	}
	setStatus := func(shard, status string) error {
		_, err := client.Schema.SchemaObjectsShardsUpdate(clschema.NewSchemaObjectsShardsUpdateParams().
			WithContext(ctx).WithClassName(className).WithShardName(shard).
			WithBody(&models.ShardStatus{Status: status}), nil)
		return err
	}

	var shard string
	for name := range shards(t) {
		shard = name
		break
	}
	require.NotEmpty(t, shard)

	t.Run("only READY and READONLY are accepted", func(t *testing.T) {
		for _, status := range []string{"SHUTDOWN", "INDEXING", "LOADING", "HOT", ""} {
			var invalid *clschema.SchemaObjectsShardsUpdateUnprocessableEntity
			require.True(t, errors.As(setStatus(shard, status), &invalid), "status %q", status)
		}
	})

	t.Run("READONLY holds on every replica", func(t *testing.T) {
		require.NoError(t, setStatus(shard, "readonly"))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			for name, got := range shards(c) {
				require.Len(c, got.PerNodeStatus, 3)
				for node, status := range got.PerNodeStatus {
					if name != shard {
						assert.Empty(c, got.PerNodeConditions[node], "shard %s on %s", name, node)
						continue
					}
					assert.Equal(c, storagestate.StatusReadOnly.String(), status, node)
					assert.Equal(c, []string{storagestate.ConditionUserReadOnly}, got.PerNodeConditions[node], node)
				}
			}
		}, 30*time.Second, time.Second)
	})

	t.Run("a client write fails at every consistency level", func(t *testing.T) {
		follower := newNodeClient(compose.GetWeaviateNode(2).URI())
		for _, level := range []string{"ONE", "QUORUM", "ALL"} {
			require.Error(t, write(follower, level), level)
		}
	})

	t.Run("a restarted node comes back without it", func(t *testing.T) {
		require.NoError(t, compose.StopNode(ctx, 2, nil))
		require.NoError(t, compose.StartNode(ctx, 2))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			got := shards(c)[shard]
			require.Len(c, got.PerNodeStatus, 3)
			var readOnly, cleared int
			for node, status := range got.PerNodeStatus {
				conditions, reported := got.PerNodeConditions[node]
				require.True(c, reported, "node %s did not report", node)
				if status == storagestate.StatusReadOnly.String() {
					assert.Equal(c, []string{storagestate.ConditionUserReadOnly}, conditions, node)
					readOnly++
					continue
				}
				assert.Empty(c, conditions, node)
				cleared++
			}
			assert.Equal(c, 2, readOnly)
			assert.Equal(c, 1, cleared)
		}, 2*time.Minute, 2*time.Second)

		// At ALL the replicas that kept the flag refuse before the level is reached.
		require.Error(t, write(newNodeClient(compose.GetWeaviateNode(3).URI()), "ALL"))
	})

	t.Run("READY lifts it", func(t *testing.T) {
		require.NoError(t, setStatus(shard, "READY"))
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			got := shards(c)[shard]
			for node, status := range got.PerNodeStatus {
				assert.NotEqual(c, storagestate.StatusReadOnly.String(), status, node)
				assert.Empty(c, got.PerNodeConditions[node], node)
			}
		}, 30*time.Second, time.Second)
		require.NoError(t, write(client, "ALL"))
	})

	t.Run("tenants are listed without consistency", func(t *testing.T) {
		const mtClass = "ShardConditionsTenants"
		_, err := client.Schema.SchemaObjectsCreate(clschema.NewSchemaObjectsCreateParams().WithContext(ctx).
			WithObjectClass(&models.Class{
				Class:              mtClass,
				Vectorizer:         "none",
				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
			}), nil)
		require.NoError(t, err)
		_, err = client.Schema.TenantsCreate(clschema.NewTenantsCreateParams().WithContext(ctx).
			WithClassName(mtClass).WithBody([]*models.Tenant{{Name: "t1"}, {Name: "t2"}}), nil)
		require.NoError(t, err)

		consistent := false
		follower := newNodeClient(compose.GetWeaviateNode(2).URI())
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := follower.Schema.TenantsGet(clschema.NewTenantsGetParams().WithContext(ctx).
				WithClassName(mtClass).WithConsistency(&consistent), nil)
			require.NoError(c, err)
			var names []string
			for _, tenant := range resp.Payload {
				names = append(names, tenant.Name)
			}
			assert.ElementsMatch(c, []string{"t1", "t2"}, names)
		}, 30*time.Second, time.Second)
	})
}
