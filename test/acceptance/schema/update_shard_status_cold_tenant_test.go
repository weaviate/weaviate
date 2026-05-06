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
	"fmt"
	"testing"
	"time"

	httptransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apiclient "github.com/weaviate/weaviate/client"
	clcluster "github.com/weaviate/weaviate/client/cluster"
	clschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
)

// Regression guard for the re-entrant FSM apply deadlock.
// Applying UPDATE_SHARD_STATUS for a COLD
// tenant under AutoTenantActivation used to issue a recursive Raft proposal
// from inside the single-threaded runFSM goroutine and wedge the cluster.
// A failure here means the regression is back.
func TestUpdateShardStatusViaFollowerWhileTenantCold(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		// cluster may be wedged if the regression is back; Docker stop is forceful
		_ = compose.Terminate(context.Background())
	}()

	const (
		className  = "DeadlockProbe"
		tenantName = "t1"
	)

	node1Client := newNodeClient(compose.GetWeaviateNode(1).URI())

	t.Run("create MT class with autoTenantActivation and rf=3", func(t *testing.T) {
		params := clschema.NewSchemaObjectsCreateParams().
			WithContext(ctx).
			WithObjectClass(&models.Class{
				Class:      className,
				Vectorizer: "none",
				MultiTenancyConfig: &models.MultiTenancyConfig{
					Enabled:              true,
					AutoTenantActivation: true,
				},
				ReplicationConfig: &models.ReplicationConfig{Factor: 3},
			})
		_, err := node1Client.Schema.SchemaObjectsCreate(params, nil)
		require.NoError(t, err)
	})

	t.Run("add tenant (HOT) then deactivate (COLD)", func(t *testing.T) {
		createParams := clschema.NewTenantsCreateParams().
			WithContext(ctx).
			WithClassName(className).
			WithBody([]*models.Tenant{{
				Name:           tenantName,
				ActivityStatus: models.TenantActivityStatusHOT,
			}})
		_, err := node1Client.Schema.TenantsCreate(createParams, nil)
		require.NoError(t, err)

		updateParams := clschema.NewTenantsUpdateParams().
			WithContext(ctx).
			WithClassName(className).
			WithBody([]*models.Tenant{{
				Name:           tenantName,
				ActivityStatus: models.TenantActivityStatusCOLD,
			}})
		_, err = node1Client.Schema.TenantsUpdate(updateParams, nil)
		require.NoError(t, err)

		// Without this wait the leader's OptimisticTenantStatus could still
		// see HOT and short-circuit, hiding the regression.
		assert.EventuallyWithT(t, func(c *assert.CollectT) {
			synced, err := clusterSynchronized(ctx, node1Client)
			assert.NoError(c, err)
			assert.True(c, synced)
		}, 30*time.Second, 200*time.Millisecond)
	})

	var followerClient *apiclient.Weaviate
	t.Run("identify a follower", func(t *testing.T) {
		stats, err := getClusterStatistics(ctx, node1Client)
		require.NoError(t, err)

		var leaderName string
		for _, s := range stats.Statistics {
			if s.Raft != nil && s.Raft.State == "Leader" {
				leaderName = s.Name
			}
		}
		require.NotEmpty(t, leaderName, "no leader in cluster/statistics; got: %+v", stats.Statistics)

		// docker/compose.go sets CLUSTER_HOSTNAME=nodeN.
		for i := 1; i <= 3; i++ {
			if fmt.Sprintf("node%d", i) != leaderName {
				followerClient = newNodeClient(compose.GetWeaviateNode(i).URI())
				break
			}
		}
		require.NotNil(t, followerClient, "could not identify a follower")
	})

	t.Run("update shard status via follower returns without error", func(t *testing.T) {
		// Bounded so an unfixed regression fails fast instead of hanging the suite.
		callCtx, callCancel := context.WithTimeout(ctx, 30*time.Second)
		defer callCancel()

		params := clschema.NewSchemaObjectsShardsUpdateParams().
			WithContext(callCtx).
			WithClassName(className).
			WithShardName(tenantName).
			WithBody(&models.ShardStatus{Status: "READONLY"})
		_, err := followerClient.Schema.SchemaObjectsShardsUpdate(params, nil)
		require.NoError(t, err)
	})
}

func newNodeClient(hostPort string) *apiclient.Weaviate {
	transport := httptransport.New(hostPort, "/v1", []string{"http"})
	return apiclient.New(transport, strfmt.Default)
}

func getClusterStatistics(ctx context.Context, c *apiclient.Weaviate) (*models.ClusterStatisticsResponse, error) {
	params := clcluster.NewClusterGetStatisticsParams().WithContext(ctx)
	resp, err := c.Cluster.ClusterGetStatistics(params, nil)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

func clusterSynchronized(ctx context.Context, c *apiclient.Weaviate) (bool, error) {
	stats, err := getClusterStatistics(ctx, c)
	if err != nil {
		return false, err
	}
	return stats.Synchronized, nil
}
