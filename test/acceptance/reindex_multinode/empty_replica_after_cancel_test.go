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

package reindex_multinode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	replicationclient "github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// A replica added with no source after a cancel, to a tenant the run already
// swapped, must serve the migrated index before and after the schema changes.
func TestMultiNode_EmptyReplicaAfterCancel_ServesTheMigratedIndex(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t, "REPLICA_MOVEMENT_ENABLED", "true")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className, tenant = "EmptyReplicaAfterCancel", "t1"
	restURI := compose.GetWeaviate().URI()
	// Replication factor 1, because a replica can be removed only down to the
	// factor and the new one must answer alone.
	createAsyncMTCollection(t, restURI, className)
	createTenants(t, restURI, className, []string{tenant, "large"})
	ids := []string{uuid.NewString(), uuid.NewString()}
	postBatchToEveryReplica(t, restURI, []map[string]any{
		{"class": className, "id": ids[0], "tenant": tenant, "properties": map[string]any{"text": "The Hobbit"}},
		{"class": className, "id": ids[1], "tenant": tenant, "properties": map[string]any{"text": "other words"}},
	})
	importLargeTenant(t, restURI, className)
	migratedOn := func(uri string) bool { return tenantMigratedOn(t, uri, className, tenant, "The Hobbit", "Hobbit") }

	// The large tenant keeps the run cancellable after t1 swaps.
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	require.Eventually(t, func() bool {
		return migratedOn(restURIOf(compose, 1)) && migratedOn(restURIOf(compose, 2)) && migratedOn(restURIOf(compose, 3))
	}, 2*time.Minute, 200*time.Millisecond, "t1 must swap")
	require.Equal(t, "STARTED", reindexTaskStatus(restURI, taskID), "t1 must swap while the large tenant reindexes")
	reindexhelpers.CancelIndex(t, restURI, className, "text", "searchable")
	require.Eventually(t, func() bool {
		for _, prop := range reindexhelpers.GetIndexes(t, restURI, className).Properties {
			for _, idx := range prop.Indexes {
				if prop.Name == "text" && idx.Type == "searchable" {
					return idx.Status == "partial" && idx.MigratedTenants == 1 && idx.TotalTenants == 2
				}
			}
		}
		return false
	}, time.Minute, 500*time.Millisecond, "the indexes endpoint must read partial, 1 of 2 tenants")

	replicasOfTenant := func() []string {
		var state models.ReplicationShardingStateResponse
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s&shard=%s",
			restURI, className, tenant), &state) || state.ShardingState == nil || len(state.ShardingState.Shards) != 1 {
			return nil
		}
		return state.ShardingState.Shards[0].Replicas
	}
	owners := replicasOfTenant()
	require.Len(t, owners, 1)
	joined := slices.IndexFunc([]string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2},
		func(n string) bool { return !slices.Contains(owners, n) })
	joinedName := fmt.Sprintf("weaviate-%d", joined)
	joinedURI := func() string { return restURIOf(compose, joined+1) }

	scale := func(actions models.ReplicationScalePlanShardScaleActionsAnon) {
		helper.SetupClient(restURI)
		_, err := helper.Client(t).Replication.ApplyReplicationScalePlan(
			replicationclient.NewApplyReplicationScalePlanParams().WithBody(&models.ReplicationScalePlan{
				PlanID: strfmt.UUID(uuid.NewString()), Collection: className,
				ShardScaleActions: map[string]models.ReplicationScalePlanShardScaleActionsAnon{tenant: actions},
			}), nil)
		var refused *replicationclient.ApplyReplicationScalePlanInternalServerError
		if errors.As(err, &refused) && refused.Payload != nil && len(refused.Payload.Error) > 0 {
			t.Fatalf("scale plan refused: %s", refused.Payload.Error[0].Message)
		}
		require.NoError(t, err)
	}
	scale(models.ReplicationScalePlanShardScaleActionsAnon{AddNodes: map[string]string{joinedName: ""}})
	// Read from the new replica alone; the nodes endpoint's object count lags behind it.
	require.Eventually(t, func() bool {
		return !slices.ContainsFunc(ids, func(id string) bool {
			resp, err := http.Get(fmt.Sprintf("http://%s/v1/objects/%s/%s?tenant=%s&node_name=%s",
				restURI, className, id, tenant, joinedName))
			if err != nil {
				return true
			}
			resp.Body.Close()
			return resp.StatusCode != http.StatusOK
		})
	}, 3*time.Minute, time.Second, "async replication must fill the new replica")
	scale(models.ReplicationScalePlanShardScaleActionsAnon{RemoveNodes: owners})
	require.Eventually(t, func() bool { return slices.Equal(replicasOfTenant(), []string{joinedName}) },
		30*time.Second, 200*time.Millisecond, "only the new replica must be listed")

	require.True(t, migratedOn(joinedURI()),
		"the new replica, alone, must serve t1 with field tokenization before the schema changes")

	retry := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	reindexhelpers.AwaitReindexFinished(t, restURI, retry, reindexhelpers.WithTimeout(10*time.Minute))
	reindexhelpers.AwaitTokenizationVisible(t, joinedURI(), className, "text", "field")
	require.True(t, migratedOn(joinedURI()), "the new replica must serve t1 after the schema changes")

	require.NoError(t, compose.StopNode(ctx, joined, nil))
	require.NoError(t, compose.StartNode(ctx, joined))
	require.Eventually(t, func() bool { return migratedOn(joinedURI()) }, time.Minute, 500*time.Millisecond,
		"the new replica must serve t1 after it restarts")
}

func createAsyncMTCollection(t *testing.T, restURI, className string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{
		"class": className, "vectorizer": "none", "properties": textProps("text"),
		"multiTenancyConfig": map[string]any{"enabled": true},
		"replicationConfig": map[string]any{
			"factor": 1, "asyncEnabled": true,
			"asyncConfig": map[string]any{"frequency": 5000, "frequencyWhilePropagating": 1000, "propagationDelay": 1000},
		},
	})
	require.NoError(t, err)
	resp, err := http.Post(fmt.Sprintf("http://%s/v1/schema", restURI), "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	require.Equal(t, http.StatusOK, resp.StatusCode, "create class: %s", respBody)
}
