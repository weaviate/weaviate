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
	// Factor 1: a replica can be removed only down to the factor, and the new one must answer alone.
	createMTCollection(t, restURI, className, 1, textProps("text"))
	createTenants(t, restURI, className, []string{tenant, "large"})
	objects := map[string]string{uuid.NewString(): "The Hobbit", uuid.NewString(): "other words"}
	putToEveryReplica := func() {
		batch := make([]map[string]any, 0, len(objects))
		for id, text := range objects {
			batch = append(batch, map[string]any{
				"class": className, "id": id, "tenant": tenant, "properties": map[string]any{"text": text},
			})
		}
		body, err := json.Marshal(map[string]any{"objects": batch})
		require.NoError(t, err)
		resp, err := http.Post(fmt.Sprintf("http://%s/v1/batch/objects?consistency_level=ALL", restURI),
			"application/json", bytes.NewReader(body))
		require.NoError(t, err)
		respBody, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode, "batch: %s", respBody)
		require.NotContains(t, string(respBody), `"errors"`, "batch: %s", respBody)
	}
	putToEveryReplica()
	importLargeTenant(t, restURI, className)

	migratedOn := func(uri string) bool {
		whole, err := runBM25QueryOnNodeTenant(t, uri, className, tenant, "The Hobbit")
		if err != nil || len(whole) != 1 {
			return false
		}
		word, err := runBM25QueryOnNodeTenant(t, uri, className, tenant, "Hobbit")
		return err == nil && len(word) == 0
	}
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

	var state models.ReplicationShardingStateResponse
	require.True(t, httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s&shard=%s",
		restURI, className, tenant), &state) && state.ShardingState != nil && len(state.ShardingState.Shards) == 1)
	owners := state.ShardingState.Shards[0].Replicas
	require.Len(t, owners, 1)
	joined := slices.IndexFunc([]string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2},
		func(n string) bool { return !slices.Contains(owners, n) })
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
	scale(models.ReplicationScalePlanShardScaleActionsAnon{AddNodes: map[string]string{fmt.Sprintf("weaviate-%d", joined): ""}})
	// Written again once the new replica is listed, so they reach it through the write path.
	require.Eventually(t, func() bool {
		var check models.ReplicationShardingStateResponse
		return httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s&shard=%s",
			restURI, className, tenant), &check) && check.ShardingState != nil &&
			len(check.ShardingState.Shards) == 1 && len(check.ShardingState.Shards[0].Replicas) == 2
	}, 30*time.Second, 200*time.Millisecond, "the new replica must be listed")
	putToEveryReplica()
	scale(models.ReplicationScalePlanShardScaleActionsAnon{RemoveNodes: owners})

	require.Eventually(t, func() bool { return migratedOn(joinedURI()) }, 30*time.Second, 200*time.Millisecond,
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
