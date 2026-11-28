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
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	replicationclient "github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/helper"
)

// A replica added after a tenant switched, while one of its replicas is
// paused, must serve the run's tokenization once the resubmit finishes.
func TestMultiNode_ReplicaAddedWhileSwapOwed(t *testing.T) {
	ctx := context.Background()
	// The default 1s tick: switches land per tick, so a long one switches both tenants at once.
	compose, cleanup := start3NodeReindexCluster(ctx, t, "REPLICA_MOVEMENT_ENABLED", "true")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "ReplicaWhileOwed"
	leader := raftLeaderIndex(t, compose) + 1
	helper.SetupClient(restURIOf(compose, leader))
	frequency, delay := int64(1000), int64(500)
	helper.CreateClass(t, &models.Class{
		Class: className, Vectorizer: "none", Properties: textProps("text"),
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		ReplicationConfig: &models.ReplicationConfig{Factor: 2, AsyncConfig: &models.ReplicationAsyncConfig{
			Frequency: &frequency, FrequencyWhilePropagating: &delay, PropagationDelay: &delay,
		}},
	})
	createTenants(t, restURIOf(compose, leader), className, []string{"small", "large"})

	var state models.ReplicationShardingStateResponse
	require.True(t, httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s&shard=small",
		restURIOf(compose, leader), className), &state) && state.ShardingState != nil)
	require.Len(t, state.ShardingState.Shards, 1)
	holders := state.ShardingState.Shards[0].Replicas
	require.Len(t, holders, 2)
	nodeIndex := func(name string) int {
		return slices.Index([]string{"weaviate-0", "weaviate-1", "weaviate-2"}, name) + 1
	}
	// B is paused, so it is never the leader; requests go through A.
	a, b := nodeIndex(holders[0]), nodeIndex(holders[1])
	if b == leader {
		a, b = b, a
	}
	c := 6 - a - b
	cName := fmt.Sprintf("weaviate-%d", c-1)
	uri := restURIOf(compose, a)

	batchImportTenant(t, uri, className, "small", []string{"needle phrase", "other words"})
	bulk := make([]string, perTenantLargeObjects)
	for i := range bulk {
		bulk[i] = fmt.Sprintf("bulk %d", i)
	}
	batchImportTenant(t, uri, className, "large", bulk)

	reindexhelpers.SubmitIndexUpsert(t, uri, className, "text", "searchable", `{"tokenization":"field"}`)
	// The indexes endpoint counts switched tenants only for a stopped run, so both holders are asked.
	require.Eventually(t, func() bool {
		for _, node := range []int{a, b} {
			whole, err1 := runBM25QueryOnNodeTenant(t, restURIOf(compose, node), className, "small", "needle phrase")
			word, err2 := runBM25QueryOnNodeTenant(t, restURIOf(compose, node), className, "small", "needle")
			if err1 != nil || err2 != nil || len(whole) != 1 || len(word) != 0 {
				return false
			}
		}
		return true
	}, 3*time.Minute, 100*time.Millisecond, "small must switch while large still builds")
	resumeB := pauseNode(ctx, t, compose, b)
	defer resumeB()

	cancelled := reindexhelpers.CancelIndex(t, uri, className, "text", "searchable")
	require.Equal(t, "CANCELLED", cancelled.Status)
	require.Eventually(t, func() bool {
		for _, prop := range reindexhelpers.GetIndexes(t, uri, className).Properties {
			for _, idx := range prop.Indexes {
				if prop.Name == "text" && idx.Type == "searchable" {
					return idx.Status == "partial" && idx.MigratedTenants == 1 && idx.TotalTenants == 2
				}
			}
		}
		return false
	}, time.Minute, 500*time.Millisecond, "the cancelled run must keep small switched")

	helper.SetupClient(uri)
	_, err := helper.Client(t).Replication.ApplyReplicationScalePlan(
		replicationclient.NewApplyReplicationScalePlanParams().WithBody(&models.ReplicationScalePlan{
			PlanID: strfmt.UUID(uuid.NewString()), Collection: className,
			ShardScaleActions: map[string]models.ReplicationScalePlanShardScaleActionsAnon{
				"small": {AddNodes: map[string]string{cName: ""}},
			},
		}), nil)
	require.NoError(t, err)
	resumeB()

	require.Eventually(t, func() bool {
		ids, err := executeGraphQL(restURIOf(compose, c), className,
			fmt.Sprintf(`{ Get { %s(tenant: "small") { _additional { id } } } }`, className))
		return err == nil && len(ids) == 2
	}, 3*time.Minute, time.Second, "async replication must fill the new replica on %s", cName)

	retry := reindexhelpers.SubmitIndexUpsert(t, uri, className, "text", "searchable", `{"tokenization":"field"}`)
	reindexhelpers.AwaitReindexFinished(t, uri, retry, reindexhelpers.WithTimeout(10*time.Minute))
	reindexhelpers.AwaitTokenizationVisible(t, restURIOf(compose, c), className, "text", "field")
	for query, want := range map[string]int{"needle phrase": 1, "needle": 0} {
		ids, err := runBM25QueryOnNodeTenant(t, restURIOf(compose, c), className, "small", query)
		require.NoError(t, err)
		require.Lenf(t, ids, want, "the replica added on %s must answer %q at field tokenization", cName, query)
	}
}
