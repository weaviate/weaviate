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
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	replicationclient "github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	reindexhelpers "github.com/weaviate/weaviate/test/acceptance/helpers/reindex"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// Pins that a cancelled run still owing a tenant's swap on one replica refuses
// copying or moving that tenant until the owing node swaps.
func TestMultiNode_MovementRefusedWhileSwapOwed(t *testing.T) {
	ctx := context.Background()
	compose, cleanup := start3NodeReindexCluster(ctx, t, "REPLICA_MOVEMENT_ENABLED", "true")
	defer cleanup()
	defer dumpContainerLogs(ctx, t, compose)

	const className = "SwapOwedMovement"
	// Only followers are paused, so the leader keeps serving every request.
	leader := raftLeaderIndex(t, compose) + 1
	restURI := restURIOf(compose, leader)

	// One request of three tenants at RF=2 places them on all three node pairs.
	createMTCollection(t, restURI, className, 2, textProps("text"))
	createTenants(t, restURI, className, []string{"t1", "t2", "t3"})
	placement := tenantPlacement(t, restURI, className)
	roles := pickSwapOwedRoles(t, placement, nodeName(leader))
	helper.SetupClient(restURI)
	require.NoError(t, helper.DeleteTenants(t, className, []string{roles.unused}))

	importObjectsTenant(t, restURI, className, roles.owed, []string{"needle phrase", "other words"})
	// The kept tenant's unit on the owing node outlasts the owed tenant's, so the run stays cancellable.
	importLargeTenant(t, restURI, className, roles.kept)

	// The swap is decided by the last replica's prep ack. Making the swapped node ack last, while the
	// owing node is paused after its own ack, leaves the owing node's swap owed.
	resumeOwing := pauseNode(ctx, t, compose, roles.owing)
	defer resumeOwing()
	taskID := reindexhelpers.SubmitIndexUpsert(t, restURI, className, "text", "searchable", `{"tokenization":"field"}`)
	awaitNodeUnitsCompleted(t, restURI, taskID, roles.swapped)

	resumeSwapped := pauseNode(ctx, t, compose, roles.swapped)
	defer resumeSwapped()
	require.Zero(t, prepAcksApplied(ctx, t, compose, leader), "no group can be prepared before the owing node runs")
	resumeOwing()
	require.Eventually(t, func() bool {
		return prepAcksApplied(ctx, t, compose, leader) > 0
	}, 2*time.Minute, 100*time.Millisecond, "the owing node must ack the owed tenant's prep")

	resumeOwing = pauseNode(ctx, t, compose, roles.owing)
	keptUnit := fmt.Sprintf("%s__%s", roles.kept, nodeName(roles.owing))
	task, ok := fetchTask(restURI, taskID)
	require.True(t, ok)
	require.Equal(t, "STARTED", task.Status)
	for _, u := range task.Units {
		if u.ID == keptUnit {
			t.Logf("kept tenant's unit on the owing node at pause: %s, progress %.2f", u.Status, u.Progress)
			require.NotEqual(t, "COMPLETED", u.Status,
				"the kept tenant must still be reindexing, so the only applied prep ack is the owed tenant's")
		}
	}
	resumeSwapped()

	swappedURI := restURIOf(compose, roles.swapped)
	require.Eventually(t, func() bool {
		whole, err := runBM25QueryOnNodeTenant(t, swappedURI, className, roles.owed, "needle phrase")
		if err != nil || len(whole) != 1 {
			return false
		}
		word, err := runBM25QueryOnNodeTenant(t, swappedURI, className, roles.owed, "needle")
		return err == nil && len(word) == 0
	}, 2*time.Minute, 200*time.Millisecond, "the last replica's ack decides the swap, and that replica swaps")

	cancelled := reindexhelpers.CancelIndex(t, restURI, className, "text", "searchable")
	require.Equal(t, "CANCELLED", cancelled.Status)

	owingName, swappedName, leaderName := nodeName(roles.owing), nodeName(roles.swapped), nodeName(leader)
	for _, req := range []struct{ kind, source string }{{"COPY", swappedName}, {"MOVE", owingName}} {
		var conflict *replicationclient.ReplicateConflict
		require.ErrorAsf(t, replicateShard(t, restURI, className, roles.owed, req.source, leaderName, req.kind), &conflict,
			"a %s of a tenant whose swap is owed must be refused", req.kind)
		require.NotEmpty(t, conflict.Payload.Error)
		msg := conflict.Payload.Error[0].Message
		require.Contains(t, msg, "still owes the swap")
		require.Contains(t, msg, owingName)
		require.NotContains(t, msg, swappedName, "only the owing node may be named")
	}

	keptSource := placement[roles.kept][0]
	if keptSource == owingName {
		keptSource = placement[roles.kept][1]
	}
	require.NoError(t, replicateShard(t, restURI, className, roles.kept, keptSource, swappedName, "COPY"),
		"a tenant with no owed swap may still be copied")

	resumeOwing()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		assert.NoError(ct, replicateShard(t, restURI, className, roles.owed, swappedName, leaderName, "COPY"))
	}, 2*time.Minute, 500*time.Millisecond, "the copy must be admitted once the owing node swapped")
}

type swapOwedRoles struct {
	owed, kept, unused string
	// Node numbers, 1-based.
	owing, swapped int
}

// pickSwapOwedRoles puts the owed tenant on the two followers; the kept tenant shares one of them, the owing node.
func pickSwapOwedRoles(t *testing.T, placement map[string][]string, leader string) swapOwedRoles {
	t.Helper()
	var roles swapOwedRoles
	for _, tenant := range slices.Sorted(maps.Keys(placement)) {
		switch {
		case !slices.Contains(placement[tenant], leader):
			roles.owed = tenant
		case roles.kept == "":
			roles.kept = tenant
		default:
			roles.unused = tenant
		}
	}
	require.NotEmptyf(t, roles.owed, "no tenant on the two followers: %v", placement)
	require.NotEmpty(t, roles.kept, "placement: %v", placement)
	for _, node := range placement[roles.owed] {
		if slices.Contains(placement[roles.kept], node) {
			roles.owing = nodeNumber(t, node)
		} else {
			roles.swapped = nodeNumber(t, node)
		}
	}
	require.NotZerof(t, roles.owing*roles.swapped, "placement: %v", placement)
	return roles
}

func tenantPlacement(t *testing.T, restURI, className string) map[string][]string {
	t.Helper()
	placement := map[string][]string{}
	require.Eventually(t, func() bool {
		var state models.ReplicationShardingStateResponse
		if !httpGetJSON(fmt.Sprintf("http://%s/v1/replication/sharding-state?collection=%s", restURI, className), &state) ||
			state.ShardingState == nil {
			return false
		}
		for _, shard := range state.ShardingState.Shards {
			placement[shard.Shard] = shard.Replicas
		}
		return len(placement) == 3
	}, 30*time.Second, 100*time.Millisecond, "sharding state must list the three tenants")
	return placement
}

func replicateShard(t *testing.T, restURI, className, shard, source, target, kind string) error {
	helper.SetupClient(restURI)
	_, err := helper.Client(t).Replication.Replicate(replicationclient.NewReplicateParams().
		WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &className, Shard: &shard, SourceNode: &source, TargetNode: &target, Type: &kind,
		}), nil)
	return err
}

// prepAcksApplied counts the prep acks a node's store applied, from its debug log.
func prepAcksApplied(ctx context.Context, t *testing.T, compose *docker.DockerCompose, node int) int {
	t.Helper()
	reader, err := compose.GetWeaviateNode(node).Container().Logs(ctx)
	require.NoError(t, err)
	defer reader.Close()
	raw, err := io.ReadAll(reader)
	require.NoError(t, err)
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.Contains(line, "server.apply") &&
			strings.Contains(line, "TYPE_DISTRIBUTED_TASK_RECORD_PREPARATION_COMPLETE_ACK") {
			n++
		}
	}
	return n
}

func nodeName(node int) string {
	return fmt.Sprintf("weaviate-%d", node-1)
}

func nodeNumber(t *testing.T, name string) int {
	t.Helper()
	for node := 1; node <= 3; node++ {
		if nodeName(node) == name {
			return node
		}
	}
	t.Fatalf("unknown node %q", name)
	return 0
}
