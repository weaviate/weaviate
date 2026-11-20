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

package slow

import (
	"context"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/compactions"
)

// Guards against a regression that re-introduces a long source halt during
// HYDRATING. The target holds the copy after the source has resumed maintenance
// until the test creates copyHoldFile, so the op stays HYDRATING for as long as
// compaction needs: up to the 60s maximum of the compaction cycle backoff.
func (suite *ReplicationTestSuiteSlow) TestReplicaMovementCompactionContinuesOnSourceDuringHydrating() {
	t := suite.T()
	ctx := context.Background()

	const (
		className       = "ReplMoveCompactionTest"
		copyHoldFile    = "/tmp/release-replica-copy"
		holdDeadline    = 3 * time.Minute
		prePopBatches   = 20
		observeDeadline = 10 * time.Minute
	)

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateEnv("WEAVIATE_TEST_COPY_REPLICA_HOLD_FILE", copyHoldFile).
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.Nil(t, err)
	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	cls := &models.Class{
		Class:             className,
		Vectorizer:        "none",
		Properties:        []*models.Property{{Name: "text", DataType: []string{"text"}}},
		ShardingConfig:    map[string]interface{}{"desiredCount": 1},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
	helper.DeleteClass(t, cls.Class)
	helper.CreateClass(t, cls)

	ns, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams(), nil)
	require.NoError(t, err)
	require.Len(t, ns.Payload.Nodes, 3)
	nodeToContainer := map[string]*docker.DockerContainer{}
	for idx, n := range ns.Payload.Nodes {
		nodeToContainer[n.Name] = compose.GetWeaviateNode(idx + 1)
	}

	state, err := helper.Client(t).Replication.GetCollectionShardingState(
		replication.NewGetCollectionShardingStateParams().WithCollection(&cls.Class), nil,
	)
	require.NoError(t, err)
	require.Len(t, state.Payload.ShardingState.Shards, 1)
	shard := state.Payload.ShardingState.Shards[0]
	require.Len(t, shard.Replicas, 1)
	sourceName := shard.Replicas[0]
	var targetName string
	for n := range nodeToContainer {
		if n != sourceName {
			targetName = n
			break
		}
	}
	require.NotEmpty(t, targetName)
	sourceContainer := nodeToContainer[sourceName].Container()
	t.Logf("source=%s target=%s shard=%s", sourceName, targetName, shard.Shard)

	// Replica movement is refused on a filesystem without hard links, so the
	// movement this test drives would never start.
	if !canHardlinkInContainer(ctx, sourceContainer) {
		t.Skip("container fs does not support hardlinks; replica movement is refused")
	}

	t.Log("pre-populating source shard")
	for range prePopBatches {
		compactions.ImportBatch(t, cls.Class)
	}

	require.Eventually(t, func() bool {
		return compactions.TotalSegmentFileCount(ctx, sourceContainer, cls.Class, shard.Shard, "objects") >= 1
	}, 60*time.Second, time.Second, "no segments in bucket %q", "objects")

	repResp, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &cls.Class,
			SourceNode: &sourceName,
			TargetNode: &targetName,
			Shard:      &shard.Shard,
		}),
		nil,
	)
	require.NoError(t, err)
	opID := *repResp.Payload.ID
	t.Logf("started replicate op_id=%s", opID)

	// The hardlinks are made while the source shard is halted, so every segment
	// listed for the first time after this point was created during or after the halt.
	for stageDeadline := time.Now().Add(2 * time.Minute); ; time.Sleep(time.Second) {
		require.True(t, time.Now().Before(stageDeadline), "source never staged a hard-linked snapshot")
		n, err := compactions.StagedSegmentCount(ctx, sourceContainer, "/data/.replica-staging-*/lsm/objects")
		require.NoError(t, err)
		if n > 0 {
			break
		}
	}

	segments, err := compactions.SegmentNames(ctx, sourceContainer, cls.Class, shard.Shard, "objects")
	require.NoError(t, err)
	var watcher compactions.MergeWatcher
	watcher.Arm(segments)

	for holdEnd := time.Now().Add(holdDeadline); ; {
		require.True(t, time.Now().Before(holdEnd),
			"no source segment created after the snapshot was compacted within %s while the copy was held", holdDeadline)
		time.Sleep(time.Second)

		compactions.ImportBatch(t, cls.Class)

		segments, err := compactions.SegmentNames(ctx, sourceContainer, cls.Class, shard.Shard, "objects")
		require.NoError(t, err)
		merged := watcher.Observe(segments)

		// Read after the listing, so a merge it shows happened while the op was HYDRATING.
		state := replicationState(t, opID)
		require.NotEqual(t, "READY", state, "op finished while held; copy hold not honoured")
		t.Logf("[poll] op=%s source_segments=%d merged=%v", state, len(segments), merged)
		if merged {
			break
		}
	}

	targetContainer := nodeToContainer[targetName].Container()
	code, _, err := targetContainer.Exec(ctx, []string{"touch", copyHoldFile})
	require.NoError(t, err)
	require.Zero(t, code)

	for readyDeadline := time.Now().Add(observeDeadline); ; time.Sleep(time.Second) {
		require.True(t, time.Now().Before(readyDeadline),
			"replication op_id=%s did not reach READY in %s", opID, observeDeadline)
		if replicationState(t, opID) == "READY" {
			break
		}
	}

	targetURI := nodeToContainer[targetName].URI()
	require.Greater(t, common.CountObjects(t, targetURI, cls.Class), int64(0),
		"target node should serve the replicated data")
}

func replicationState(t *testing.T, opID strfmt.UUID) string {
	t.Helper()
	details, err := helper.Client(t).Replication.ReplicationDetails(
		replication.NewReplicationDetailsParams().WithID(opID), nil,
	)
	require.NoError(t, err)
	if details.Payload == nil || details.Payload.Status == nil {
		return ""
	}
	return details.Payload.Status.State
}

func canHardlinkInContainer(ctx context.Context, c testcontainers.Container) bool {
	code, _, err := c.Exec(ctx, []string{
		"sh", "-c",
		"touch /data/.repl-probe && " +
			"ln /data/.repl-probe /data/.repl-probe2 && " +
			"rm -f /data/.repl-probe /data/.repl-probe2",
	})
	return err == nil && code == 0
}
