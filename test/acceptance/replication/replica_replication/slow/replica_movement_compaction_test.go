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
	"strings"
	"time"

	"github.com/stretchr/testify/assert"
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
// HYDRATING. WEAVIATE_TEST_COPY_REPLICA_SLEEP holds the transfer open for a
// known wall-clock window after the source has resumed maintenance, giving
// us a deterministic interval to observe compaction signal on disk.
func (suite *ReplicationTestSuite) TestReplicaMovementCompactionContinuesOnSourceDuringHydrating() {
	t := suite.T()
	ctx := context.Background()

	const (
		className       = "ReplMoveCompactionTest"
		sleepWindow     = 30 * time.Second
		observeDeadline = 3 * time.Minute
	)

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateEnv("WEAVIATE_TEST_COPY_REPLICA_SLEEP", sleepWindow.String()).
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "2").
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

	// Halt-for-duration mode keeps the source halted for the whole transfer,
	// which would make this test fail for the wrong reason.
	if !canHardlinkInContainer(ctx, sourceContainer) {
		t.Skip("container fs does not support hardlinks; replica path uses halt-for-duration mode")
	}

	t.Log("pre-populating source shard")
	for i := 0; i < 1000; i++ {
		compactions.ImportBatch(t, cls.Class)
	}

	// Property bucket name varies by LSM layout (property_text_searchable etc.);
	// resolve from disk so the test isn't pinned to a specific naming scheme.
	var propBucket string
	require.Eventually(t, func() bool {
		for _, b := range compactions.ListLSMBuckets(ctx, sourceContainer, cls.Class, shard.Shard) {
			if strings.HasPrefix(b, "property_") {
				propBucket = b
				return true
			}
		}
		return false
	}, 60*time.Second, time.Second, "no property_* bucket on source")
	t.Logf("property bucket: %s", propBucket)

	buckets := []string{"objects", propBucket}
	for _, b := range buckets {
		bucket := b
		require.Eventually(t, func() bool {
			return compactions.TotalSegmentFileCount(ctx, sourceContainer, cls.Class, shard.Shard, bucket) >= 1
		}, 60*time.Second, time.Second, "no segments in bucket %q", bucket)
	}

	prevMaxLevel := map[string]int{}
	prevSegCount := map[string]int{}
	sawCompaction := map[string]bool{}
	for _, b := range buckets {
		prevMaxLevel[b], prevSegCount[b] = compactions.MaxLevelAndCount(ctx, sourceContainer, cls.Class, shard.Shard, b)
		t.Logf("baseline bucket=%q max_level=%d seg_count=%d", b, prevMaxLevel[b], prevSegCount[b])
	}

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

	// Single-goroutine loop avoids any race on the global helper client and
	// on the per-bucket tracking maps.
	deadline := time.Now().Add(observeDeadline)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		if time.Now().After(deadline) {
			t.Fatalf("replication op_id=%s did not reach READY in %s", opID, observeDeadline)
		}

		compactions.ImportBatch(t, cls.Class)

		for _, b := range buckets {
			ml, sc := compactions.MaxLevelAndCount(ctx, sourceContainer, cls.Class, shard.Shard, b)
			if ml > prevMaxLevel[b] || (prevSegCount[b] > 0 && sc < prevSegCount[b]) {
				sawCompaction[b] = true
			}
			if ml > prevMaxLevel[b] {
				prevMaxLevel[b] = ml
			}
			prevSegCount[b] = sc
		}

		details, derr := helper.Client(t).Replication.ReplicationDetails(
			replication.NewReplicationDetailsParams().WithID(opID), nil,
		)
		if derr == nil && details.Payload != nil && details.Payload.Status != nil &&
			details.Payload.Status.State == "READY" {
			break
		}

		<-ticker.C
	}

	for _, b := range buckets {
		assert.Truef(t, sawCompaction[b],
			"expected compaction signal in bucket %q during HYDRATING (max level/seg count never moved)", b)
	}

	targetURI := nodeToContainer[targetName].URI()
	require.Greater(t, common.CountObjects(t, targetURI, cls.Class), int64(0),
		"target node should serve the replicated data")
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
