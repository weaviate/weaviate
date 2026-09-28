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

package crash_resume

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	pauseBeforeSeal     = "30s"
	integratingTimeout  = 3 * time.Minute
	gapDeletes          = 25
	rehydrateConvergeIn = 3 * time.Minute
)

// TestReplicaOpRehydratesAfterSourceSIGKILLBeforeSeal pins that a COPY/MOVE
// whose source is SIGKILLed after the target joined the sharding state, before
// the seal, re-hydrates to READY with the exact object set instead of being held.
func TestReplicaOpRehydratesAfterSourceSIGKILLBeforeSeal(t *testing.T) {
	tests := []struct {
		name         string
		transferType string
	}{
		{name: "copy", transferType: models.ReplicationReplicateReplicaRequestTypeCOPY},
		{name: "move", transferType: models.ReplicationReplicateReplicaRequestTypeMOVE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runSourceCrashBeforeSeal(t, tt.transferType)
		})
	}
}

func runSourceCrashBeforeSeal(t *testing.T, transferType string) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(numNodes).
		WithWeaviateEnv("WEAVIATE_TEST_REPLICA_PAUSE_BEFORE_SEAL", pauseBeforeSeal).
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			var sb strings.Builder
			compose.DumpWeaviateLogs(ctx, &sb, 400)
			t.Log(sb.String())
		}
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate test containers: %v", err)
		}
	})

	className := "SourceCrash" + strings.ToUpper(transferType[:1]) + strings.ToLower(transferType[1:])
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.CreateClass(t, &models.Class{
		Class:          className,
		Vectorizer:     "none",
		Properties:     []*models.Property{{Name: "contents", DataType: []string{"text"}}},
		ShardingConfig: map[string]interface{}{"desiredCount": 1},
		ReplicationConfig: &models.ReplicationConfig{
			Factor:           2,
			AsyncConfig:      common.FastAsyncConfig(),
			DeletionStrategy: models.ReplicationConfigDeletionStrategyTimeBasedResolution,
		},
	})

	ids := insertObjects(t, compose.GetWeaviate().URI(), className, 0, initialObjects)

	holders, shardName := shardHolders(t, compose, className)
	require.Len(t, holders, 2)
	sourceIdx, peerIdx := holders[0], holders[1]
	targetIdx := 3 - sourceIdx - peerIdx
	sourceName, targetName := nodeName(sourceIdx), nodeName(targetIdx)
	controlURI := compose.GetWeaviateNode(peerIdx + 1).URI()
	t.Logf("%s shard %s from %s to %s, peer %s", transferType, shardName, sourceName, targetName, nodeName(peerIdx))

	helper.SetupClient(controlURI)
	resp, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &className,
			Shard:      &shardName,
			SourceNode: &sourceName,
			TargetNode: &targetName,
			Type:       &transferType,
		}), nil)
	require.NoError(t, err)
	opID := *resp.Payload.ID

	waitOpState(t, controlURI, opID, "INTEGRATING", integratingTimeout)
	require.NoError(t, compose.KillNode(ctx, sourceIdx))
	t.Logf("SIGKILLed source %s while the op is paused before the seal", sourceName)

	ids = append(ids, insertObjectsCL(t, controlURI, className, initialObjects, downtimeObjects, types.ConsistencyLevelOne)...)
	deleted := ids[:gapDeletes]
	helper.SetupClient(controlURI)
	for _, id := range deleted {
		helper.DeleteObjectCL(t, className, id, types.ConsistencyLevelOne)
	}
	live := ids[gapDeletes:]

	require.NoError(t, compose.StartNode(ctx, sourceIdx))
	t.Logf("restarted source %s", sourceName)

	details := waitOpState(t, controlURI, opID, "READY", opCompletionTimeout)
	require.True(t, rewoundAfterIntegrating(details), "op never re-hydrated after INTEGRATING; history:\n%s", formatHistory(details))

	targetURI := compose.GetWeaviateNode(targetIdx + 1).URI()
	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		var missing, resurrected []strfmt.UUID
		for _, id := range live {
			if _, err := common.GetObjectFromNode(t, targetURI, className, id, targetName); err != nil {
				missing = append(missing, id)
			}
		}
		for _, id := range deleted {
			if _, err := common.GetObjectFromNode(t, targetURI, className, id, targetName); err == nil {
				resurrected = append(resurrected, id)
			}
		}
		assert.Empty(ct, missing, "objects missing on target %s", targetName)
		assert.Empty(ct, resurrected, "deleted objects present on target %s", targetName)
		counts := shardObjectCounts(t, controlURI, className)
		assert.Equal(ct, int64(len(live)), counts[targetName], "object count on target %s", targetName)
		_, sourceHolds := counts[sourceName]
		if transferType == models.ReplicationReplicateReplicaRequestTypeMOVE {
			assert.False(ct, sourceHolds, "source %s still holds the moved replica", sourceName)
		} else {
			assert.True(ct, sourceHolds, "source %s lost the copied replica", sourceName)
		}
	}, rehydrateConvergeIn, time.Second)
}

func insertObjectsCL(t *testing.T, uri, className string, offset, n int, cl types.ConsistencyLevel) []strfmt.UUID {
	t.Helper()
	objs := make([]*models.Object, n)
	ids := make([]strfmt.UUID, n)
	for i := range objs {
		ids[i] = strfmt.UUID(uuid.NewString())
		objs[i] = &models.Object{
			Class:      className,
			ID:         ids[i],
			Properties: map[string]interface{}{"contents": fmt.Sprintf("object#%d", offset+i)},
		}
	}
	common.CreateObjectsCL(t, uri, objs, cl)
	return ids
}

func shardHolders(t *testing.T, compose *docker.DockerCompose, className string) ([]int, string) {
	t.Helper()
	var (
		holders []int
		shard   string
	)
	for node, shards := range shardsByNode(t, compose.GetWeaviate().URI(), className) {
		for s := range shards {
			shard = s
			for idx := 0; idx < numNodes; idx++ {
				if nodeName(idx) == node {
					holders = append(holders, idx)
				}
			}
		}
	}
	slices.Sort(holders)
	return holders, shard
}

func rewoundAfterIntegrating(d *models.ReplicationReplicateDetailsReplicaResponse) bool {
	seenIntegrating := false
	for _, s := range allStatuses(d) {
		if s == nil {
			continue
		}
		switch s.State {
		case "INTEGRATING":
			seenIntegrating = true
		case "HYDRATING":
			if seenIntegrating {
				return true
			}
		}
	}
	return false
}

func formatHistory(d *models.ReplicationReplicateDetailsReplicaResponse) string {
	var sb strings.Builder
	for _, s := range allStatuses(d) {
		if s != nil {
			sb.WriteString("  " + s.State + "\n")
		}
	}
	return sb.String() + formatErrors(d)
}
