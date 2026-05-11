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

package selfrecovery

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

// forceRaftSnapshot forces a snapshot before a wipe so the rejoin goes through
// InstallSnapshot. Requires WithWeaviateWithDebugPort: /debug/* is on the
// profiling port, not the main API port.
func forceRaftSnapshot(ctx context.Context, t *testing.T, compose *docker.DockerCompose, idx int) {
	t.Helper()
	c, err := compose.ContainerAt(idx)
	require.NoError(t, err, "forceRaftSnapshot[%d]: container", idx)
	uri := c.DebugURI()
	require.NotEmpty(t, uri, "forceRaftSnapshot[%d]: empty DebugURI (did you call WithWeaviateWithDebugPort?)", idx)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://"+uri+"/debug/raft/snapshot", nil)
	require.NoError(t, err, "forceRaftSnapshot[%d]: build request", idx)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err, "forceRaftSnapshot[%d]: do request", idx)
	defer resp.Body.Close()
	require.Less(t, resp.StatusCode, 300, "forceRaftSnapshot[%d]: status %d", idx, resp.StatusCode)
}

func TestSelfRecoveryEndToEnd(t *testing.T) {
	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, raftTrailingLogs: true, debugPort: true})

	const (
		objCount = 500
		wipedIdx = 2
	)
	wipedNodeName := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	paragraphClass := articles.ParagraphsClass()

	t.Run("wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
	})

	t.Run("create RF=3 single-shard collection", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		paragraphClass.Vectorizer = "none"
		helper.CreateClass(t, paragraphClass)
	})

	t.Run("ingest objects", func(t *testing.T) {
		batch := make([]*models.Object, objCount)
		for i := 0; i < objCount; i++ {
			batch[i] = articles.NewParagraph().
				WithID(strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		submitBatch(t, batch, "")
	})

	t.Run("verify all 3 nodes report shard loaded", func(t *testing.T) {
		// A peer that crash-restarted during formation may still be recovering;
		// settle and require every shard loaded so the wipe leaves enough replicas.
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		waitShardsLoaded(t, paragraphClass.Class, 1)
	})

	t.Run("force a RAFT snapshot before wipe", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			forceRaftSnapshot(ctx, t, compose, i)
		}
	})

	t.Run("wipe node-3 data and restart", func(t *testing.T) {
		wipeAndRestart(ctx, t, compose, wipedIdx)
	})

	t.Run("a SELF_RECOVERY op was registered for node-3", func(t *testing.T) {
		waitSelfRecoveryOpFired(t, wipedNodeName)
	})

	t.Run("recovery completes and node-3 reports full object count", func(t *testing.T) {
		assertNodeRecovered(t, paragraphClass.Class, wipedNodeName, 1, int64(objCount))
	})

	t.Run("direct query to node-3 returns full data at consistency=ONE", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 0; i < 10; i++ {
				id := strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))
				exists, err := common.ObjectExistsCL(t, compose.ContainerURI(wipedIdx), paragraphClass.Class, id, types.ConsistencyLevelOne)
				assert.NoError(ct, err)
				assert.True(ct, exists, "object %s missing on node-3", id)
			}
		}, 30*time.Second, 1*time.Second, "node-3 sampled data not available at consistency=ONE")
	})
}

func TestSelfRecoveryReadsContinueAtConsistencyONE(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, raftTrailingLogs: true, debugPort: true})

	const (
		objCount = 200
		wipedIdx = 2
	)
	wipedNodeName := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	paragraphClass := articles.ParagraphsClass()
	paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
	paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	paragraphClass.Vectorizer = "none"

	waitClusterHealthy(t)
	helper.CreateClass(t, paragraphClass)

	ids := make([]strfmt.UUID, objCount)
	batch := make([]*models.Object, objCount)
	for i := 0; i < objCount; i++ {
		ids[i] = strfmt.UUID(fmt.Sprintf("11111111-1111-1111-1111-%012d", i+1))
		batch[i] = articles.NewParagraph().WithID(ids[i]).WithContents(fmt.Sprintf("p#%d", i)).Object()
	}
	common.CreateObjects(t, compose.GetWeaviate().URI(), batch)

	// A peer that crash-restarted during formation may still be recovering; wiping
	// node-2 on top of that would leave too few replicas to serve the CL=ONE probe.
	waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
	waitShardsLoaded(t, paragraphClass.Class, 1)

	for i := 0; i < 3; i++ {
		forceRaftSnapshot(ctx, t, compose, i)
	}

	wipeAndRestart(ctx, t, compose, wipedIdx)

	deadline := time.Now().Add(5 * time.Minute)
	probeCount := 0
	for time.Now().Before(deadline) {
		for _, id := range []strfmt.UUID{ids[0], ids[objCount/4], ids[objCount/2], ids[3*objCount/4], ids[objCount-1]} {
			exists, err := common.ObjectExistsCL(t, compose.GetWeaviate().URI(), paragraphClass.Class, id, types.ConsistencyLevelOne)
			require.NoError(t, err, "consistency=ONE probe failed during recovery (probe #%d, id=%s)", probeCount, id)
			require.True(t, exists, "consistency=ONE probe missing object during recovery (probe #%d, id=%s)", probeCount, id)
		}
		probeCount++

		body, err := helper.Client(t).Replication.ListReplication(
			replication.NewListReplicationParams().WithTargetNode(&wipedNodeName), nil)
		if err == nil && body != nil && body.Payload != nil {
			doneCount := 0
			selfRecoveryCount := 0
			for _, op := range body.Payload {
				if op.Type != nil && *op.Type == "SELF_RECOVERY" {
					selfRecoveryCount++
					if op.Status != nil && op.Status.State == "READY" {
						doneCount++
					}
				}
			}
			if selfRecoveryCount > 0 && doneCount == selfRecoveryCount {
				t.Logf("recovery complete after %d probes", probeCount)
				return
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("recovery did not finish within deadline; %d probes succeeded but op never reached READY", probeCount)
}
