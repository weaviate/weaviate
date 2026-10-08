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
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	numNodes            = 3
	initialObjects      = 300
	downtimeObjects     = 20
	maxHydratingErrors  = 10
	copySleep           = "20s"
	hydratingTimeout    = 90 * time.Second
	opCompletionTimeout = 5 * time.Minute
	stuckChangelogErr   = "file exists"
)

// TestReplicaOpResumesAfterTargetSIGKILL pins that a COPY/MOVE whose target is
// SIGKILLed mid-HYDRATING resumes to READY instead of failing StartChangeCapture
// with "file exists" on the donor's leftover change-capture log until auto-cancel.
func TestReplicaOpResumesAfterTargetSIGKILL(t *testing.T) {
	tests := []struct {
		name         string
		transferType string
	}{
		{name: "copy", transferType: models.ReplicationReplicateReplicaRequestTypeCOPY},
		{name: "move", transferType: models.ReplicationReplicateReplicaRequestTypeMOVE},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runTargetCrashResume(t, tt.transferType)
		})
	}
}

func runTargetCrashResume(t *testing.T, transferType string) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(numNodes).
		WithWeaviateEnv("WEAVIATE_TEST_COPY_REPLICA_SLEEP", copySleep).
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			var sb strings.Builder
			compose.DumpWeaviateLogs(ctx, &sb, 300)
			t.Log(sb.String())
		}
		if err := compose.Terminate(ctx); err != nil {
			t.Errorf("failed to terminate test containers: %v", err)
		}
	})

	className := "CrashResume" + strings.ToUpper(transferType[:1]) + strings.ToLower(transferType[1:])
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.CreateClass(t, &models.Class{
		Class:             className,
		Vectorizer:        "none",
		Properties:        []*models.Property{{Name: "contents", DataType: []string{"text"}}},
		ShardingConfig:    map[string]interface{}{"desiredCount": 1},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	})

	ids := insertObjects(t, compose.GetWeaviate().URI(), className, 0, initialObjects)

	sourceIdx, shardName := findShardHolder(t, compose, className)
	targetIdx := (sourceIdx + 1) % numNodes
	sourceName, targetName := nodeName(sourceIdx), nodeName(targetIdx)
	controlURI := func() string { return compose.GetWeaviateNode(sourceIdx + 1).URI() }
	t.Logf("%s shard %s from %s to %s", transferType, shardName, sourceName, targetName)

	helper.SetupClient(controlURI())
	resp, err := helper.Client(t).Replication.Replicate(
		replication.NewReplicateParams().WithBody(&models.ReplicationReplicateReplicaRequest{
			Collection: &className,
			Shard:      &shardName,
			SourceNode: &sourceName,
			TargetNode: &targetName,
			Type:       &transferType,
		}), nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.Code())
	opID := *resp.Payload.ID

	waitOpState(t, controlURI(), opID, "HYDRATING", hydratingTimeout)
	require.Eventually(t, func() bool {
		return changelogPresent(t, compose.GetWeaviateNode(sourceIdx+1), shardName)
	}, hydratingTimeout, 200*time.Millisecond, "donor %s never opened a change-capture log for shard %s", sourceName, shardName)

	require.NoError(t, compose.KillNode(ctx, targetIdx))
	t.Logf("SIGKILLed target %s mid-HYDRATING", targetName)

	ids = append(ids, insertObjects(t, controlURI(), className, initialObjects, downtimeObjects)...)

	require.NoError(t, compose.StartNode(ctx, targetIdx))
	t.Logf("restarted target %s", targetName)

	details := waitOpState(t, controlURI(), opID, "READY", opCompletionTimeout)

	hydratingErrs := errorsInState(details, "HYDRATING")
	for _, msg := range hydratingErrs {
		assert.NotContains(t, msg, stuckChangelogErr)
	}
	assert.Less(t, len(hydratingErrs), maxHydratingErrors, "HYDRATING errors: %v", hydratingErrs)

	targetURI := compose.GetWeaviateNode(targetIdx + 1).URI()
	var missing []strfmt.UUID
	for _, id := range ids {
		if _, err := common.GetObjectFromNode(t, targetURI, className, id, targetName); err != nil {
			missing = append(missing, id)
		}
	}
	assert.Empty(t, missing, "objects missing on target %s", targetName)

	assert.EventuallyWithT(t, func(ct *assert.CollectT) {
		counts := shardObjectCounts(t, controlURI(), className)
		assert.Equal(ct, int64(len(ids)), counts[targetName], "object count on target %s", targetName)
		_, sourceHolds := counts[sourceName]
		if transferType == models.ReplicationReplicateReplicaRequestTypeMOVE {
			assert.False(ct, sourceHolds, "source %s still holds the moved replica", sourceName)
		} else {
			assert.True(ct, sourceHolds, "source %s lost the copied replica", sourceName)
		}
	}, 2*time.Minute, time.Second)
}

func nodeName(idx int) string {
	return fmt.Sprintf("weaviate-%d", idx)
}

func insertObjects(t *testing.T, uri, className string, offset, n int) []strfmt.UUID {
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
	common.CreateObjects(t, uri, objs)
	return ids
}

func findShardHolder(t *testing.T, compose *docker.DockerCompose, className string) (int, string) {
	t.Helper()
	for node, shards := range shardsByNode(t, compose.GetWeaviate().URI(), className) {
		for shard := range shards {
			for idx := 0; idx < numNodes; idx++ {
				if nodeName(idx) == node {
					return idx, shard
				}
			}
		}
	}
	require.FailNow(t, "no node holds a shard", "class %s", className)
	return -1, ""
}

func shardsByNode(t *testing.T, uri, className string) map[string]map[string]int64 {
	t.Helper()
	helper.SetupClient(uri)
	verbose := verbosity.OutputVerbose
	body, err := helper.Client(t).Nodes.NodesGetClass(
		nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), nil)
	require.NoError(t, err)
	out := map[string]map[string]int64{}
	for _, n := range body.Payload.Nodes {
		for _, s := range n.Shards {
			if s.Class != className {
				continue
			}
			if out[n.Name] == nil {
				out[n.Name] = map[string]int64{}
			}
			out[n.Name][s.Name] = s.ObjectCount
		}
	}
	return out
}

func shardObjectCounts(t *testing.T, uri, className string) map[string]int64 {
	t.Helper()
	out := map[string]int64{}
	for node, shards := range shardsByNode(t, uri, className) {
		for _, c := range shards {
			out[node] += c
		}
	}
	return out
}

// changelogPresent reports whether the donor holds a change-capture log for the shard; logs are named by the op's numeric FSM id, not its UUID.
func changelogPresent(t *testing.T, c *docker.DockerContainer, shardName string) bool {
	t.Helper()
	code, reader, err := c.Container().Exec(context.Background(), []string{"find", "/data", "-path", "*/" + shardName + "/changelog/*.log"})
	if err != nil || code != 0 {
		return false
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return false
	}
	return strings.Contains(string(out), "/changelog/")
}

func waitOpState(t *testing.T, uri string, opID strfmt.UUID, want string, timeout time.Duration) *models.ReplicationReplicateDetailsReplicaResponse {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last *models.ReplicationReplicateDetailsReplicaResponse
	for time.Now().Before(deadline) {
		helper.SetupClient(uri)
		details, err := helper.Client(t).Replication.ReplicationDetails(
			replication.NewReplicationDetailsParams().WithID(opID).WithIncludeHistory(boolPtr(true)), nil)
		if err == nil && details.Payload != nil && details.Payload.Status != nil {
			last = details.Payload
			switch last.Status.State {
			case want:
				return last
			case "CANCELLED":
				require.FailNowf(t, "replication op cancelled", "op %s reached CANCELLED waiting for %s; errors:\n%s", opID, want, formatErrors(last))
			}
			if hasErrorContaining(last, stuckChangelogErr) {
				require.FailNowf(t, "replication op stuck on leftover change-capture log", "op %s retries StartChangeCapture against a donor log that already exists; errors:\n%s", opID, formatErrors(last))
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	require.FailNowf(t, "replication op timed out", "op %s not %s after %s; last: %s; errors:\n%s", opID, want, timeout, stateOf(last), formatErrors(last))
	return nil
}

func hasErrorContaining(d *models.ReplicationReplicateDetailsReplicaResponse, substr string) bool {
	for _, s := range allStatuses(d) {
		if s == nil {
			continue
		}
		for _, e := range s.Errors {
			if strings.Contains(e.Message, substr) {
				return true
			}
		}
	}
	return false
}

func stateOf(d *models.ReplicationReplicateDetailsReplicaResponse) string {
	if d == nil || d.Status == nil {
		return "<unknown>"
	}
	return d.Status.State
}

func allStatuses(d *models.ReplicationReplicateDetailsReplicaResponse) []*models.ReplicationReplicateDetailsReplicaStatus {
	if d == nil {
		return nil
	}
	out := append([]*models.ReplicationReplicateDetailsReplicaStatus{}, d.StatusHistory...)
	if d.Status != nil {
		out = append(out, d.Status)
	}
	return out
}

func errorsInState(d *models.ReplicationReplicateDetailsReplicaResponse, state string) []string {
	var out []string
	for _, s := range allStatuses(d) {
		if s == nil || s.State != state {
			continue
		}
		for _, e := range s.Errors {
			out = append(out, e.Message)
		}
	}
	return out
}

func formatErrors(d *models.ReplicationReplicateDetailsReplicaResponse) string {
	var sb strings.Builder
	for _, s := range allStatuses(d) {
		if s == nil {
			continue
		}
		fmt.Fprintf(&sb, "  %s: %d errors\n", s.State, len(s.Errors))
		for _, e := range s.Errors {
			fmt.Fprintf(&sb, "    %s\n", e.Message)
		}
	}
	return sb.String()
}

func boolPtr(b bool) *bool { return &b }
