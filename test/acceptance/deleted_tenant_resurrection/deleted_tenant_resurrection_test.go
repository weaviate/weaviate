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

package deleted_tenant_resurrection

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	className    = "Paragraph"
	keepTenant   = "keep"
	doomedTenant = "doomed"

	// Must sit above the RAFT commands the setup emits and below the churn. At a
	// threshold of 1 the victim snapshots its own pre-delete state every second
	// or so; restoring that registers the shard, the reconcile then drops it,
	// and the failure is masked — observed on 4 of 5 runs.
	raftSnapshotThreshold = 50

	churnClasses = 40 // create+delete pairs, two RAFT commands each
)

var (
	doomedObjID   = strfmt.UUID("6f9a5f4b-1f3e-4a6f-9a1c-2b4d6e8f0a11")
	keepObjID     = strfmt.UUID("6f9a5f4b-1f3e-4a6f-9a1c-2b4d6e8f0a22")
	sentinelObjID = strfmt.UUID("6f9a5f4b-1f3e-4a6f-9a1c-2b4d6e8f0a33")
)

// A node that misses a DELETE_TENANT and is caught up by InstallSnapshot keeps
// the deleted tenant's directory, and a re-created tenant of the same name then
// serves the pre-delete data.
//
// The leader snapshot is the control variable, asserted directly on both sides.
// With RAFT_TRAILING_LOGS=1 a snapshot means the log was compacted past the
// delete, leaving the victim no route back except InstallSnapshot; no snapshot
// means the log survived and replays.
func TestDeletedTenantResurrection(t *testing.T) {
	tests := []struct {
		name            string
		forceTruncation bool
	}{
		{name: "delete_lost_to_raft_snapshot", forceTruncation: true},
		{name: "delete_replayed_from_raft_log", forceTruncation: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runResurrectionScenario(t, tt.forceTruncation)
		})
	}
}

func runResurrectionScenario(t *testing.T, forceTruncation bool) {
	ctx := context.Background()

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("RAFT_SNAPSHOT_THRESHOLD", strconv.Itoa(raftSnapshotThreshold)).
		WithWeaviateEnv("RAFT_SNAPSHOT_INTERVAL", "1").
		WithWeaviateEnv("RAFT_TRAILING_LOGS", "1").
		Start(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := compose.Terminate(context.Background()); err != nil {
			t.Logf("failed to terminate test containers: %v", err)
		}
	})
	t.Cleanup(helper.ResetClient)

	// RF=3 puts every tenant on every node, so the re-created tenant lands back
	// on the victim without a placement lottery.
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	victim, victimC := docker.Weaviate2, compose.GetWeaviateNode3()
	liveC := []*docker.DockerContainer{compose.GetWeaviate(), compose.GetWeaviateNode2()}
	helper.SetupClient(compose.GetWeaviate().URI())

	class := articles.ParagraphsClass()
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	helper.CreateClass(t, class)

	helper.CreateTenants(t, className, []*models.Tenant{
		{Name: keepTenant, ActivityStatus: models.TenantActivityStatusHOT},
		{Name: doomedTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})

	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(doomedObjID).
		WithContents("pre-delete data").
		WithTenant(doomedTenant).
		Object(), types.ConsistencyLevelAll))
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(keepObjID).
		WithContents("survivor data").
		WithTenant(keepTenant).
		Object(), types.ConsistencyLevelAll))

	for _, node := range allNodes {
		obj, err := helper.GetTenantObjectFromNode(t, className, doomedObjID, node, doomedTenant)
		require.NoError(t, err, "precondition: node %s must serve the pre-delete object", node)
		require.Equal(t, helper.ObjectContentsProp("pre-delete data"), obj.Properties)
	}

	require.False(t, hasRaftSnapshot(victimC),
		"precondition: node %s must not have a local RAFT snapshot yet; the setup emitted more than "+
			"RAFT_SNAPSHOT_THRESHOLD=%d commands and the scenario no longer tests what it claims",
		victim, raftSnapshotThreshold)

	require.NoError(t, compose.StopNode(ctx, 2, nil))

	require.NoError(t, helper.DeleteTenants(t, className, []string{doomedTenant}))

	_, err = helper.TenantExists(t, className, doomedTenant)
	require.Error(t, err, "tenant %q must be gone from the schema after the delete", doomedTenant)

	for _, node := range allNodes {
		if node == victim {
			continue
		}
		_, err := helper.GetTenantObjectFromNode(t, className, doomedObjID, node, doomedTenant)
		require.Error(t, err,
			"node %s applied DELETE_TENANT and must no longer serve the object", node)
	}

	if forceTruncation {
		forceRaftLogTruncation(t, liveC)
	} else {
		for _, c := range liveC {
			require.False(t, hasRaftSnapshot(c),
				"control: node %s snapshotted, so its log may have been compacted past the delete "+
					"and node %s would not replay it — this no longer isolates the trigger",
				c.Name(), victim)
		}
	}

	require.NoError(t, compose.StartNode(ctx, 2))
	// Stays on the coordinator; the assertions below pin the reader with
	// node_name, so the victim's own shard answers without catch-up 503s.
	helper.SetupClient(compose.GetWeaviate().URI())

	t.Logf("after restart, node %s tenant directories for class %q: %s",
		victim, className, tenantDirs(t, victimC, className))

	helper.CreateTenants(t, className, []*models.Tenant{
		{Name: doomedTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})

	// Proves the re-created shard is live on the victim, so a "not found" below
	// cannot just mean "not ready yet".
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(sentinelObjID).
		WithContents("post-recreate data").
		WithTenant(doomedTenant).
		Object(), types.ConsistencyLevelAll))

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		obj, err := helper.GetTenantObjectFromNode(t, className, sentinelObjID, victim, doomedTenant)
		if !assert.NoError(ct, err) {
			return
		}
		assert.Equal(ct, helper.ObjectContentsProp("post-recreate data"), obj.Properties)
	}, 60*time.Second, time.Second,
		"the re-created tenant %q must be live on node %s before we assert on the deleted object",
		doomedTenant, victim)

	for _, node := range allNodes {
		obj, err := helper.GetTenantObjectFromNode(t, className, doomedObjID, node, doomedTenant)
		if err == nil {
			t.Errorf("DATA RESURRECTION on node %s: tenant %q was deleted and re-created, "+
				"but the node still serves the pre-delete object %s: properties=%v creationTimeUnix=%d",
				node, doomedTenant, doomedObjID, obj.Properties, obj.CreationTimeUnix)
			continue
		}
		t.Logf("node %s correctly reports the pre-delete object as absent: %v", node, err)
	}

	for _, node := range allNodes {
		obj, err := helper.GetTenantObjectFromNode(t, className, keepObjID, node, keepTenant)
		require.NoError(t, err, "tenant %q must be untouched on node %s", keepTenant, node)
		require.Equal(t, helper.ObjectContentsProp("survivor data"), obj.Properties)
	}
}

// forceRaftLogTruncation churns schema commands until every live node has
// snapshotted. Churn is the only lever; no endpoint triggers a RAFT snapshot.
// The classes are multi-tenant with no tenants, so each costs a RAFT command
// without creating shards.
func forceRaftLogTruncation(t *testing.T, live []*docker.DockerContainer) {
	t.Helper()

	for i := 0; i < churnClasses; i++ {
		name := fmt.Sprintf("Churn_%d", i)
		helper.CreateClass(t, &models.Class{
			Class:              name,
			Vectorizer:         "none",
			MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		})
		helper.DeleteClass(t, name)
	}

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, c := range live {
			assert.True(ct, hasRaftSnapshot(c), "node %s has not snapshotted yet", c.Name())
		}
	}, 90*time.Second, time.Second,
		"RAFT snapshot was never written on the live nodes — the log was not truncated past the delete")
}

// hasRaftSnapshot answers via the exit code, avoiding the docker stream.
func hasRaftSnapshot(c *docker.DockerContainer) bool {
	code, _, err := c.Container().Exec(context.Background(),
		[]string{"sh", "-c", `[ -n "$(ls -A data/raft/snapshots 2>/dev/null)" ]`})
	return err == nil && code == 0
}

// tenantDirs lists a node's shard directories; the index dir is the lowercased
// class name.
func tenantDirs(t *testing.T, c *docker.DockerContainer, class string) string {
	t.Helper()

	_, reader, err := c.Container().Exec(context.Background(),
		[]string{"sh", "-c", fmt.Sprintf("ls -1 data/%s 2>/dev/null | tr '\\n' ' '", strings.ToLower(class))})
	if err != nil {
		return fmt.Sprintf("<unavailable: %v>", err)
	}
	out, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Sprintf("<unavailable: %v>", err)
	}
	// Strip the docker stream framing bytes.
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 32 || r > 126 {
			return -1
		}
		return r
	}, string(out)))
}
