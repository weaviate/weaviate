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
	doomedColdTenant = "doomed_cold"
	keepColdTenant   = "keep_cold"
)

var (
	doomedColdObjID   = strfmt.UUID("7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c11")
	keepColdObjID     = strfmt.UUID("7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c22")
	sentinelColdObjID = strfmt.UUID("7a1b2c3d-4e5f-4a6b-8c9d-0e1f2a3b4c33")
)

// An inactive tenant is unloaded by design, so it is never in Index.shards
// however the node recovers — the case a fix keyed on loaded shards cannot
// reach. keep_cold pins the boundary: equally inactive and equally on disk, so
// "not loaded" alone must not sweep it. Both are asserted in one run so a fix
// cannot buy one with the other.
func TestDeletedInactiveTenantResurrection(t *testing.T) {
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

	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	victim, victimC := docker.Weaviate2, compose.GetWeaviateNode3()
	liveC := []*docker.DockerContainer{compose.GetWeaviate(), compose.GetWeaviateNode2()}
	helper.SetupClient(compose.GetWeaviate().URI())

	class := articles.ParagraphsClass()
	class.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
	class.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	helper.CreateClass(t, class)

	helper.CreateTenants(t, className, []*models.Tenant{
		{Name: keepColdTenant, ActivityStatus: models.TenantActivityStatusHOT},
		{Name: doomedColdTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})

	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(doomedColdObjID).
		WithContents("pre-delete data").
		WithTenant(doomedColdTenant).
		Object(), types.ConsistencyLevelAll))
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(keepColdObjID).
		WithContents("survivor data").
		WithTenant(keepColdTenant).
		Object(), types.ConsistencyLevelAll))

	for _, node := range allNodes {
		_, err := helper.GetTenantObjectFromNode(t, className, doomedColdObjID, node, doomedColdTenant)
		require.NoError(t, err, "precondition: node %s must serve the pre-delete object", node)
	}

	helper.UpdateTenants(t, className, []*models.Tenant{
		{Name: doomedColdTenant, ActivityStatus: models.TenantActivityStatusCOLD},
		{Name: keepColdTenant, ActivityStatus: models.TenantActivityStatusCOLD},
	})

	for _, tenant := range []string{doomedColdTenant, keepColdTenant} {
		requireTenantDir(ctx, t, victimC, className, tenant, true,
			"precondition: deactivating tenant %q must unload it without removing its data", tenant)
	}

	require.False(t, hasRaftSnapshot(victimC),
		"precondition: node %s must not have a local RAFT snapshot yet; the setup emitted more than "+
			"RAFT_SNAPSHOT_THRESHOLD=%d commands and the scenario no longer tests what it claims",
		victim, raftSnapshotThreshold)
	require.NoError(t, compose.StopNode(ctx, 2, nil))

	require.NoError(t, helper.DeleteTenants(t, className, []string{doomedColdTenant}))
	_, err = helper.TenantExists(t, className, doomedColdTenant)
	require.Error(t, err, "tenant %q must be gone from the schema after the delete", doomedColdTenant)

	for _, c := range liveC {
		requireTenantDir(ctx, t, c, className, doomedColdTenant, false,
			"node %s applied DELETE_TENANT and must not keep the unloaded tenant's data", c.Name())
	}

	forceRaftLogTruncation(t, liveC)
	require.NoError(t, compose.StartNode(ctx, 2))
	helper.SetupClient(compose.GetWeaviate().URI())

	requireTenantDir(ctx, t, victimC, className, doomedColdTenant, false,
		"tenant %q was deleted while node %s was down and unloaded, so its data must not "+
			"survive the snapshot catch-up", doomedColdTenant, victim)

	requireTenantDir(ctx, t, victimC, className, keepColdTenant, true,
		"tenant %q is unloaded but still in the schema, so node %s must not sweep it",
		keepColdTenant, victim)

	helper.CreateTenants(t, className, []*models.Tenant{
		{Name: doomedColdTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})

	// Proves the re-created shard is live, so a "not found" below is real.
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(sentinelColdObjID).
		WithContents("post-recreate data").
		WithTenant(doomedColdTenant).
		Object(), types.ConsistencyLevelAll))

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		obj, err := helper.GetTenantObjectFromNode(t, className, sentinelColdObjID, victim, doomedColdTenant)
		if !assert.NoError(ct, err) {
			return
		}
		assert.Equal(ct, helper.ObjectContentsProp("post-recreate data"), obj.Properties)
	}, 60*time.Second, time.Second,
		"the re-created tenant %q must be live on node %s before we assert on the deleted object",
		doomedColdTenant, victim)

	for _, node := range allNodes {
		obj, err := helper.GetTenantObjectFromNode(t, className, doomedColdObjID, node, doomedColdTenant)
		if err == nil {
			t.Errorf("DATA RESURRECTION on node %s: tenant %q was deactivated, deleted and "+
				"re-created, but the node still serves the pre-delete object %s: "+
				"properties=%v creationTimeUnix=%d",
				node, doomedColdTenant, doomedColdObjID, obj.Properties, obj.CreationTimeUnix)
			continue
		}
		t.Logf("node %s correctly reports the pre-delete object as absent: %v", node, err)
	}

	helper.UpdateTenants(t, className, []*models.Tenant{
		{Name: keepColdTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})

	for _, node := range allNodes {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := helper.GetTenantObjectFromNode(t, className, keepColdObjID, node, keepColdTenant)
			if !assert.NoError(ct, err) {
				return
			}
			assert.Equal(ct, helper.ObjectContentsProp("survivor data"), obj.Properties)
		}, 60*time.Second, time.Second,
			"tenant %q must be untouched on node %s", keepColdTenant, node)
	}
}

// The only assertion that reaches an unloaded tenant: with no shard registered
// the API serves nothing, so disk is the sole evidence.
func requireTenantDir(ctx context.Context, t *testing.T, c *docker.DockerContainer,
	class, tenant string, want bool, msg string, args ...any,
) {
	t.Helper()

	dir := fmt.Sprintf("data/%s/%s", strings.ToLower(class), tenant)
	explanation := fmt.Sprintf(msg, args...)

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		code, _, err := c.Container().Exec(ctx, []string{"test", "-d", dir})
		if !assert.NoError(ct, err, "exec on node %s", c.Name()) {
			return
		}
		assert.Equal(ct, want, code == 0, "%s (%s)", explanation, dir)
	}, 15*time.Second, 250*time.Millisecond, explanation)
}
