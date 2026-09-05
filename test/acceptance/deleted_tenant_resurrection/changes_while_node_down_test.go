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
	"strconv"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	clientschema "github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

const (
	stableHotTenant  = "stable_hot"
	stableColdTenant = "stable_cold"
	addedTenant      = "added_while_down"
)

var (
	stableHotObjID   = strfmt.UUID("5c2d7e91-3a4b-4c5d-9e8f-1a2b3c4d5e11")
	stableColdObjID  = strfmt.UUID("5c2d7e91-3a4b-4c5d-9e8f-1a2b3c4d5e22")
	lateHotObjID     = strfmt.UUID("5c2d7e91-3a4b-4c5d-9e8f-1a2b3c4d5e33")
	addedTenantObjID = strfmt.UUID("5c2d7e91-3a4b-4c5d-9e8f-1a2b3c4d5e44")
	newPropObjID     = strfmt.UUID("5c2d7e91-3a4b-4c5d-9e8f-1a2b3c4d5e55")
)

// Ordinary multi-tenant changes made while a node is down must survive the same
// InstallSnapshot catch-up, and the destructive directory sweep on that path
// must touch none of them. Covers every state a live tenant can arrive in:
// written to again, deactivated, and created while the node was absent.
func TestChangesWhileNodeDown(t *testing.T) {
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
		{Name: stableHotTenant, ActivityStatus: models.TenantActivityStatusHOT},
		{Name: stableColdTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(stableHotObjID).WithContents("hot before outage").
		WithTenant(stableHotTenant).Object(), types.ConsistencyLevelAll))
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(stableColdObjID).WithContents("cold before outage").
		WithTenant(stableColdTenant).Object(), types.ConsistencyLevelAll))

	require.False(t, hasRaftSnapshot(victimC),
		"precondition: node %s must not have a local RAFT snapshot yet", victim)
	require.NoError(t, compose.StopNode(ctx, 2, nil))

	helper.CreateTenants(t, className, []*models.Tenant{
		{Name: addedTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(addedTenantObjID).WithContents("added while down").
		WithTenant(addedTenant).Object(), types.ConsistencyLevelQuorum))
	require.NoError(t, helper.CreateObjectCL(t, articles.NewParagraph().
		WithID(lateHotObjID).WithContents("hot during outage").
		WithTenant(stableHotTenant).Object(), types.ConsistencyLevelQuorum))
	helper.UpdateTenants(t, className, []*models.Tenant{
		{Name: stableColdTenant, ActivityStatus: models.TenantActivityStatusCOLD},
	})
	_, err = helper.Client(t).Schema.SchemaObjectsPropertiesAdd(
		clientschema.NewSchemaObjectsPropertiesAddParams().
			WithClassName(className).
			WithBody(&models.Property{Name: "author", DataType: schema.DataTypeText.PropString()}),
		nil)
	require.NoError(t, err)

	forceRaftLogTruncation(t, liveC)
	require.NoError(t, compose.StartNode(ctx, 2))
	helper.SetupClient(compose.GetWeaviate().URI())

	// Nothing the schema still names may be swept.
	for _, tenant := range []string{stableHotTenant, stableColdTenant, addedTenant} {
		requireTenantDir(ctx, t, victimC, className, tenant, true,
			"tenant %q is in the schema, so node %s must not sweep it during catch-up",
			tenant, victim)
	}

	for _, tc := range []struct {
		tenant   string
		id       strfmt.UUID
		contents string
	}{
		{stableHotTenant, stableHotObjID, "hot before outage"},
		{stableHotTenant, lateHotObjID, "hot during outage"},
		{addedTenant, addedTenantObjID, "added while down"},
	} {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := helper.GetTenantObjectFromNode(t, className, tc.id, victim, tc.tenant)
			if !assert.NoError(ct, err) {
				return
			}
			assert.Equal(ct, helper.ObjectContentsProp(tc.contents), obj.Properties)
		}, 90*time.Second, time.Second,
			"node %s must converge on object %s of tenant %q after catch-up", victim, tc.id, tc.tenant)
	}

	// Readable only once reloaded, so its data survived the catch-up.
	helper.UpdateTenants(t, className, []*models.Tenant{
		{Name: stableColdTenant, ActivityStatus: models.TenantActivityStatusHOT},
	})
	for _, node := range allNodes {
		require.EventuallyWithT(t, func(ct *assert.CollectT) {
			obj, err := helper.GetTenantObjectFromNode(t, className, stableColdObjID, node, stableColdTenant)
			if !assert.NoError(ct, err) {
				return
			}
			assert.Equal(ct, helper.ObjectContentsProp("cold before outage"), obj.Properties)
		}, 90*time.Second, time.Second,
			"reactivated tenant %q must still hold its data on node %s", stableColdTenant, node)
	}

	// Usable on the recovered node, not merely present in its schema.
	obj := articles.NewParagraph().
		WithID(newPropObjID).WithContents("uses late property").
		WithTenant(stableHotTenant).Object()
	obj.Properties.(map[string]interface{})["author"] = "written after catch-up"
	require.NoError(t, helper.CreateObjectCL(t, obj, types.ConsistencyLevelAll))

	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		got, err := helper.GetTenantObjectFromNode(t, className, newPropObjID, victim, stableHotTenant)
		if !assert.NoError(ct, err) {
			return
		}
		props, ok := got.Properties.(map[string]interface{})
		if !assert.True(ct, ok, "unexpected property payload %T", got.Properties) {
			return
		}
		assert.Equal(ct, "written after catch-up", props["author"])
	}, 90*time.Second, time.Second,
		"node %s must accept the property added while it was down", victim)
}
