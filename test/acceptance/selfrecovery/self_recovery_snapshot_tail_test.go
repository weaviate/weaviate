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
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/assert"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/acceptance/replication/common"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func srParagraphClass(name string) *models.Class {
	c := articles.ParagraphsClass()
	c.Class = name
	c.ShardingConfig = map[string]interface{}{"desiredCount": 1}
	c.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
	c.Vectorizer = "none"
	return c
}

func srParagraphObjects(class, idPrefix string, n int, tenant string) []*models.Object {
	objs := make([]*models.Object, n)
	for i := 0; i < n; i++ {
		p := articles.NewParagraph().
			WithID(strfmt.UUID(fmt.Sprintf("%s-%012d", idPrefix, i+1))).
			WithContents(fmt.Sprintf("paragraph#%d", i))
		if tenant != "" {
			p = p.WithTenant(tenant)
		}
		o := p.Object()
		o.Class = class
		objs[i] = o
	}
	return objs
}

// Pins snapshot-tail divergence: changes between last snapshot and join barrier must still self-recover.
func TestSelfRecoverySnapshotTailChangesRecover(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true, raftTrailingLogs: true, debugPort: true})

	const (
		baseClass       = "BasePara"
		tailClass       = "TailPara"
		tenantedClass   = "TailTenanted"
		tailTenant      = "tail-tenant"
		baseCount       = 300
		tailCount       = 250
		tailTenantCount = 200
		wipedIdx        = 2
	)
	wipedNodeName := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}

	mustRun(t, "wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
	})

	mustRun(t, "create pre-snapshot collections", func(t *testing.T) {
		ensureClass(t, srParagraphClass(baseClass))
		mt := srParagraphClass(tenantedClass)
		mt.ShardingConfig = nil
		mt.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		ensureClass(t, mt)
	})

	mustRun(t, "verify all 3 nodes report shard loaded", func(t *testing.T) {
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		waitShardsLoaded(t, baseClass, 1)
	})

	mustRun(t, "ingest baseline objects", func(t *testing.T) {
		submitBatch(t, srParagraphObjects(baseClass, "00000000-0000-0000-0000", baseCount, ""), "")
	})

	mustRun(t, "force a RAFT snapshot on every node", func(t *testing.T) {
		for i := 0; i < 3; i++ {
			forceRaftSnapshot(ctx, t, compose, i)
		}
	})

	mustRun(t, "wipe and stop node-3", func(t *testing.T) {
		common.WipeNodeDataAt(ctx, t, compose, wipedIdx)
	})

	mustRun(t, "commit tail schema changes and data while node-3 is down", func(t *testing.T) {
		ensureClass(t, srParagraphClass(tailClass))
		ensureTenants(t, tenantedClass, []*models.Tenant{{Name: tailTenant}})
		submitBatch(t, srParagraphObjects(tailClass, "22222222-2222-2222-2222", tailCount, ""), types.ConsistencyLevelQuorum)
		submitBatch(t, srParagraphObjects(tenantedClass, "33333333-3333-3333-3333", tailTenantCount, tailTenant), types.ConsistencyLevelQuorum)
	})

	mustRun(t, "restart node-3", func(t *testing.T) {
		common.StartNodeAt(ctx, t, compose, wipedIdx)
		helper.SetupClient(compose.GetWeaviate().URI())
	})

	mustRun(t, "a SELF_RECOVERY op was registered for node-3", func(t *testing.T) {
		waitSelfRecoveryOpFired(t, wipedNodeName)
	})

	mustRun(t, "pre-snapshot collection recovers on node-3", func(t *testing.T) {
		assertNodeRecovered(t, baseClass, wipedNodeName, 1, int64(baseCount))
	})

	mustRun(t, "tail collection recovers on node-3", func(t *testing.T) {
		assertNodeRecovered(t, tailClass, wipedNodeName, 1, int64(tailCount))
	})

	mustRun(t, "tail tenant recovers on node-3", func(t *testing.T) {
		assertNodeRecovered(t, tenantedClass, wipedNodeName, 1, int64(tailTenantCount))
	})

	mustRun(t, "direct query to node-3 returns tail data at consistency=ONE", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			for i := 0; i < 10; i++ {
				id := strfmt.UUID(fmt.Sprintf("22222222-2222-2222-2222-%012d", i+1))
				exists, err := common.ObjectExistsCL(t, compose.ContainerURI(wipedIdx), tailClass, id, types.ConsistencyLevelOne)
				assert.NoError(ct, err)
				assert.True(ct, exists, "tail object %s missing on node-3", id)
			}
		}, 30*time.Second, 1*time.Second, "node-3 tail data not available at consistency=ONE")
	})
}
