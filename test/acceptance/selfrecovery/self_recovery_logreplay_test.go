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
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/replication"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/test/helper/sample-schema/articles"
)

func hasSelfRecoveryOp(t *testing.T, targetNode string) (bool, error) {
	body, err := helper.Client(t).Replication.ListReplication(
		replication.NewListReplicationParams().WithTargetNode(&targetNode), nil)
	if err != nil {
		return false, err
	}
	for _, op := range body.Payload {
		if op.Type != nil && *op.Type == "SELF_RECOVERY" {
			return true, nil
		}
	}
	return false, nil
}

func selfRecoveryOpTerminal(op *models.ReplicationReplicateDetailsReplicaResponse) bool {
	if op.Status == nil {
		return false
	}
	switch op.Status.State {
	case models.ReplicationReplicateDetailsReplicaStatusStateREADY,
		models.ReplicationReplicateDetailsReplicaStatusStateCANCELLED:
		return true
	default:
		return false
	}
}

func hasActiveSelfRecoveryOp(t *testing.T, targetNode string) (bool, error) {
	body, err := helper.Client(t).Replication.ListReplication(
		replication.NewListReplicationParams().WithTargetNode(&targetNode), nil)
	if err != nil {
		return false, err
	}
	for _, op := range body.Payload {
		if op.Type != nil && *op.Type == "SELF_RECOVERY" && !selfRecoveryOpTerminal(op) {
			return true, nil
		}
	}
	return false, nil
}

func waitForSelfRecoveryToSettle(t *testing.T, nodeNames []string, timeout time.Duration) {
	t.Helper()
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		for _, n := range nodeNames {
			active, err := hasActiveSelfRecoveryOp(t, n)
			require.NoError(ct, err)
			require.False(ct, active, "node %s still has an in-progress SELF_RECOVERY op", n)
		}
	}, timeout, 1*time.Second, "self-recovery did not settle on the cluster")
}

func TestSelfRecoveryViaLogReplay(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true})

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

	t.Run("create RF=3 single-shard collection and ingest", func(t *testing.T) {
		paragraphClass.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		paragraphClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		paragraphClass.Vectorizer = "none"
		helper.CreateClass(t, paragraphClass)
		waitShardsLoaded(t, paragraphClass.Class, 1)

		batch := make([]*models.Object, objCount)
		for i := 0; i < objCount; i++ {
			batch[i] = articles.NewParagraph().
				WithID(strfmt.UUID(fmt.Sprintf("00000000-0000-0000-0000-%012d", i+1))).
				WithContents(fmt.Sprintf("paragraph#%d", i)).
				Object()
		}
		submitBatch(t, batch, "")
	})

	t.Run("creating a collection on a healthy cluster does not recover", func(t *testing.T) {
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)

		fresh := articles.ArticlesClass()
		fresh.ShardingConfig = map[string]interface{}{"desiredCount": 1}
		fresh.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		fresh.Vectorizer = "none"
		helper.CreateClass(t, fresh)

		assertNoActiveRecovery(t, allNodes, 10*time.Second)
	})

	t.Run("wipe node-3 data and restart (rejoins via log replay)", func(t *testing.T) {
		wipeAndRestart(ctx, t, compose, wipedIdx)
	})

	t.Run("a SELF_RECOVERY op fires for the wiped node", func(t *testing.T) {
		waitSelfRecoveryOpFired(t, wipedNodeName)
	})

	t.Run("recovery completes and the wiped node reports full object count", func(t *testing.T) {
		assertNodeRecovered(t, paragraphClass.Class, wipedNodeName, 1, int64(objCount))
	})
}

func TestSelfRecoveryViaLogReplayMultiTenant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose := startSelfRecoveryCluster(ctx, t, srClusterCfg{asyncDisabled: true})

	const (
		wipedIdx      = 2
		objsPerTenant = 50
	)
	wipedNodeName := docker.Weaviate2
	allNodes := []string{docker.Weaviate0, docker.Weaviate1, docker.Weaviate2}
	tenants := []string{"tenantA", "tenantB", "tenantC"}
	mtClass := articles.ParagraphsClass()

	t.Run("wait for cluster to form quorum", func(t *testing.T) {
		waitClusterHealthy(t)
	})

	t.Run("create RF=3 multi-tenant collection with HOT tenants", func(t *testing.T) {
		mtClass.MultiTenancyConfig = &models.MultiTenancyConfig{Enabled: true}
		mtClass.ReplicationConfig = &models.ReplicationConfig{Factor: 3}
		mtClass.Vectorizer = "none"
		helper.CreateClass(t, mtClass)

		ts := make([]*models.Tenant, len(tenants))
		for i, name := range tenants {
			ts[i] = &models.Tenant{Name: name, ActivityStatus: "HOT"}
		}
		helper.CreateTenants(t, mtClass.Class, ts)
	})

	t.Run("wait for tenant shards to be placed on all nodes", func(t *testing.T) {
		waitShardsLoaded(t, mtClass.Class, len(tenants))
	})

	t.Run("ingest objects per tenant", func(t *testing.T) {
		for _, tenant := range tenants {
			batch := make([]*models.Object, objsPerTenant)
			for i := 0; i < objsPerTenant; i++ {
				batch[i] = articles.NewParagraph().
					WithContents(fmt.Sprintf("%s#%d", tenant, i)).
					WithTenant(tenant).
					Object()
			}
			submitBatch(t, batch, "")
		}
	})

	t.Run("creating tenants on a healthy cluster does not recover", func(t *testing.T) {
		waitForSelfRecoveryToSettle(t, allNodes, 3*time.Minute)
		assertNoActiveRecovery(t, allNodes, 10*time.Second)
	})

	t.Run("wipe node-3 data and restart (rejoins via log replay)", func(t *testing.T) {
		wipeAndRestart(ctx, t, compose, wipedIdx)
	})

	t.Run("SELF_RECOVERY ops fire for the wiped node's tenant shards", func(t *testing.T) {
		waitSelfRecoveryOpFired(t, wipedNodeName)
	})

	t.Run("recovery completes and the wiped node reports all tenant shards", func(t *testing.T) {
		assertNodeRecovered(t, mtClass.Class, wipedNodeName, len(tenants), int64(objsPerTenant))
	})
}
