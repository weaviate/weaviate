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

package namespace_limits

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestObjectLimitAfterReplicaCopy copies an RF=1 shard onto a second node, so
// every later put goes through Replicator.PutObject. A put entering on a third
// node still gets the cap's 429, because each replica checks it at prepare.
func TestObjectLimitAfterReplicaCopy(t *testing.T) {
	t.Parallel()
	const (
		sourceNode = docker.Weaviate1
		targetNode = docker.Weaviate2
	)
	ns := uniqueNS()

	require.NotContains(t, []string{sourceNode, targetNode}, sharedCompose.GetWeaviate().Name(),
		"test invariant: the client must enter on a node that holds no replica")

	helper.CreateNamespaceWithHomeNode(t, ns, sourceNode, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u", ns)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u", adminKey) })

	helper.CreateClassAuth(t, &models.Class{Class: "Items"}, userKey)
	qualified := ns + ":Items"
	t.Cleanup(func() { helper.DeleteClassAuth(t, qualified, adminKey) })

	// One object materializes the shard, so the COPY has a source replica.
	require.NoError(t, insertItem(t, "Items", userKey, 0))

	var shardName string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		shards, err := helper.ShardsForClass(t, qualified, adminKey)
		if assert.NoError(c, err) && assert.Len(c, shards, 1) {
			shardName = shards[0]
		}
	}, 30*time.Second, 250*time.Millisecond, "the shard never materialized")

	opID := helper.StartCopyMovement(t, qualified, shardName, sourceNode, targetNode, adminKey)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		state, messages, err := helper.ReplicationOpState(t, opID, adminKey)
		if assert.NoError(c, err) {
			assert.Equal(c, "READY", state, "COPY still %q; errors %v", state, messages)
		}
	}, 180*time.Second, 2*time.Second, "the COPY never finished")

	fillUntilQuota(t, "Items", userKey)
}
