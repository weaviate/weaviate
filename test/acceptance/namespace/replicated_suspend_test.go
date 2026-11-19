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

package namespace

import (
	"testing"

	"github.com/go-openapi/strfmt"
	"github.com/google/uuid"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestNamespaces_SuspendClosesEveryReplica suspends a namespace whose RF=2 class
// and tenant sit on both its home nodes. Each node releases both replicas, and
// after the resume a write at ALL reopens them with the earlier object.
func TestNamespaces_SuspendClosesEveryReplica(t *testing.T) {
	t.Parallel()

	homeNodes := []string{docker.Weaviate1, docker.Weaviate2}
	ns := uniqueNS()
	helper.CreateNamespaceWithHomeNodes(t, ns, homeNodes, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })
	userKey := createNamespacedUser(t, "u1", ns, adminKey)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u1", adminKey) })
	live := map[string]presentShard{}
	for _, node := range homeNodes {
		live[node] = newPresentShard(t, node)
	}

	targets := newRF2Shards(t, ns, userKey)
	ids := make([]strfmt.UUID, len(targets))
	requireEveryReplicaServes := func(t *testing.T) {
		t.Helper()
		for i, tg := range targets {
			for _, node := range homeNodes {
				requireReplicaServes(t, ns, node, tg, ids[i], userKey)
			}
		}
	}

	t.Run("every home node serves both replicas before the suspend", func(t *testing.T) {
		for i, tg := range targets {
			ids[i] = strfmt.UUID(uuid.NewString())
			writeAtAllEventually(t, tg, ids[i], userKey)
		}
		requireEveryReplicaServes(t)
	})

	helper.SuspendNamespace(t, ns, adminKey)
	t.Cleanup(func() { helper.ResumeNamespace(t, ns, adminKey) })

	t.Run("every home node releases both replicas", func(t *testing.T) {
		for _, node := range homeNodes {
			for _, tg := range targets {
				requireShardReleasedOnNode(t, ns+":"+tg.class, tg.shard, node, live[node])
			}
		}
	})

	helper.ResumeNamespace(t, ns, adminKey)

	t.Run("a write at ALL reopens both replicas on every home node", func(t *testing.T) {
		for _, tg := range targets {
			writeAtAllEventually(t, tg, "", userKey)
		}
		requireEveryReplicaServes(t)
	})
}
