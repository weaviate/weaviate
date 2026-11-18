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

	"github.com/go-openapi/strfmt"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	pb "github.com/weaviate/weaviate/grpc/generated/protocol/v1"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// TestObjectLimitReplicated writes to an RF=2 class through a node that holds
// no replica, so every write reaches both replicas over HTTP.
func TestObjectLimitReplicated(t *testing.T) {
	t.Parallel()
	replicas := []*docker.DockerContainer{sharedCompose.GetWeaviateNode2(), sharedCompose.GetWeaviateNode3()}
	homeNodes := []string{replicas[0].Name(), replicas[1].Name()}
	ns := uniqueNS()

	require.NotContains(t, homeNodes, sharedCompose.GetWeaviate().Name(),
		"test invariant: the client must enter on a node that holds no replica")

	helper.CreateNamespaceWithHomeNodes(t, ns, homeNodes, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u", ns)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u", adminKey) })

	helper.CreateClassAuth(t, &models.Class{
		Class:             "Items",
		ReplicationConfig: &models.ReplicationConfig{Factor: 2},
	}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Items", adminKey) })

	t.Run("writes under the cap land on both replicas", func(t *testing.T) {
		ids := make([]strfmt.UUID, 3)
		for i := range ids {
			id, err := insertItemVia(t, helper.Client(t), "Items", userKey, i, "")
			require.NoError(t, err)
			ids[i] = id
		}
		for _, replica := range replicas {
			c := helper.NewClient(t, replica.URI())
			helper.WaitForClassVia(t, c, "Items", userKey)
			for _, id := range ids {
				_, err := helper.GetObjectVia(t, c, "Items", id, types.ConsistencyLevelOne, userKey)
				require.NoError(t, err, "object %s missing on %s", id, replica.Name())
			}
		}
	})

	t.Run("writes at the cap are refused over REST and gRPC", func(t *testing.T) {
		fillUntilQuota(t, "Items", userKey)
		requireBatchAtQuota(t, sharedCompose.GetWeaviate().GrpcURI(), "Items", userKey, nil)
	})
}

// TestObjectLimitRF3AllHomeNodes fills an RF=3 class on all three nodes to
// the cap. It then writes at ONE through each of the other two nodes, where a
// write fails only once every replica refuses it, the receiving node's included.
func TestObjectLimitRF3AllHomeNodes(t *testing.T) {
	t.Parallel()
	nodes := []*docker.DockerContainer{sharedCompose.GetWeaviate(), sharedCompose.GetWeaviateNode2(), sharedCompose.GetWeaviateNode3()}
	ns := uniqueNS()

	helper.CreateNamespaceWithHomeNodes(t, ns, []string{nodes[0].Name(), nodes[1].Name(), nodes[2].Name()}, adminKey)
	t.Cleanup(func() { helper.DeleteNamespace(t, ns, adminKey) })

	userKey := createNamespacedUser(t, "u", ns)
	t.Cleanup(func() { helper.DeleteUser(t, ns+":u", adminKey) })

	helper.CreateClassAuth(t, &models.Class{
		Class:             "Items",
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
	}, userKey)
	t.Cleanup(func() { helper.DeleteClassAuth(t, ns+":Items", adminKey) })

	// The fill runs at ONE, so it ends only once all three replicas refuse. At
	// the default QUORUM it ends once two refuse, while the third's count, which
	// excludes its memtable, can still be under the cap.
	fillUntilQuotaAt(t, "Items", userKey, types.ConsistencyLevelOne)

	for _, node := range nodes[1:] {
		c := helper.NewClient(t, node.URI())
		helper.WaitForClassVia(t, c, "Items", userKey)
		_, err := insertItemVia(t, c, "Items", userKey, 0, types.ConsistencyLevelOne)
		requireObjectCapRefusal(t, err)
	}

	one := pb.ConsistencyLevel_CONSISTENCY_LEVEL_ONE
	requireBatchAtQuota(t, nodes[1].GrpcURI(), "Items", userKey, &one)
}
