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

package replica_reachability

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/client/objects"
	"github.com/weaviate/weaviate/cluster/router/types"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const className = "ReplicaReachability"

func TestReplicaReachability(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		With3NodeCluster().
		// The default LeaderOnly strategy fetches the class from the RAFT leader.
		// With two of three voters stopped there is no leader, and the write
		// fails on that lookup before it reaches the router.
		WithWeaviateEnv("COLLECTION_RETRIEVAL_STRATEGY", "LocalOnly").
		Start(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, compose.Terminate(ctx)) }()

	// weaviate-0 stays up throughout; every request below goes to it.
	defer helper.ResetClient()
	helper.SetupClient(compose.GetWeaviate().URI())

	helper.CreateClass(t, &models.Class{
		Class:             className,
		Vectorizer:        "none",
		ReplicationConfig: &models.ReplicationConfig{Factor: 3},
		Properties: []*models.Property{
			{Name: "title", DataType: schema.DataTypeText.PropString()},
		},
	})

	t.Run("quorum write succeeds with one of three replicas down", func(t *testing.T) {
		require.NoError(t, compose.StopNode(ctx, 2, nil))
		awaitMembers(t, docker.Weaviate0, docker.Weaviate1)

		require.NoError(t, createObjectQuorum(t))
	})

	t.Run("quorum write is rejected with two of three replicas down", func(t *testing.T) {
		require.NoError(t, compose.StopNode(ctx, 1, nil))
		awaitMembers(t, docker.Weaviate0)

		err := createObjectQuorum(t)
		var serverErr *objects.ObjectsCreateInternalServerError
		require.ErrorAs(t, err, &serverErr)
		require.NotNil(t, serverErr.Payload)
		require.NotEmpty(t, serverErr.Payload.Error)
		msg := serverErr.Payload.Error[0].Message
		require.Contains(t, msg, "cannot reach enough replicas")
		// Only the router's reachability check puts the reachable count in the
		// message. The write coordinator returns the same sentinel error without it.
		require.Contains(t, msg, "1 of 3 replicas reachable")
	})
}

// awaitMembers waits until /v1/nodes on the client's node lists exactly the
// nodes in want. That endpoint reports the node's memberlist members. The
// router resolves replica hostnames from the same set.
func awaitMembers(t *testing.T, want ...string) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Nodes.NodesGet(nodes.NewNodesGetParams().WithTimeout(10*time.Second), nil)
		require.NoError(c, err)
		names := make([]string, 0, len(resp.Payload.Nodes))
		for _, n := range resp.Payload.Nodes {
			names = append(names, n.Name)
		}
		assert.ElementsMatch(c, want, names)
	}, 2*time.Minute, 500*time.Millisecond)
}

func createObjectQuorum(t *testing.T) error {
	t.Helper()
	cl := string(types.ConsistencyLevelQuorum)
	params := objects.NewObjectsCreateParams().
		WithBody(&models.Object{Class: className, Properties: map[string]any{"title": "reachability"}}).
		WithConsistencyLevel(&cl)
	_, err := helper.Client(t).Objects.ObjectsCreate(params, nil)
	return err
}
