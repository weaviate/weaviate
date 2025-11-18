//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2025 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package recovery

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

func TestNetworkIsolationSplitBrain(t *testing.T) {
	ctx := context.Background()

	compose, err := docker.New().
		With3NodeCluster().
		WithText2VecContextionary().
		WithWeaviateEnv("RAFT_TIMEOUTS_MULTIPLIER", "3").
		Start(ctx)
	require.NoError(t, err)

	defer func() {
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}()

	helper.SetupClient(compose.GetWeaviate().URI())

	verbose := "verbose"
	params := nodes.NewNodesGetParams().WithOutput(&verbose)
	t.Run("verify nodes are healthy", func(t *testing.T) {
		resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
		require.NoError(t, err)

		nodeStatusResp := resp.GetPayload()
		require.NotNil(t, nodeStatusResp)

		require.Len(t, nodeStatusResp.Nodes, 3)
	})

	t.Run("disconnect node 3 from the network", func(t *testing.T) {
		err = compose.DisconnectFromNetwork(ctx, docker.Weaviate3)
		require.NoError(t, err)
	})

	t.Run("verify 2 nodes are healthy", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			require.NoError(ct, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(ct, nodeStatusResp)

			assert.Len(ct, nodeStatusResp.Nodes, 2)
		}, 60*time.Second, 1*time.Second)
	})

	t.Run("reconnect node 3 to the network", func(t *testing.T) {
		err = compose.ConnectToNetwork(ctx, docker.Weaviate3)
		require.NoError(t, err)
	})

	t.Run("verify nodes are healthy and 3rd node successfully rejoined", func(t *testing.T) {
		assert.EventuallyWithT(t, func(ct *assert.CollectT) {
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			require.NoError(ct, err)

			nodeStatusResp := resp.GetPayload()
			require.NotNil(ct, nodeStatusResp)

			assert.Len(ct, nodeStatusResp.Nodes, 3)
		}, 120*time.Second, 1*time.Second)
	})
}
