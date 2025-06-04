//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	compose, err := docker.New().
		With3NodeCluster().
		WithText2VecContextionary().
		Start(ctx)
	require.Nil(t, err)

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
		require.Nil(t, err)

		nodeStatusResp := resp.GetPayload()
		require.NotNil(t, nodeStatusResp)

		nodes := nodeStatusResp.Nodes
		require.NotNil(t, nodes)
		require.Len(t, nodes, 3)
	})

	t.Run("disconnect node 3 from the network", func(t *testing.T) {
		err = compose.DisconnectFromNetwork(ctx, 3)
		require.Nil(t, err)
		// this sleep to make sure network is disconnected
		time.Sleep(3 * time.Second)
	})

	t.Run("verify 2 nodes are healthy", func(t *testing.T) {
		assert.Eventually(t, func() bool {
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			assert.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			assert.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			assert.NotNil(t, nodes)
			return len(nodes) == 2
		}, 30*time.Second, 500*time.Millisecond)
	})

	t.Run("reconnect node 3 to the network", func(t *testing.T) {
		err = compose.ConnectToNetwork(ctx, 3)
		require.Nil(t, err)
		// this sleep to make sure network is connected
		time.Sleep(3 * time.Second)
	})

	t.Run("verify nodes are healthy and 3rd node successfully rejoined", func(t *testing.T) {
		assert.Eventually(t, func() bool {
			resp, err := helper.Client(t).Nodes.NodesGet(params, nil)
			assert.Nil(t, err)

			nodeStatusResp := resp.GetPayload()
			assert.NotNil(t, nodeStatusResp)

			nodes := nodeStatusResp.Nodes
			assert.NotNil(t, nodes)
			return len(nodes) == 3
		}, 90*time.Second, 500*time.Millisecond)
	})
}
