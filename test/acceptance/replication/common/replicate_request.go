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

package common

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/verbosity"
	"github.com/weaviate/weaviate/test/helper"
)

// GetRequest builds a replicate-replica request for className, sourced from
// the first node/shard and targeting the second node, waiting for the class
// to be initialized and propagated across the cluster first.
func GetRequest(t *testing.T, className string) *models.ReplicationReplicateReplicaRequest {
	verbose := verbosity.OutputVerbose
	var nodesResp *nodes.NodesGetClassOK
	var err error

	// Wait for the class to be fully initialized and propagated across the cluster
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		nodesResp, err = helper.Client(t).Nodes.NodesGetClass(nodes.NewNodesGetClassParams().WithOutput(&verbose).WithClassName(className), nil)
		assert.Nil(ct, err, "NodesGetClass should succeed")
		if err == nil {
			assert.NotNil(ct, nodesResp, "nodes response should not be nil")
			if nodesResp != nil && nodesResp.Payload != nil && len(nodesResp.Payload.Nodes) >= 2 {
				assert.GreaterOrEqual(ct, len(nodesResp.Payload.Nodes[0].Shards), 1, "first node should have at least one shard")
			}
		}
	}, 30*time.Second, 100*time.Millisecond, "class %s should be initialized and available on nodes", className)

	require.NoError(t, err)
	require.NotNil(t, nodesResp)
	require.NotNil(t, nodesResp.Payload)
	require.GreaterOrEqual(t, len(nodesResp.Payload.Nodes), 2, "should have at least 2 nodes")
	require.GreaterOrEqual(t, len(nodesResp.Payload.Nodes[0].Shards), 1, "first node should have at least one shard")

	return &models.ReplicationReplicateReplicaRequest{
		Collection: &className,
		SourceNode: &nodesResp.Payload.Nodes[0].Name,
		TargetNode: &nodesResp.Payload.Nodes[1].Name,
		Shard:      &nodesResp.Payload.Nodes[0].Shards[0].Name,
	}
}
