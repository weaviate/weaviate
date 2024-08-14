package helper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/client/nodes"
	"github.com/weaviate/weaviate/entities/models"
)

func GetNodes(t *testing.T) []*models.NodeStatus {
	output := "verbose"
	params := nodes.NewNodesGetParams().WithOutput(&output)
	resp, err := Client(t).Nodes.NodesGet(params, nil)
	AssertRequestOk(t, resp, err, nil)
	require.NotNil(t, resp.Payload)
	require.NotNil(t, resp.Payload.Nodes)
	return resp.Payload.Nodes
}

func WaitForAsyncIndexing(t *testing.T, ctx context.Context) bool {
	nodes := GetNodes(t)
	select {
	case <-ctx.Done():
		return false
	default:
		isReady := true
		for _, node := range nodes {
			for _, shard := range node.Shards {
				fmt.Println("Node: ", node.Name, "Shard: ", shard.Name, "Status: ", shard.VectorIndexingStatus)
				if shard.VectorIndexingStatus != "READY" || shard.VectorQueueLength > 0 {
					isReady = false
					break
				}
			}
		}
		if !isReady {
			time.Sleep(1 * time.Second)
			return WaitForAsyncIndexing(t, ctx)
		}
		return true
	}
}
