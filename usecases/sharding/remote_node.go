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

package sharding

import (
	"context"
	"fmt"

	"github.com/weaviate/weaviate/entities/models"
)

type RemoteNodeClient interface {
	GetNodeStatus(ctx context.Context, hostName, className, output string) (*models.NodeStatus, error)
}

type RemoteNode struct {
	client       RemoteNodeClient
	nodeResolver nodeResolver
}

func NewRemoteNode(nodeResolver nodeResolver, client RemoteNodeClient) *RemoteNode {
	return &RemoteNode{
		client:       client,
		nodeResolver: nodeResolver,
	}
}

func (rn *RemoteNode) GetNodeStatus(ctx context.Context, nodeName, className, output string) (*models.NodeStatus, error) {
	host, ok := rn.nodeResolver.NodeHostname(nodeName)
	if !ok {
		return nil, fmt.Errorf("resolve node name %q to host", nodeName)
	}
	return rn.client.GetNodeStatus(ctx, host, className, output)
}
