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

	"github.com/weaviate/weaviate/entities/models"
)

type RemoteNodeIncomingRepo interface {
	IncomingGetNodeStatus(ctx context.Context, className, shardName, output string) (*models.NodeStatus, error)
	IncomingGetNodeStatistics() (*models.Statistics, error)
}

type RemoteNodeIncoming struct {
	repo RemoteNodeIncomingRepo
}

func NewRemoteNodeIncoming(repo RemoteNodeIncomingRepo) *RemoteNodeIncoming {
	return &RemoteNodeIncoming{
		repo: repo,
	}
}

func (rni *RemoteNodeIncoming) GetNodeStatus(ctx context.Context, className, shardName, output string) (*models.NodeStatus, error) {
	return rni.repo.IncomingGetNodeStatus(ctx, className, shardName, output)
}

func (rni *RemoteNodeIncoming) GetStatistics(ctx context.Context) (*models.Statistics, error) {
	return rni.repo.IncomingGetNodeStatistics()
}
