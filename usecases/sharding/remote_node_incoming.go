//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

package sharding

import (
	"context"

	"github.com/semi-technologies/weaviate/entities/models"
)

type RemoteNodeIncomingRepo interface {
	IncomingGetNodeStatus(ctx context.Context) (*models.NodeStatus, error)
}

type RemoteNodeIncoming struct {
	repo RemoteNodeIncomingRepo
}

func NewRemoteNodeIncoming(repo RemoteNodeIncomingRepo) *RemoteNodeIncoming {
	return &RemoteNodeIncoming{
		repo: repo,
	}
}

func (rni *RemoteNodeIncoming) GetNodeStatus(ctx context.Context) (*models.NodeStatus, error) {
	return rni.repo.IncomingGetNodeStatus(ctx)
}
