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

package types

import (
	"context"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// ReplicaCopier see cluster/replication/copier.Copier
type ReplicaCopier interface {
	// CopyReplica see cluster/replication/copier.Copier.CopyReplica
	CopyReplica(ctx context.Context, sourceNode string, sourceCollection string, sourceShard string) error

	// CopyReplica see cluster/replication/copier.Copier.RemoveLocalReplica
	RemoveLocalReplica(ctx context.Context, sourceCollection string, sourceShard string) error

	// InitAsyncReplicationLocally see cluster/replication/copier.Copier.InitAsyncReplicationLocally
	InitAsyncReplicationLocally(ctx context.Context, collectionName, shardName string) error

	// SetAsyncReplicationTargetNode see cluster/replication/copier.Copier.SetAsyncReplicationTargetNode
	SetAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error

	// AsyncReplicationStats see cluster/replication/copier.Copier.AsyncReplicationStatus
	AsyncReplicationStatus(ctx context.Context, srcNodeId, targetNodeId, collectionName, shardName string) (models.AsyncReplicationStatus, error)
}
