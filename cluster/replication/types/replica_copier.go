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
	// CopyReplicaFiles see cluster/replication/copier.Copier.CopyReplicaFiles
	CopyReplicaFiles(ctx context.Context, sourceNode string, sourceCollection string, sourceShard string, schemaVersion uint64) error

	// LoadLocalShard see cluster/replication/copier.Copier.LoadLocalShard
	LoadLocalShard(ctx context.Context, collectionName, shardName string) error

	// InitAsyncReplicationLocally see cluster/replication/copier.Copier.InitAsyncReplicationLocally
	InitAsyncReplicationLocally(ctx context.Context, collectionName, shardName string) error

	// RevertAsyncReplicationLocally see cluster/replication/copier.Copier.RevertAsyncReplicationLocally
	RevertAsyncReplicationLocally(ctx context.Context, collectionName, shardName string) error

	// AddAsyncReplicationTargetNode see cluster/replication/copier.Copier.AddAsyncReplicationTargetNode
	AddAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error

	// RemoveAsyncReplicationTargetNode see cluster/replication/copier.Copier.RemoveAsyncReplicationTargetNode
	RemoveAsyncReplicationTargetNode(ctx context.Context, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error

	// AsyncReplicationStats see cluster/replication/copier.Copier.AsyncReplicationStatus
	AsyncReplicationStatus(ctx context.Context, srcNodeId, targetNodeId, collectionName, shardName string) (models.AsyncReplicationStatus, error)
}
