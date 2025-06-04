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

package types

import (
	"context"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// DbWrapper is a type that hides a db.DB, this is used to avoid a circular
// dependency between the copier and the db package.
type DbWrapper interface {
	// GetIndex See adapters/repos/db.Index.GetIndex
	GetIndex(name schema.ClassName) *db.Index

	// GetOneNodeStatus See adapters/repos/db.DB.GetOneNodeStatus
	GetOneNodeStatus(ctx context.Context, nodeName string, className, shardName, output string) (*models.NodeStatus, error)
}

// ShardLoader is a type that can load a shard from disk files, this is used to avoid a circular
// dependency between the copier and the db package.
type ShardLoader interface {
	// LoadLocalShard See adapters/repos/db.Index.LoadLocalShard
	LoadLocalShard(ctx context.Context, name string) error
}

// RemoteIndex is a type that can interact with a remote index, this is used to avoid a circular
// dependency between the copier and the db package.
type RemoteIndex interface {
	// AddAsyncReplicationTargetNode See adapters/clients.RemoteIndex.AddAsyncReplicationTargetNode
	AddAsyncReplicationTargetNode(ctx context.Context,
		hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error
	// RemoveAsyncReplicationTargetNode See adapters/clients.RemoteIndex.RemoveAsyncReplicationTargetNode
	RemoveAsyncReplicationTargetNode(ctx context.Context,
		hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error
}
