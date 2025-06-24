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
	"io"

	"github.com/weaviate/weaviate/adapters/repos/db"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/file"
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
	// PauseFileActivity See adapters/clients.RemoteIndex.PauseFileActivity
	PauseFileActivity(ctx context.Context, hostName, indexName, shardName string, schemaVersion uint64) error
	// ResumeFileActivity See adapters/clients.RemoteIndex.ResumeFileActivity
	ResumeFileActivity(ctx context.Context, hostName, indexName, shardName string) error
	// ListFiles See adapters/clients.RemoteIndex.ListFiles
	ListFiles(ctx context.Context,
		hostName, indexName, shardName string) ([]string, error)
	// GetFileMetadata See adapters/clients.RemoteIndex.GetFileMetadata
	GetFileMetadata(ctx context.Context,
		hostName, indexName, shardName, fileName string) (file.FileMetadata, error)
	// GetFile See adapters/clients.RemoteIndex.GetFile
	GetFile(ctx context.Context,
		hostName, indexName, shardName, fileName string) (io.ReadCloser, error)
	// AddAsyncReplicationTargetNode See adapters/clients.RemoteIndex.AddAsyncReplicationTargetNode
	AddAsyncReplicationTargetNode(ctx context.Context,
		hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride, schemaVersion uint64) error
	// RemoveAsyncReplicationTargetNode See adapters/clients.RemoteIndex.RemoveAsyncReplicationTargetNode
	RemoveAsyncReplicationTargetNode(ctx context.Context,
		hostName, indexName, shardName string, targetNodeOverride additional.AsyncReplicationTargetNodeOverride) error
}
