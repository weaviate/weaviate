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

package types

import (
	"context"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
)

// ReplicaCopier see cluster/replication/copier.Copier
type ReplicaCopier interface {
	// CopyReplicaFiles see cluster/replication/copier.Copier.CopyReplicaFiles
	CopyReplicaFiles(ctx context.Context, opID strfmt.UUID, sourceNode string, sourceCollection string, sourceShard string, schemaVersion uint64) error

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

	// StartChangeCapture see cluster/replication/copier.Copier.StartChangeCapture
	StartChangeCapture(ctx context.Context, srcNodeId, indexName, shardName, opID string) error

	// TailAndApply see cluster/replication/copier.Copier.TailAndApply
	TailAndApply(ctx context.Context, srcNodeId, indexName, shardName, opID string, untilLSN uint64) (lastAppliedLSN uint64, err error)

	// SnapshotChangeLogLSN see cluster/replication/copier.Copier.SnapshotChangeLogLSN
	SnapshotChangeLogLSN(ctx context.Context, srcNodeId, indexName, shardName, opID string) (uint64, error)

	// FinalizeChangeLog see cluster/replication/copier.Copier.FinalizeChangeLog
	FinalizeChangeLog(ctx context.Context, srcNodeId, indexName, shardName, opID string) (uint64, error)

	// StopChangeCapture see cluster/replication/copier.Copier.StopChangeCapture
	StopChangeCapture(ctx context.Context, srcNodeId, indexName, shardName, opID string) error

	// ReleaseReplicaSnapshot is the cancellation-path cleanup for snapshots
	// that escaped the CopyReplicaFiles release defer. Idempotent.
	ReleaseReplicaSnapshot(ctx context.Context, srcNodeId, indexName, opID string) error
}
