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

	"github.com/weaviate/weaviate/cluster/proto/api"
)

type FSMUpdater interface {
	ReplicationAddReplicaToShard(ctx context.Context, collection string, shard string, nodeId string, opId uint64) (uint64, error)
	DeleteReplicaFromShard(ctx context.Context, collection string, shard string, nodeId string) (uint64, error)
	SyncShard(ctx context.Context, collection string, shard string, nodeId string) (uint64, error)
	// Returns the apply's RAFT index for a subsequent WaitForUpdateAllNodes.
	ReplicationUpdateReplicaOpStatus(ctx context.Context, id uint64, state api.ShardReplicationState) (uint64, error)
	ReplicationRegisterError(ctx context.Context, id uint64, errorToRegister string) error
	ReplicationRemoveReplicaOp(ctx context.Context, id uint64) error
	ReplicationCancellationComplete(ctx context.Context, id uint64) error
	ReplicationGetReplicaOpStatus(ctx context.Context, id uint64) (api.ShardReplicationState, error)
	ReplicationStoreSchemaVersion(ctx context.Context, id uint64, schemaVersion uint64) error
	UpdateTenants(ctx context.Context, class string, req *api.UpdateTenantsRequest) (uint64, error)
	WaitForUpdate(ctx context.Context, schemaVersion uint64) error
	// WaitForUpdateAllNodes blocks until every RAFT peer has applied version.
	WaitForUpdateAllNodes(ctx context.Context, version uint64) error
}
