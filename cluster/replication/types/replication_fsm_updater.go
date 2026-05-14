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
	// Returns the apply's RAFT index so the caller can fan out a
	// WaitForUpdateAllNodes on it.
	ReplicationUpdateReplicaOpStatus(ctx context.Context, id uint64, state api.ShardReplicationState) (uint64, error)
	ReplicationRegisterError(ctx context.Context, id uint64, errorToRegister string) error
	ReplicationRemoveReplicaOp(ctx context.Context, id uint64) error
	ReplicationCancellationComplete(ctx context.Context, id uint64) error
	ReplicationGetReplicaOpStatus(ctx context.Context, id uint64) (api.ShardReplicationState, error)
	ReplicationStoreSchemaVersion(ctx context.Context, id uint64, schemaVersion uint64) error
	UpdateTenants(ctx context.Context, class string, req *api.UpdateTenantsRequest) (uint64, error)
	WaitForUpdate(ctx context.Context, schemaVersion uint64) error
	// Blocks until every peer in the RAFT configuration has applied at
	// least version. Convergence checkpoint before irreversible work.
	WaitForUpdateAllNodes(ctx context.Context, version uint64) error
}
