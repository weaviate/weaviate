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
	context "context"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

type Manager interface {
	QueryShardingStateByCollection(ctx context.Context, collection string) (api.ShardingState, error)
	QueryShardingStateByCollectionAndShard(ctx context.Context, collection string, shard string) (api.ShardingState, error)

	ReplicationReplicateReplica(ctx context.Context, opId strfmt.UUID, sourceNode string, sourceCollection string, sourceShard string, targetNode string, transferType string) error

	// GetReplicationDetailsByReplicationId retrieves the details of a replication operation by its UUID.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	//
	// Returns:
	//   - api.ReplicationDetailsResponse: Contains the details of the requested replication operation.
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why retrieving the replication operation details failed.
	GetReplicationDetailsByReplicationId(ctx context.Context, uuid strfmt.UUID) (api.ReplicationDetailsResponse, error)

	// GetReplicationDetailsByCollection retrieves the details of all replication operations for a given collection.
	//
	// Parameters:
	//   - collection: The name of the collection to retrieve replication details for.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given collection. Returns an empty list if there are no replication operations for the given collection.
	//   - error: Returns an error if fetching the replication details failed.
	GetReplicationDetailsByCollection(ctx context.Context, collection string) ([]api.ReplicationDetailsResponse, error)

	// GetReplicationDetailsByCollectionAndShard retrieves the details of all replication operations for a given collection and shard.
	//
	// Parameters:
	//   - collection: The name of the collection to retrieve replication details for.
	//   - shard: The name of the shard to retrieve replication details for.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given collection and shard. Returns an empty list if there are no replication operations for the given collection and shard.
	//   - error: Returns an error if fetching the replication details failed.
	GetReplicationDetailsByCollectionAndShard(ctx context.Context, collection string, shard string) ([]api.ReplicationDetailsResponse, error)

	// GetReplicationDetailsByTargetNode retrieves the details of all replication operations for a given target node.
	//
	// Parameters:
	//   - node: The name of the target node to retrieve replication details for.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given target node. Returns an empty list if there are no replication operations for the given target node.
	//   - error: Returns an error if fetching the replication details failed.
	GetReplicationDetailsByTargetNode(ctx context.Context, node string) ([]api.ReplicationDetailsResponse, error)

	// GetAllReplicationDetails retrieves the details of all replication operations.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given target node. Returns an empty list if there are no replication operations for the given target node.
	//   - error: Returns an error if fetching the replication details failed.
	GetAllReplicationDetails(ctx context.Context) ([]api.ReplicationDetailsResponse, error)

	// CancelReplication cancels a replication operation meaning that the operation is stopped, cleaned-up on the target, and moved to the CANCELLED state.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	// Returns:
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why cancelling the replication operation failed.
	CancelReplication(ctx context.Context, uuid strfmt.UUID) error
	// DeleteReplication removes a replication operation from the FSM. If it's in progress, it will be cancelled first.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	// Returns:
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why cancelling the replication operation failed.
	DeleteReplication(ctx context.Context, uuid strfmt.UUID) error

	// DeleteReplicationsByCollection removes all replication operations for a specific collection.
	//
	// This is required when a collection is deleted, and all replication operations for that collection should be removed including in-flight operations that must be cancelled first.
	//
	// Parameters:
	//   - collection: The name of the collection for which to delete replication operations.
	// Returns:
	//   - error: Returns an error if the deletion of replication operations fails.
	DeleteReplicationsByCollection(ctx context.Context, collection string) error
	// DeleteReplicationsByTenants removes all replication operations for specified tenants in a specific collection.
	//
	// This is required when tenants are deleted, and all replication operations for those tenants should be removed including in-flight operations that must be cancelled first.
	//
	// Parameters:
	//   - collection: The name of the collection for which to delete replication operations.
	//   - tenants: The list of tenants for which to delete replication operations.
	// Returns:
	//   - error: Returns an error if the deletion of replication operations fails.
	DeleteReplicationsByTenants(ctx context.Context, collection string, tenants []string) error
	// DeleteAllReplications removes all replication operation from the FSM.
	// If they are in progress, then they are cancelled first.
	//
	// Returns:
	//   - error: any error explaining why cancelling the replication operation failed.
	DeleteAllReplications(ctx context.Context) error

	// ForceDeleteReplicationByReplicationId forcefully deletes a replication operation by its UUID.
	// This operation does not cancel the replication operation, it simply removes it from the FSM.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	//
	// Returns:
	//   - error: Returns an error if force deleting the replication operation failed.
	ForceDeleteReplicationByUuid(ctx context.Context, uuid strfmt.UUID) error
	// ForceDeleteReplicationByCollection forcefully deletes all replication operations for a given collection.
	// This operation does not cancel the replication operations, it simply removes it from the FSM.
	//
	// Parameters:
	//   - collection: The name of the collection to force delete replication operations for.
	//
	// Returns:
	//   - error: Returns an error if force deleting the replication operation failed.
	ForceDeleteReplicationsByCollection(ctx context.Context, collection string) error
	// ForceDeleteReplicationByCollectionAndShard forcefully deletes all replication operations for a given collection and shard.
	// This operation does not cancel the replication operations, it simply removes it from the FSM.
	//
	// Parameters:
	//   - collection: The name of the collection to force delete replication operations for.
	//   - shard: The name of the shard to force delete replication operations for.
	//
	// Returns:
	//   - error: Returns an error if force deleting the replication operation failed.
	ForceDeleteReplicationsByCollectionAndShard(ctx context.Context, collection string, shard string) error
	// ForceDeleteReplicationByTargetNode forcefully deletes all replication operations for a given target node.
	// This operation does not cancel the replication operations, it simply removes it from the FSM.
	//
	// Parameters:
	//   - node: The name of the target node to force delete replication operations for.
	//
	// Returns:
	//   - error: Returns an error if force deleting the replication operation failed.
	ForceDeleteReplicationsByTargetNode(ctx context.Context, node string) error
	// ForceDeleteAllReplication forcefully deletes all replication operations.
	// This operation does not cancel the replication operations, it simply removes it from the FSM.
	//
	// Returns:
	//   - error: Returns an error if force deleting the replication operation failed.
	ForceDeleteAllReplications(ctx context.Context) error
}
