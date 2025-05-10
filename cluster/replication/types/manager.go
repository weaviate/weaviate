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
	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/proto/api"
)

type Manager interface {
	QueryShardingStateByCollection(collection string) (api.ShardingState, error)
	QueryShardingStateByCollectionAndShard(collection string, shard string) (api.ShardingState, error)

	ReplicationReplicateReplica(opId strfmt.UUID, sourceNode string, sourceCollection string, sourceShard string, targetNode string, transferType string) error
	ReplicationDisableReplica(node string, collection string, shard string) error
	ReplicationDeleteReplica(node string, collection string, shard string) error

	// GetReplicationDetailsByReplicationId retrieves the details of a replication operation by its UUID.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	//
	// Returns:
	//   - api.ReplicationDetailsResponse: Contains the details of the requested replication operation.
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why retrieving the replication operation details failed.
	GetReplicationDetailsByReplicationId(uuid strfmt.UUID) (api.ReplicationDetailsResponse, error)

	// GetReplicationDetailsByCollection retrieves the details of all replication operations for a given collection.
	//
	// Parameters:
	//   - collection: The name of the collection to retrieve replication details for.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given collection. Returns an empty list if there are no replication operations for the given collection.
	//   - error: Returns an error if fetching the replication details failed.
	GetReplicationDetailsByCollection(collection string) ([]api.ReplicationDetailsResponse, error)

	// GetReplicationDetailsByCollectionAndShard retrieves the details of all replication operations for a given collection and shard.
	//
	// Parameters:
	//   - collection: The name of the collection to retrieve replication details for.
	//   - shard: The name of the shard to retrieve replication details for.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given collection and shard. Returns an empty list if there are no replication operations for the given collection and shard.
	//   - error: Returns an error if fetching the replication details failed.
	GetReplicationDetailsByCollectionAndShard(collection string, shard string) ([]api.ReplicationDetailsResponse, error)

	// GetReplicationDetailsByTargetNode retrieves the details of all replication operations for a given target node.
	//
	// Parameters:
	//   - node: The name of the target node to retrieve replication details for.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given target node. Returns an empty list if there are no replication operations for the given target node.
	//   - error: Returns an error if fetching the replication details failed.
	GetReplicationDetailsByTargetNode(node string) ([]api.ReplicationDetailsResponse, error)

	// GetAllReplicationDetails retrieves the details of all replication operations.
	//
	// Returns:
	//   - []api.ReplicationDetailsResponse: A list of replication details for the given target node. Returns an empty list if there are no replication operations for the given target node.
	//   - error: Returns an error if fetching the replication details failed.
	GetAllReplicationDetails() ([]api.ReplicationDetailsResponse, error)

	// CancelReplication cancels a replication operation meaning that the operation is stopped, cleaned-up on the target, and moved to the CANCELLED state.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	// Returns:
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why cancelling the replication operation failed.
	CancelReplication(uuid strfmt.UUID) error
	// DeleteReplication removes a replication operation from the FSM. If it's in progress, it will be cancelled first.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	// Returns:
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why cancelling the replication operation failed.
	DeleteReplication(uuid strfmt.UUID) error
	// DeleteReplication removes all replication operation from the FSM. If they are in progress, then they are cancelled first.
	//
	// Returns:
	//   - error: any error explaining why cancelling the replication operation failed.
	DeleteAllReplications() error
}
