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

	// DeleteReplicationsByCollection removes all replication operations for a specific collection.
	//
	// This is required when a collection is deleted, and all replication operations for that collection should be removed including in-flight operations that must be cancelled first.
	//
	// Parameters:
	//   - collection: The name of the collection for which to delete replication operations.
	// Returns:
	//   - error: Returns an error if the deletion of replication operations fails.
	DeleteReplicationsByCollection(collection string) error
	// DeleteReplicationsByTenants removes all replication operations for specified tenants in a specific collection.
	//
	// This is required when tenants are deleted, and all replication operations for those tenants should be removed including in-flight operations that must be cancelled first.
	//
	// Parameters:
	//   - collection: The name of the collection for which to delete replication operations.
	//   - tenants: The list of tenants for which to delete replication operations.
	// Returns:
	//   - error: Returns an error if the deletion of replication operations fails.
	DeleteReplicationsByTenants(collection string, tenants []string) error
}
