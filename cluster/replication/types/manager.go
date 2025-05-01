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
	ReplicationReplicateReplica(opId strfmt.UUID, sourceNode string, sourceCollection string, sourceShard string, targetNode string) error
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
	// CancelReplication cancels a replication operation meaning that the operation is stopped and removed from the FSM.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	// Returns:
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why cancelling the replication operation failed.
	CancelReplication(uuid strfmt.UUID) error
	// StopReplication stops a replication operation meaning that the operation is stopped and its state moved to STOPPED.
	//
	// Parameters:
	//   - uuid: The unique identifier for the replication operation (strfmt.UUID).
	// Returns:
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why cancelling the replication operation failed.
	StopReplication(uuid strfmt.UUID) error
}
