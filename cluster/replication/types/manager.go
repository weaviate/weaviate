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

import "github.com/weaviate/weaviate/cluster/proto/api"

type Manager interface {
	ReplicationReplicateReplica(sourceNode string, sourceCollection string, sourceShard string, targetNode string) error
	ReplicationDisableReplica(node string, collection string, shard string) error
	ReplicationDeleteReplica(node string, collection string, shard string) error

	// GetReplicationDetailsByReplicationId retrieves the details of a replication operation by its ID.
	//
	// Parameters:
	//   - id: The unique identifier for the replication operation (uint64).
	//
	// Returns:
	//   - api.ReplicationDetailsResponse: Contains the details of the requested replication operation.
	//   - error: Returns ErrReplicationOperationNotFound if the operation doesn't exist,
	//     or another error explaining why retrieving the replication operation details failed.
	GetReplicationDetailsByReplicationId(id uint64) (api.ReplicationDetailsResponse, error)
}
