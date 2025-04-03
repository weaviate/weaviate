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

package cluster

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

// GetReplicationDetailsByReplicationId retrieves the details of a replication operation by its ID.
//
// This method creates a request to fetch replication details, marshals it into a query command,
// executes the query against the Raft cluster, and un-marshals the response.
//
// Parameters:
//   - id: The unique identifier for the replication operation (uint64).
//
// Returns:
//   - api.ReplicationDetailsResponse: Contains the details of the requested replication operation.
//   - error: Returns ErrReplicationOperationNotFound if the replication with the specified ID
//     does not exist, or a wrapped error explaining the specific failure that occurred.
func (s *Raft) GetReplicationDetailsByReplicationId(id uint64) (api.ReplicationDetailsResponse, error) {
	request := &api.ReplicationDetailsRequest{
		Id: id,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return api.ReplicationDetailsResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(context.Background(), command)
	if errors.Is(err, replication.ErrReplicationOperationNotFound) {
		return api.ReplicationDetailsResponse{}, replicationTypes.ErrReplicationOperationNotFound
	} else if err != nil {
		return api.ReplicationDetailsResponse{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := api.ReplicationDetailsResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return api.ReplicationDetailsResponse{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}
