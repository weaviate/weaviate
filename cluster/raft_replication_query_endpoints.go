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
	"strings"

	"github.com/go-openapi/strfmt"
	"github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/replication"
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
)

func (s *Raft) GetReplicationDetailsByReplicationId(uuid strfmt.UUID) (api.ReplicationDetailsResponse, error) {
	request := &api.ReplicationDetailsRequest{
		Uuid: uuid,
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
	if err != nil {
		if strings.Contains(err.Error(), replication.ErrReplicationOperationNotFound.Error()) {
			return api.ReplicationDetailsResponse{}, replicationTypes.ErrReplicationOperationNotFound
		}
		return api.ReplicationDetailsResponse{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := api.ReplicationDetailsResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return api.ReplicationDetailsResponse{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}
