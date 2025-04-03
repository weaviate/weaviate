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
