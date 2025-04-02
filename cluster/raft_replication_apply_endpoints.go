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

func (s *Raft) ReplicationReplicateReplica(sourceNode string, sourceCollection string, sourceShard string, targetNode string) error {
	req := &api.ReplicationReplicateShardRequest{
		Version:          api.ReplicationCommandVersionV0,
		SourceNode:       sourceNode,
		SourceCollection: sourceCollection,
		SourceShard:      sourceShard,
		TargetNode:       targetNode,
	}

	if err := replication.ValidateReplicationReplicateShard(s.SchemaReader(), req); err != nil {
		return fmt.Errorf("%w: %w", replicationTypes.ErrInvalidRequest, err)
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) ReplicationDisableReplica(node string, collection string, shard string) error {
	return fmt.Errorf("not implemented")
}

func (s *Raft) ReplicationDeleteReplica(node string, collection string, shard string) error {
	return fmt.Errorf("not implemented")
}

func (s *Raft) ReplicationUpdateReplicaOpStatus(id uint64, state api.ShardReplicationState) error {
	req := &api.ReplicationUpdateOpStateRequest{
		Version: api.ReplicationCommandVersionV0,
		Id:      id,
		State:   state,
	}

	subCommand, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &api.ApplyRequest{
		Type:       api.ApplyRequest_TYPE_REPLICATION_REPLICATE_UPDATE_STATE,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

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
