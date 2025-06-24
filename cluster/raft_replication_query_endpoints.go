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
	replicationTypes "github.com/weaviate/weaviate/cluster/replication/types"
	"github.com/weaviate/weaviate/cluster/types"
)

func (s *Raft) GetReplicationDetailsByReplicationId(ctx context.Context, uuid strfmt.UUID) (api.ReplicationDetailsResponse, error) {
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

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return api.ReplicationDetailsResponse{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrReplicationOperationNotFound)
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

func (s *Raft) GetReplicationDetailsByCollection(ctx context.Context, collection string) ([]api.ReplicationDetailsResponse, error) {
	request := &api.ReplicationDetailsRequestByCollection{
		Collection: collection,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS_BY_COLLECTION,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return []api.ReplicationDetailsResponse{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrReplicationOperationNotFound)
		}
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := []api.ReplicationDetailsResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}

func (s *Raft) GetReplicationDetailsByCollectionAndShard(ctx context.Context, collection string, shard string) ([]api.ReplicationDetailsResponse, error) {
	request := &api.ReplicationDetailsRequestByCollectionAndShard{
		Collection: collection,
		Shard:      shard,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS_BY_COLLECTION_AND_SHARD,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return []api.ReplicationDetailsResponse{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrReplicationOperationNotFound)
		}
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := []api.ReplicationDetailsResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}

func (s *Raft) GetReplicationDetailsByTargetNode(ctx context.Context, node string) ([]api.ReplicationDetailsResponse, error) {
	request := &api.ReplicationDetailsRequestByTargetNode{
		Node: node,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_REPLICATION_DETAILS_BY_TARGET_NODE,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return []api.ReplicationDetailsResponse{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrReplicationOperationNotFound)
		}
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := []api.ReplicationDetailsResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}

func (s *Raft) GetAllReplicationDetails(ctx context.Context) ([]api.ReplicationDetailsResponse, error) {
	command := &api.QueryRequest{
		Type: api.QueryRequest_TYPE_GET_ALL_REPLICATION_DETAILS,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return []api.ReplicationDetailsResponse{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrReplicationOperationNotFound)
		}
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := []api.ReplicationDetailsResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return []api.ReplicationDetailsResponse{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}

func (s *Raft) QueryShardingStateByCollection(ctx context.Context, collection string) (api.ShardingState, error) {
	request := &api.ReplicationQueryShardingStateByCollectionRequest{
		Collection: collection,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return api.ShardingState{}, fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_SHARDING_STATE_BY_COLLECTION,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrNotFound.Error()) {
			return api.ShardingState{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrNotFound)
		}
		return api.ShardingState{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := api.ShardingState{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return api.ShardingState{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}

func (s *Raft) QueryShardingStateByCollectionAndShard(ctx context.Context, collection string, shard string) (api.ShardingState, error) {
	request := &api.ReplicationQueryShardingStateByCollectionAndShardRequest{
		Collection: collection,
		Shard:      shard,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return api.ShardingState{}, fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_SHARDING_STATE_BY_COLLECTION_AND_SHARD,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrNotFound.Error()) {
			return api.ShardingState{}, fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrNotFound)
		}
		return api.ShardingState{}, fmt.Errorf("failed to execute query: %w", err)
	}

	response := api.ShardingState{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return api.ShardingState{}, fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response, nil
}

func (s *Raft) ReplicationGetReplicaOpStatus(ctx context.Context, id uint64) (api.ShardReplicationState, error) {
	request := &api.ReplicationOperationStateRequest{
		Id: id,
	}

	subCommand, err := json.Marshal(request)
	if err != nil {
		return "", fmt.Errorf("marshal request: %w", err)
	}

	command := &api.QueryRequest{
		Type:       api.QueryRequest_TYPE_GET_REPLICATION_OPERATION_STATE,
		SubCommand: subCommand,
	}

	queryResponse, err := s.Query(ctx, command)
	if err != nil {
		if strings.Contains(err.Error(), replicationTypes.ErrReplicationOperationNotFound.Error()) {
			return "", fmt.Errorf("%w: %w", types.ErrNotFound, replicationTypes.ErrReplicationOperationNotFound)
		}
		return "", fmt.Errorf("failed to execute query: %w", err)
	}

	response := api.ReplicationOperationStateResponse{}
	err = json.Unmarshal(queryResponse.Payload, &response)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal query response: %w", err)
	}

	return response.State, nil
}
