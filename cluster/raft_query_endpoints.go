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
	"slices"

	"github.com/prometheus/client_golang/prometheus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/versioned"
	"github.com/weaviate/weaviate/usecases/monitoring"
	"github.com/weaviate/weaviate/usecases/sharding"
)

// QueryReadOnlyClass will verify that class is non empty and then build a Query that will be directed to the leader to
// ensure we will read the class with strong consistency
func (s *Raft) QueryReadOnlyClasses(classes ...string) (map[string]versioned.Class, error) {
	if len(classes) == 0 {
		return nil, fmt.Errorf("empty classes names: %w", schema.ErrBadRequest)
	}

	// remove dedup and empty
	slices.Sort(classes)
	classes = slices.Compact(classes)
	if len(classes) == 0 {
		return map[string]versioned.Class{}, fmt.Errorf("empty classes names: %w", schema.ErrBadRequest)
	}

	if len(classes) > 1 && classes[0] == "" {
		classes = classes[1:]
	}

	// Build the query and execute it
	req := cmd.QueryReadOnlyClassesRequest{Classes: classes}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return map[string]versioned.Class{}, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_CLASSES,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return map[string]versioned.Class{}, fmt.Errorf("failed to execute query: %w", err)
	}

	// Empty payload doesn't unmarshal to an empty struct and will instead result in an error.
	// We have an empty payload when the requested class if not present in the schema.
	// In that case return a nil pointer and no error.
	if len(queryResp.Payload) == 0 {
		return nil, nil
	}

	// Unmarshal the response
	resp := cmd.QueryReadOnlyClassResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return map[string]versioned.Class{}, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	return resp.Classes, nil
}

// QuerySchema build a Query to read the schema that will be directed to the leader to ensure we will read the class
// with strong consistency
func (s *Raft) QuerySchema() (models.Schema, error) {
	command := &cmd.QueryRequest{
		Type: cmd.QueryRequest_TYPE_GET_SCHEMA,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return models.Schema{}, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QuerySchemaResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return models.Schema{}, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	return resp.Schema, nil
}

// QueryTenants build a Query to read the tenants of a given class that will be directed to the leader to ensure we
// will read the class with strong consistency
func (s *Raft) QueryTenants(class string, tenants []string) ([]*models.Tenant, uint64, error) {
	// Build the query and execute it
	req := cmd.QueryTenantsRequest{Class: class, Tenants: tenants}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return []*models.Tenant{}, 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_TENANTS,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return []*models.Tenant{}, 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QueryTenantsResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return []*models.Tenant{}, 0, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return resp.Tenants, resp.ShardVersion, nil
}

// QueryShardOwner build a Query to read the tenants of a given class that will be directed to the leader to ensure we
// will read the tenant with strong consistency and return the shard owner node
func (s *Raft) QueryShardOwner(class, shard string) (string, uint64, error) {
	// Build the query and execute it
	req := cmd.QueryShardOwnerRequest{Class: class, Shard: shard}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return "", 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_SHARD_OWNER,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return "", 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QueryShardOwnerResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return "", 0, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return resp.Owner, resp.ShardVersion, nil
}

// QueryTenantsShards build a Query to read the tenants and their activity status of a given class.
// The request will be directed to the leader to ensure we  will read the tenant with strong consistency and return the
// shard owner node
func (s *Raft) QueryTenantsShards(class string, tenants ...string) (map[string]string, uint64, error) {
	// Build the query and execute it
	req := cmd.QueryTenantsShardsRequest{Class: class, Tenants: tenants}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_TENANTS_SHARDS,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QueryTenantsShardsResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return resp.TenantsActivityStatus, resp.SchemaVersion, nil
}

// QueryShardingState build a Query to read the sharding state of a given class.
// The request will be directed to the leader to ensure we  will read the shard state with strong consistency and return the
// state and it's version.
func (s *Raft) QueryShardingState(class string) (*sharding.State, uint64, error) {
	// Build the query and execute it
	req := cmd.QueryShardingStateRequest{Class: class}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_SHARDING_STATE,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QueryShardingStateResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return resp.State, resp.Version, nil
}

// Query receives a QueryRequest and ensure it is executed on the leader and returns the related QueryResponse
// If any error happens it returns it
func (s *Raft) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	t := prometheus.NewTimer(
		monitoring.GetMetrics().SchemaReadsLeader.WithLabelValues(
			req.Type.String(),
		))
	defer t.ObserveDuration()

	if s.store.IsLeader() {
		return s.store.Query(req)
	}

	leader := s.store.Leader()
	if leader == "" {
		return &cmd.QueryResponse{}, s.leaderErr()
	}

	return s.cl.Query(ctx, leader, req)
}
