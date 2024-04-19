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

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"google.golang.org/protobuf/proto"
)

// Service abstracts away the Raft store, providing clients with an interface that encompasses all write operations.
// It ensures that these operations are executed on the current leader, regardless of the specific leader in the cluster.
type Service struct {
	store *Store
	cl    client
	log   *slog.Logger
}

// client to communicate with remote services
type client interface {
	Apply(leaderAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error)
	Query(ctx context.Context, leaderAddr string, req *cmd.QueryRequest) (*cmd.QueryResponse, error)
	Remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error)
	Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
}

func NewService(store *Store, client client) *Service {
	return &Service{store: store, cl: client, log: store.log}
}

// / RAFT-TODO Documentation
func (s *Service) Open(ctx context.Context, db Indexer) error {
	s.log.Info("starting raft sub-system ...")
	s.store.SetDB(db)
	return s.store.Open(ctx)
}

func (s *Service) Close(ctx context.Context) (err error) {
	s.log.Info("shutting down raft sub-system ...")

	// non-voter can be safely removed, as they don't partake in RAFT elections
	if !s.store.IsVoter() {
		s.log.Info("removing this node from cluster prior to shutdown ...")
		if err := s.Remove(ctx, s.store.ID()); err != nil {
			s.log.Error("remove this node from cluster: " + err.Error())
		} else {
			s.log.Info("successfully removed this node from the cluster.")
		}
	}
	return s.store.Close(ctx)
}

func (s *Service) Ready() bool {
	return s.store.Ready()
}

func (s *Service) SchemaReader() retrySchema {
	return s.store.SchemaReader()
}

func (s *Service) AddClass(cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name : %w", errBadRequest)
	}

	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) UpdateClass(cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name : %w", errBadRequest)
	}
	req := cmd.UpdateClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) DeleteClass(name string) (uint64, error) {
	command := &cmd.ApplyRequest{
		Type:  cmd.ApplyRequest_TYPE_DELETE_CLASS,
		Class: name,
	}
	return s.Execute(command)
}

func (s *Service) RestoreClass(cls *models.Class, ss *sharding.State) (uint64, error) {
	if cls == nil || cls.Class == "" {
		return 0, fmt.Errorf("nil class or empty class name : %w", errBadRequest)
	}
	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_RESTORE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) AddProperty(class string, props ...*models.Property) (uint64, error) {
	for _, p := range props {
		if p == nil || p.Name == "" || class == "" {
			return 0, fmt.Errorf("empty property or empty class name : %w", errBadRequest)
		}
	}
	req := cmd.AddPropertyRequest{Properties: props}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) UpdateShardStatus(class, shard, status string) (uint64, error) {
	if class == "" || shard == "" {
		return 0, fmt.Errorf("empty class or shard : %w", errBadRequest)
	}
	req := cmd.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) AddTenants(class string, req *cmd.AddTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request : %w", errBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) UpdateTenants(class string, req *cmd.UpdateTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request : %w", errBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) DeleteTenants(class string, req *cmd.DeleteTenantsRequest) (uint64, error) {
	if class == "" || req == nil {
		return 0, fmt.Errorf("empty class name or nil request : %w", errBadRequest)
	}
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) StoreSchemaV1() error {
	command := &cmd.ApplyRequest{
		Type: cmd.ApplyRequest_TYPE_STORE_SCHEMA_V1,
	}
	_, err := s.Execute(command)
	return err
}

func (s *Service) Execute(req *cmd.ApplyRequest) (uint64, error) {
	if s.store.IsLeader() {
		return s.store.Execute(req)
	}
	if cmd.ApplyRequest_Type_name[int32(req.Type.Number())] == "" {
		return 0, ErrUnknownCommand
	}

	leader := s.store.Leader()
	if leader == "" {
		return 0, ErrLeaderNotFound
	}
	resp, err := s.cl.Apply(leader, req)
	if err != nil {
		return 0, err
	}

	return resp.Version, err
}

func (s *Service) Join(ctx context.Context, id, addr string, voter bool) error {
	// log.Printf("membership.join %v %v %v", id, addr, voter)
	if s.store.IsLeader() {
		return s.store.Join(id, addr, voter)
	}
	leader := s.store.Leader()
	if leader == "" {
		return ErrLeaderNotFound
	}
	req := &cmd.JoinPeerRequest{Id: id, Address: addr, Voter: voter}
	_, err := s.cl.Join(ctx, leader, req)
	return err
}

func (s *Service) Remove(ctx context.Context, id string) error {
	// log.Printf("membership.remove %v ", id)
	if s.store.IsLeader() {
		return s.store.Remove(id)
	}
	leader := s.store.Leader()
	if leader == "" {
		return ErrLeaderNotFound
	}
	req := &cmd.RemovePeerRequest{Id: id}
	_, err := s.cl.Remove(ctx, leader, req)
	return err
}

func (s *Service) Stats() map[string]any {
	// log.Printf("membership.Stats")
	return s.store.Stats()
}

// LeaderWithID is used to return the current leader address and ID of the cluster.
// It may return empty strings if there is no current leader or the leader is unknown.
func (s *Service) LeaderWithID() (string, string) {
	addr, id := s.store.LeaderWithID()
	return string(addr), string(id)
}

func (s *Service) WaitUntilDBRestored(ctx context.Context, period time.Duration) error {
	return s.store.WaitToRestoreDB(ctx, period)
}

// QueryReadOnlyClass will verify that class is non empty and then build a Query that will be directed to the leader to
// ensure we will read the class with strong consistency
func (s *Service) QueryReadOnlyClass(class string) (*models.Class, uint64, error) {
	if class == "" {
		return &models.Class{}, 0, fmt.Errorf("empty class name: %w", errBadRequest)
	}

	// Build the query and execute it
	req := cmd.QueryReadOnlyClassRequest{Class: class}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return &models.Class{}, 0, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_CLASS,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return &models.Class{}, 0, fmt.Errorf("failed to execute query: %w", err)
	}

	// Empty payload doesn't unmarshal to an empty struct and will instead result in an error.
	// We have an empty payload when the requested class if not present in the schema.
	// In that case return a nil pointer and no error.
	if len(queryResp.Payload) == 0 {
		return nil, 0, nil
	}

	// Unmarshal the response
	resp := cmd.QueryReadOnlyClassResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return &models.Class{}, 0, fmt.Errorf("failed to unmarshal query result: %w", err)
	}
	return resp.Class, resp.ClassVersion, nil
}

// QuerySchema build a Query to read the schema that will be directed to the leader to ensure we will read the class
// with strong consistency
func (s *Service) QuerySchema() (models.Schema, error) {
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
func (s *Service) QueryTenants(class string) ([]*models.Tenant, uint64, error) {
	// Build the query and execute it
	req := cmd.QueryTenantsRequest{Class: class}
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
func (s *Service) QueryShardOwner(class, shard string) (string, uint64, error) {
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

// QueryShardOwner build a Query to read the tenant and activity status  of a given class and tenant pair.
// The request will be directed to the leader to ensure we  will read the tenant with strong consistency and return the
// shard owner node
func (s *Service) QueryTenantsShards(class string, tenants ...string) (map[string]string, error) {
	// Build the query and execute it
	req := cmd.QueryTenantsShardsRequest{Class: class, Tenants: tenants}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_TENANTS_SHARDS,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Unmarshal the response
	resp := cmd.QueryTenantsShardsResponse{}
	err = json.Unmarshal(queryResp.Payload, &resp)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return resp.Tenants, nil
}

// Query receives a QueryRequest and ensure it is executed on the leader and returns the related QueryResponse
// If any error happens it returns it
func (s *Service) Query(ctx context.Context, req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
	if s.store.IsLeader() {
		return s.store.Query(req)
	}

	leader := s.store.Leader()
	if leader == "" {
		return &cmd.QueryResponse{}, ErrLeaderNotFound
	}

	return s.cl.Query(ctx, leader, req)
}

func removeNilTenants(tenants []*cmd.Tenant) []*cmd.Tenant {
	n := 0
	for i := range tenants {
		if tenants[i] != nil && tenants[i].Name != "" {
			tenants[n] = tenants[i]
			n++
		}
	}
	return tenants[:n]
}
