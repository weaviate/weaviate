//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	cmd "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"google.golang.org/protobuf/proto"
)

// Service abstracts away the Raft store, providing clients with an interface that encompasses all write operations.
// It ensures that these operations are executed on the current leader, regardless of the specific leader in the cluster.
type Service struct {
	store *Store
	rpc   client
}

// client to communicate with remote services
type client interface {
	Apply(leaderAddr string, req *cmd.ApplyRequest) (*cmd.ApplyResponse, error)
	Remove(ctx context.Context, leaderAddress string, req *cmd.RemovePeerRequest) (*cmd.RemovePeerResponse, error)
	Join(ctx context.Context, leaderAddr string, req *cmd.JoinPeerRequest) (*cmd.JoinPeerResponse, error)
}

func NewService(store *Store, client client) *Service {
	return &Service{store: store, rpc: client}
}

/// RAFT-TODO Documentation

func (s *Service) AddClass(cls *models.Class, ss *sharding.State) error {
	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) UpdateClass(cls *models.Class, ss *sharding.State) error {
	req := cmd.UpdateClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) DeleteClass(name string) error {
	command := &cmd.ApplyRequest{
		Type:  cmd.ApplyRequest_TYPE_DELETE_CLASS,
		Class: name,
	}
	return s.Execute(command)
}

func (s *Service) RestoreClass(cls *models.Class, ss *sharding.State) error {
	req := cmd.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_RESTORE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) AddProperty(class string, p *models.Property) error {
	req := cmd.AddPropertyRequest{Property: p}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) UpdateShardStatus(class, shard, status string) error {
	req := cmd.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) AddTenants(class string, req *cmd.AddTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) UpdateTenants(class string, req *cmd.UpdateTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPDATE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (s *Service) DeleteTenants(class string, req *cmd.DeleteTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(command)
}

func (st *Service) Execute(req *cmd.ApplyRequest) error {
	log.Printf("execute command: %v\n on class %v", req.Type, req.Class)
	if st.store.IsLeader() {
		return st.store.Execute(req)
	}
	leader := st.store.Leader()
	if leader == "" {
		return ErrLeaderNotFound
	}
	_, err := st.rpc.Apply(leader, req)
	return err
}

func (s *Service) Join(ctx context.Context, id, addr string, voter bool) error {
	log.Printf("membership.join %v %v %v", id, addr, voter)
	if s.store.IsLeader() {
		return s.store.Join(id, addr, voter)
	}
	leader := s.store.Leader()
	if leader == "" {
		return fmt.Errorf("cannot find leader")
	}
	req := &cmd.JoinPeerRequest{Id: id, Address: addr, Voter: voter}
	_, err := s.rpc.Join(ctx, leader, req)
	return err
}

func (s *Service) Remove(ctx context.Context, id string) error {
	log.Printf("membership.remove %v ", id)
	if s.store.IsLeader() {
		return s.store.Remove(id)
	}
	leader := s.store.Leader()
	if leader == "" {
		return ErrLeaderNotFound
	}
	req := &cmd.RemovePeerRequest{Id: id}
	_, err := s.rpc.Remove(ctx, leader, req)
	return err
}

func (s *Service) Stats() map[string]string {
	log.Printf("membership.Stats")
	return s.store.Stats()
}

func removeNilTenants(tenants []*cmd.Tenant) []*cmd.Tenant {
	n := 0
	for i := range tenants {
		if tenants[i] != nil {
			tenants[n] = tenants[i]
			n++
		}
	}
	return tenants[:n]
}
