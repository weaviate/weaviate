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
	"encoding/json"
	"fmt"
	"log"

	command "github.com/weaviate/weaviate/cloud/proto/cluster"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/sharding"
	"google.golang.org/protobuf/proto"
)

// Service abstracts away the Raft store, providing clients with an interface that encompasses all write operations.
// It ensures that these operations are executed on the current leader, regardless of the specific leader in the cluster.
type Service struct {
	store *Store
	rpc   *Client
}

func NewService(store *Store, client *Client) *Service {
	return &Service{store: store, rpc: client}
}

/// RAFT-TODO Documentation

func (s *Service) AddClass(cls *models.Class, ss *sharding.State) error {
	req := command.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_ADD_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) UpdateClass(cls *models.Class, ss *sharding.State) error {
	req := command.UpdateClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_UPDATE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) DeleteClass(name string) error {
	cmd := &command.ApplyRequest{
		Type:  command.ApplyRequest_TYPE_DELETE_CLASS,
		Class: name,
	}
	return s.Execute(cmd)
}

func (s *Service) RestoreClass(cls *models.Class, ss *sharding.State) error {
	req := command.AddClassRequest{Class: cls, State: ss}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_RESTORE_CLASS,
		Class:      cls.Class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) AddProperty(class string, p *models.Property) error {
	req := command.AddPropertyRequest{Property: p}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_ADD_PROPERTY,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) UpdateShardStatus(class, shard, status string) error {
	req := command.UpdateShardStatusRequest{Class: class, Shard: shard, Status: status}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_UPDATE_SHARD_STATUS,
		Class:      req.Class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) AddTenants(class string, req *command.AddTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_ADD_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) UpdateTenants(class string, req *command.UpdateTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_UPDATE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (s *Service) DeleteTenants(class string, req *command.DeleteTenantsRequest) error {
	subCommand, err := proto.Marshal(req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	cmd := &command.ApplyRequest{
		Type:       command.ApplyRequest_TYPE_DELETE_TENANT,
		Class:      class,
		SubCommand: subCommand,
	}
	return s.Execute(cmd)
}

func (st *Service) Execute(req *command.ApplyRequest) error {
	log.Printf("execute command: %v\n on class %v", req.Type, req.Class)
	if st.store.IsLeader() {
		return st.store.Execute(req)
	}
	leader := st.store.Leader()
	if leader == "" {
		return ErrLeaderNotFound
	}
	_, err := st.rpc.apply(leader, req)
	return err
}

func removeNilTenants(tenants []*command.Tenant) []*command.Tenant {
	n := 0
	for i := range tenants {
		if tenants[i] != nil {
			tenants[n] = tenants[i]
			n++
		}
	}
	return tenants[:n]
}
