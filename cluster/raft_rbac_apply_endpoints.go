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

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (s *Raft) UpdateRolesPermissions(roles map[string][]authorization.Policy) error {
	return s.upsertRolesPermissions(roles, false)
}

func (s *Raft) CreateRolesPermissions(roles map[string][]authorization.Policy) error {
	return s.upsertRolesPermissions(roles, true)
}

func (s *Raft) upsertRolesPermissions(roles map[string][]authorization.Policy, roleCreation bool) error {
	if len(roles) == 0 {
		return fmt.Errorf("no roles to create: %w", schema.ErrBadRequest)
	}

	req := cmd.CreateRolesRequest{Roles: roles, Version: cmd.RBACLatestCommandPolicyVersion, RoleCreation: roleCreation}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) DeleteRoles(names ...string) error {
	if len(names) == 0 {
		return fmt.Errorf("no roles to delete: %w", schema.ErrBadRequest)
	}
	req := cmd.DeleteRolesRequest{Roles: names}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_ROLES,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) RemovePermissions(role string, permissions []*authorization.Policy) error {
	if role == "" {
		return fmt.Errorf("no roles to remove permissions from: %w", schema.ErrBadRequest)
	}
	req := cmd.RemovePermissionsRequest{Role: role, Permissions: permissions, Version: cmd.RBACLatestCommandPolicyVersion}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REMOVE_PERMISSIONS,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) AddRolesForUser(user string, roles []string) error {
	if len(roles) == 0 {
		return fmt.Errorf("no roles to assign: %w", schema.ErrBadRequest)
	}
	req := cmd.AddRolesForUsersRequest{User: user, Roles: roles, Version: cmd.RBACAssignRevokeLatestCommandPolicyVersion}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ADD_ROLES_FOR_USER,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) RevokeRolesForUser(user string, roles ...string) error {
	if len(roles) == 0 {
		return fmt.Errorf("no roles to revoke: %w", schema.ErrBadRequest)
	}
	req := cmd.RevokeRolesForUserRequest{User: user, Roles: roles, Version: cmd.RBACAssignRevokeLatestCommandPolicyVersion}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_REVOKE_ROLES_FOR_USER,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}
