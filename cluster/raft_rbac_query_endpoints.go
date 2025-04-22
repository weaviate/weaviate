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

	"github.com/weaviate/weaviate/entities/models"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

func (s *Raft) GetRoles(names ...string) (map[string][]authorization.Policy, error) {
	req := cmd.QueryGetRolesRequest{
		Roles: names,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_ROLES,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryGetRolesResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Roles, nil
}

func (s *Raft) GetRolesForUser(user string, userType models.UserTypeInput) (map[string][]authorization.Policy, error) {
	req := cmd.QueryGetRolesForUserRequest{
		User:     user,
		UserType: userType,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryGetRolesForUserResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Roles, nil
}

func (s *Raft) GetUsersForRole(role string, userType models.UserTypeInput) ([]string, error) {
	req := cmd.QueryGetUsersForRoleRequest{
		Role:     role,
		UserType: userType,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_USERS_FOR_ROLE,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryGetUsersForRoleResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Users, nil
}

// HasPermission returns consistent permissions check by asking the leader
func (s *Raft) HasPermission(roleName string, permission *authorization.Policy) (bool, error) {
	req := cmd.QueryHasPermissionRequest{
		Role:       roleName,
		Permission: permission,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_HAS_PERMISSION,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryHasPermissionResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.HasPermission, nil
}
