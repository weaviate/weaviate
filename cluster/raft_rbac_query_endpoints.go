//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2026 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/types"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
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

func (s *Raft) GetUsersOrGroupsWithRoles(isGroup bool, authType authentication.AuthType) ([]string, error) {
	req := cmd.QueryGetAllUsersOrGroupsWithRolesRequest{
		IsGroup:  isGroup,
		AuthType: authType,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_USERS_OR_GROUPS_WITH_ROLES,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryGetAllUsersOrGroupsWithRolesResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.UsersOrGroups, nil
}

func (s *Raft) GetRolesForUserOrGroup(user string, authType authentication.AuthType, isGroup bool) (map[string][]authorization.Policy, error) {
	req := cmd.QueryGetRolesForUserOrGroupRequest{
		User:     user,
		UserType: authType,
		IsGroup:  isGroup,
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

	response := cmd.QueryGetRolesForUserOrGroupResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Roles, nil
}

func (s *Raft) GetRolesForSubjects(subjects []authorization.Subject) (map[string]map[string][]authorization.Policy, error) {
	query := func(req *cmd.QueryRequest) (*cmd.QueryResponse, error) {
		return s.Query(context.Background(), req)
	}
	single := func(subject authorization.Subject) (map[string][]authorization.Policy, error) {
		return s.GetRolesForUserOrGroup(subject.ID, subject.AuthType, subject.IsGroup)
	}
	return getRolesForSubjectsWithFallback(query, single, subjects, s.log)
}

// getRolesForSubjectsWithFallback asks for every subject's roles in one query, and calls
// single per subject when the leader does not know that query type. It re-asks on every
// call, because leadership can move between nodes of different versions.
func getRolesForSubjectsWithFallback(
	query func(*cmd.QueryRequest) (*cmd.QueryResponse, error),
	single func(authorization.Subject) (map[string][]authorization.Policy, error),
	subjects []authorization.Subject,
	log logrus.FieldLogger,
) (map[string]map[string][]authorization.Policy, error) {
	if len(subjects) == 0 {
		return map[string]map[string][]authorization.Policy{}, nil
	}

	subCommand, err := json.Marshal(&cmd.QueryGetRolesForSubjectsRequest{Subjects: subjects})
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_ROLES_FOR_USER_LIST,
		SubCommand: subCommand,
	}
	queryResp, err := query(command)
	// TYPE_GET_ROLES_FOR_USER_LIST is new in the 1.39 patch after v1.39.4. A leader on
	// v1.39.4 or earlier, including every 1.38 and older release, does not know it and
	// answers codes.Internal. The per-subject fallback uses TYPE_GET_ROLES_FOR_USER,
	// which those leaders do serve.
	if isUnknownQueryType(err) {
		log.Warnf("leader does not know query type %s, likely a version skew during a rolling upgrade; "+
			"looking up roles for %d subjects one at a time: %v", command.Type, len(subjects), err)
		return getRolesPerSubject(single, subjects)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryGetRolesForSubjectsResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Roles, nil
}

func getRolesPerSubject(
	single func(authorization.Subject) (map[string][]authorization.Policy, error),
	subjects []authorization.Subject,
) (map[string]map[string][]authorization.Policy, error) {
	roles := make(map[string]map[string][]authorization.Policy, len(subjects))
	for _, subject := range subjects {
		key := conv.SubjectKey(subject)
		subjectRoles, err := single(subject)
		if err != nil {
			return nil, fmt.Errorf("get roles for %q: %w", key, err)
		}
		roles[key] = subjectRoles
	}
	return roles, nil
}

// isUnknownQueryType reports whether the leader has no handler for the query type. A
// leader whose gRPC server does not map types.ErrUnknownCommand answers codes.Internal
// with "unknown command type <n>" instead. Drop that match once no supported upgrade
// starts from such a leader.
func isUnknownQueryType(err error) bool {
	if errors.Is(err, types.ErrUnknownCommand) {
		return true
	}
	st, ok := status.FromError(err)
	return ok && st.Code() == codes.Internal &&
		strings.Contains(st.Message(), "unknown command type")
}

func (s *Raft) GetUsersOrGroupForRole(role string, authType authentication.AuthType, isGroup bool) ([]string, error) {
	req := cmd.QueryGetUsersForRoleRequest{
		Role:     role,
		UserType: authType,
		IsGroup:  isGroup,
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
