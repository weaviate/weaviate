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
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
)

func (s *Raft) GetUsers(userIds ...string) (map[string]*apikey.User, error) {
	req := cmd.QueryGetUsersRequest{
		UserIds: userIds,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return nil, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_GET_USERS,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryGetUsersResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Users, nil
}

func (s *Raft) CheckUserIdentifierExists(userIdentifier string) (bool, error) {
	req := cmd.QueryUserIdentifierExistsRequest{
		UserIdentifier: userIdentifier,
	}

	subCommand, err := json.Marshal(&req)
	if err != nil {
		return false, fmt.Errorf("marshal request: %w", err)
	}

	command := &cmd.QueryRequest{
		Type:       cmd.QueryRequest_TYPE_USER_IDENTIFIER_EXISTS,
		SubCommand: subCommand,
	}
	queryResp, err := s.Query(context.Background(), command)
	if err != nil {
		return false, fmt.Errorf("failed to execute query: %w", err)
	}

	response := cmd.QueryUserIdentifierExistsResponse{}
	err = json.Unmarshal(queryResp.Payload, &response)
	if err != nil {
		return false, fmt.Errorf("failed to unmarshal query result: %w", err)
	}

	return response.Exists, nil
}
