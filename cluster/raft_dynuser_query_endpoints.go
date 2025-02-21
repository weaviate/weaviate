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
