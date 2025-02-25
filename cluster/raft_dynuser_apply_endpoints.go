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
)

func (s *Raft) CreateUser(userId, secureHash, userIdentifier string) error {
	req := cmd.CreateUsersRequest{
		UserId:         userId,
		SecureHash:     secureHash,
		UserIdentifier: userIdentifier,
		Version:        cmd.DynUserLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_UPSERT_USER,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) RotateKey(userId, secureHash string) error {
	req := cmd.RotateUserApiKeyRequest{
		UserId:     userId,
		SecureHash: secureHash,
		Version:    cmd.DynUserLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ROTATE_USER_API_KEY,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) DeleteUser(userId string) error {
	req := cmd.DeleteUsersRequest{
		UserId:  userId,
		Version: cmd.DynUserLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_DELETE_USER,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}
