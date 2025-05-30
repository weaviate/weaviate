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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
)

func (s *Raft) CreateUser(userId, secureHash, userIdentifier, apiKeyFirstLetters string, createdAt time.Time) error {
	req := cmd.CreateUsersRequest{
		UserId:             userId,
		SecureHash:         secureHash,
		UserIdentifier:     userIdentifier,
		CreatedAt:          createdAt,
		ApiKeyFirstLetters: apiKeyFirstLetters,
		Version:            cmd.DynUserLatestCommandPolicyVersion,
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

func (s *Raft) CreateUserWithKey(userId, apiKeyFirstLetters string, weakHash [sha256.Size]byte, createdAt time.Time) error {
	req := cmd.CreateUserWithKeyRequest{
		UserId:             userId,
		CreatedAt:          createdAt,
		ApiKeyFirstLetters: apiKeyFirstLetters,
		WeakHash:           weakHash,
		Version:            cmd.DynUserLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_CREATE_USER_WITH_KEY,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) RotateKey(userId, apiKeyFirstLetters, secureHash, oldIdentifier, newIdentifier string) error {
	req := cmd.RotateUserApiKeyRequest{
		UserId:             userId,
		ApiKeyFirstLetters: apiKeyFirstLetters,
		SecureHash:         secureHash,
		OldIdentifier:      oldIdentifier,
		NewIdentifier:      newIdentifier,
		Version:            cmd.DynUserLatestCommandPolicyVersion,
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

func (s *Raft) ActivateUser(userId string) error {
	req := cmd.ActivateUsersRequest{
		UserId:  userId,
		Version: cmd.DynUserLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_ACTIVATE_USER,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}

func (s *Raft) DeactivateUser(userId string, revokeKey bool) error {
	req := cmd.SuspendUserRequest{
		UserId:    userId,
		RevokeKey: revokeKey,
		Version:   cmd.DynUserLatestCommandPolicyVersion,
	}
	subCommand, err := json.Marshal(&req)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	command := &cmd.ApplyRequest{
		Type:       cmd.ApplyRequest_TYPE_SUSPEND_USER,
		SubCommand: subCommand,
	}
	if _, err := s.Execute(context.Background(), command); err != nil {
		return err
	}
	return nil
}
