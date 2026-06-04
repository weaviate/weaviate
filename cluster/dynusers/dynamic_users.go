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

package dynusers

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authentication/apikey"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	dynUser           *apikey.DBUser
	namespaces        usecasesNamespaces.Exister
	namespacesEnabled bool
	logger            logrus.FieldLogger
}

func NewManager(dynUser *apikey.DBUser, namespaces usecasesNamespaces.Exister, namespacesEnabled bool, logger logrus.FieldLogger) *Manager {
	if namespaces == nil {
		panic("cluster/dynusers: namespaces controller must not be nil")
	}
	return &Manager{dynUser: dynUser, namespaces: namespaces, namespacesEnabled: namespacesEnabled, logger: logger}
}

func (m *Manager) CreateUser(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.CreateUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	if m.namespacesEnabled && req.Namespace == "" {
		return fmt.Errorf("%w: namespace is required on namespace-enabled clusters", ErrBadRequest)
	}

	if req.Namespace != "" {
		if !m.namespaces.Exists(req.Namespace) {
			return fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceGone, req.Namespace)
		}
		if !m.namespaces.IsActive(req.Namespace) {
			return fmt.Errorf("%w: %q", usecasesNamespaces.ErrNamespaceDeleting, req.Namespace)
		}
	}

	return m.dynUser.CreateUser(req.UserId, req.SecureHash, req.UserIdentifier, req.ApiKeyFirstLetters, req.Namespace, req.CreatedAt)
}

func (m *Manager) CreateUserWithKeyRequest(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.CreateUserWithKeyRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.dynUser.CreateUserWithKey(req.UserId, req.ApiKeyFirstLetters, req.WeakHash, req.CreatedAt)
}

func (m *Manager) DeleteUser(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.DeleteUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.dynUser.DeleteUser(req.UserId)
}

func (m *Manager) DeleteUsersInNamespace(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.DeleteUsersInNamespaceRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	if req.Namespace == "" {
		return fmt.Errorf("%w: namespace is required", ErrBadRequest)
	}
	return m.dynUser.DeleteUsersInNamespace(req.Namespace)
}

func (m *Manager) ActivateUser(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.ActivateUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.dynUser.ActivateUser(req.UserId)
}

func (m *Manager) SuspendUser(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.SuspendUserRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.dynUser.DeactivateUser(req.UserId, req.RevokeKey)
}

func (m *Manager) RotateKey(c *cmd.ApplyRequest) error {
	if m.dynUser == nil {
		return nil
	}
	req := &cmd.RotateUserApiKeyRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.dynUser.RotateKey(req.UserId, req.ApiKeyFirstLetters, req.SecureHash, req.OldIdentifier, req.NewIdentifier)
}

func (m *Manager) GetUsers(req *cmd.QueryRequest) ([]byte, error) {
	if m.dynUser == nil {
		payload, _ := json.Marshal(cmd.QueryGetUsersRequest{})
		return payload, nil
	}
	subCommand := cmd.QueryGetUsersRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	users, err := m.dynUser.GetUsers(subCommand.UserIds...)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	// Rebuild *apikey.User for the wire to keep the response shape stable;
	// these pointers are local and never shared.
	wireUsers := make(map[string]*apikey.User, len(users))
	for id, v := range users {
		wireUsers[id] = &apikey.User{
			Id:                 v.Id,
			Active:             v.Active,
			InternalIdentifier: v.InternalIdentifier,
			ApiKeyFirstLetters: v.ApiKeyFirstLetters,
			CreatedAt:          v.CreatedAt,
			LastUsedAt:         v.LastUsedAt,
			ImportedWithKey:    v.ImportedWithKey,
			Namespace:          v.Namespace,
		}
	}
	response := cmd.QueryGetUsersResponse{Users: wireUsers}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) CheckUserIdentifierExists(req *cmd.QueryRequest) ([]byte, error) {
	if m.dynUser == nil {
		payload, _ := json.Marshal(cmd.QueryGetUsersRequest{})
		return payload, nil
	}
	subCommand := cmd.QueryUserIdentifierExistsRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}
	exists, err := m.dynUser.CheckUserIdentifierExists(subCommand.UserIdentifier)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryUserIdentifierExistsResponse{Exists: exists}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) Snapshot() ([]byte, error) {
	if m.dynUser == nil {
		return nil, nil
	}
	return m.dynUser.Snapshot()
}

func (m *Manager) Restore(snapshot []byte) error {
	if m.dynUser == nil {
		return nil
	}
	err := m.dynUser.Restore(snapshot)
	if err != nil {
		m.logger.Errorf("restored db users from snapshot failed with: %v", err)
		return err
	}
	m.logger.Info("successfully restored dynamic users from snapshot")
	return nil
}
