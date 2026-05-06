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

var (
	ErrBadRequest        = errors.New("bad request")
	ErrNamespaceNotFound = errors.New("namespace not found")
)

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

	// Defense-in-depth: on namespace-enabled clusters every db user must be
	// bound to a namespace. The handler already enforces this, but a stale
	// or hand-crafted RAFT command must not be able to slip an unbound user
	// past the apply path.
	if m.namespacesEnabled && req.Namespace == "" {
		return fmt.Errorf("%w: namespace is required on namespace-enabled clusters", ErrBadRequest)
	}

	// Authoritative existence re-check at apply time. Closes the
	// handler→apply TOCTOU when a delete-namespace command is RAFT-ordered
	// before this create-user command: every node observes identical state
	// and rejects deterministically.
	if req.Namespace != "" && !m.namespaces.Exists(req.Namespace) {
		return fmt.Errorf("%w: namespace %q does not exist", ErrNamespaceNotFound, req.Namespace)
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

	response := cmd.QueryGetUsersResponse{Users: users}
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
