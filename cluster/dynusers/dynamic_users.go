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
	"github.com/weaviate/weaviate/entities/dbuser"
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

	if err := usecasesNamespaces.RequireActive(m.namespaces, req.Namespace); err != nil {
		return fmt.Errorf("%w: %q", err, req.Namespace)
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

	// These pointers are local and never shared.
	wireUsers := make(map[string]*dbuser.View, len(users))
	for id, v := range users {
		wireUsers[id] = &v
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
	// false: RAFT log compaction never strips; only the backup-restore path does.
	err := m.dynUser.Restore(snapshot, false)
	if err != nil {
		m.logger.Errorf("restored db users from snapshot failed with: %v", err)
		return err
	}
	m.logger.Info("successfully restored dynamic users from snapshot")
	return nil
}

// ValidateBackupSnapshot checks the user blob without touching the user store.
// When namespaces are enabled, every namespace the blob references must be
// active.
func (m *Manager) ValidateBackupSnapshot(req *cmd.RestoreRolesAndUsersRequest, ns usecasesNamespaces.Exister) error {
	if m.dynUser == nil || len(req.Users) == 0 {
		return nil
	}
	if err := apikey.ValidateSnapshot(req.Users, req.StripNamespaces); err != nil {
		return err
	}
	if req.StripNamespaces {
		// The strip removes every namespace prefix, so there is nothing to check.
		return nil
	}
	refs, err := apikey.ReferencedNamespaces(req.Users)
	if err != nil {
		return err
	}
	if err := usecasesNamespaces.RequireActiveAll(ns, refs); err != nil {
		return fmt.Errorf("restore users: %w", err)
	}
	return nil
}

// RestoreFromBackup replaces the whole user store and persists it. Restore
// serves RAFT snapshot install instead: it never strips and must not touch
// disk, because a failure there stops the node booting.
func (m *Manager) RestoreFromBackup(req *cmd.RestoreRolesAndUsersRequest) error {
	if m.dynUser == nil || len(req.Users) == 0 {
		return nil
	}
	if err := m.dynUser.Restore(req.Users, req.StripNamespaces); err != nil {
		return err
	}
	// The file is only a boot cache. A failed write must not fail an apply
	// that the other nodes completed.
	if err := m.dynUser.Persist(); err != nil {
		m.logger.WithField("action", "restore_users_from_backup").
			Warnf("restored users are not on disk yet, RAFT state remains authoritative: %v", err)
	}
	m.logger.WithField("action", "restore_users_from_backup").
		Info("replaced dynamic-user state from backup")
	return nil
}
