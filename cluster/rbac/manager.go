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

package rbac

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	authZ  authorization.Controller
	logger logrus.FieldLogger
}

func NewManager(authZ authorization.Controller, logger logrus.FieldLogger) *Manager {
	return &Manager{authZ: authZ, logger: logger}
}

func (m *Manager) GetRoles(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		payload, _ := json.Marshal(cmd.QueryGetRolesResponse{})
		return payload, nil
	}
	subCommand := cmd.QueryGetRolesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	roles, err := m.authZ.GetRoles(subCommand.Roles...)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryGetRolesResponse{Roles: roles}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetRolesForUser(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		payload, _ := json.Marshal(cmd.QueryGetRolesForUserResponse{})
		return payload, nil
	}
	subCommand := cmd.QueryGetRolesForUserRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	roles, err := m.authZ.GetRolesForUser(subCommand.User)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryGetRolesForUserResponse{Roles: roles}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetUsersForRole(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		payload, _ := json.Marshal(cmd.QueryGetUsersForRoleResponse{})
		return payload, nil
	}
	subCommand := cmd.QueryGetUsersForRoleRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	users, err := m.authZ.GetUsersForRole(subCommand.Role)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryGetUsersForRoleResponse{Users: users}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) HasPermission(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		payload, _ := json.Marshal(cmd.QueryHasPermissionResponse{})
		return payload, nil
	}
	subCommand := cmd.QueryHasPermissionRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	hasPerm, err := m.authZ.HasPermission(subCommand.Role, subCommand.Permission)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryHasPermissionResponse{HasPermission: hasPerm}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) UpsertRolesPermissions(c *cmd.ApplyRequest) error {
	if m.authZ == nil {
		return nil
	}
	req := &cmd.CreateRolesRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	if req.Version < cmd.RBACLatestCommandPolicyVersion {
		for roleName, policies := range req.Roles {
			permissions := []*authorization.Policy{}
			for _, p := range policies {
				permissions = append(permissions, &p)
			}
			// remove old permissions
			if err := m.authZ.RemovePermissions(roleName, permissions); err != nil {
				return err
			}
		}
	}

	reqMigrated, err := migrateUpsertRolesPermissions(req)
	if err != nil {
		return err
	}

	return m.authZ.UpsertRolesPermissions(reqMigrated.Roles)
}

func (m *Manager) DeleteRoles(c *cmd.ApplyRequest) error {
	if m.authZ == nil {
		return nil
	}
	req := &cmd.DeleteRolesRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.DeleteRoles(req.Roles...)
}

func (m *Manager) AddRolesForUser(c *cmd.ApplyRequest) error {
	if m.authZ == nil {
		return nil
	}
	req := &cmd.AddRolesForUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.AddRolesForUser(req.User, req.Roles)
}

func (m *Manager) RemovePermissions(c *cmd.ApplyRequest) error {
	if m.authZ == nil {
		return nil
	}
	req := &cmd.RemovePermissionsRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	if req.Version < cmd.RBACLatestCommandPolicyVersion {
		if err := m.authZ.RemovePermissions(req.Role, req.Permissions); err != nil {
			return err
		}
	}

	reqMigrated, err := migrateRemovePermissions(req)
	if err != nil {
		return err
	}

	return m.authZ.RemovePermissions(reqMigrated.Role, reqMigrated.Permissions)
}

func (m *Manager) RevokeRolesForUser(c *cmd.ApplyRequest) error {
	if m.authZ == nil {
		return nil
	}
	req := &cmd.RevokeRolesForUserRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.RevokeRolesForUser(req.User, req.Roles...)
}
