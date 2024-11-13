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
	"github.com/weaviate/weaviate/entities/models"
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

func (m *Manager) GetRoles(names ...string) ([]*models.Role, error) {
	return m.authZ.GetRoles(names...)
}

func (m *Manager) GetRolesForUser(user string) ([]*models.Role, error) {
	return m.authZ.GetRolesForUser(user)
}

func (m *Manager) GetUsersForRole(role string) ([]string, error) {
	return m.authZ.GetUsersForRole(role)
}

func (m *Manager) UpsertRoles(c *cmd.ApplyRequest) error {
	req := &cmd.CreateRolesRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.UpsertRoles(req.Roles...)
}

func (m *Manager) DeleteRoles(c *cmd.ApplyRequest) error {
	req := &cmd.DeleteRolesRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.DeleteRoles(req.Roles...)
}

func (m *Manager) AddRolesForUser(c *cmd.ApplyRequest) error {
	req := &cmd.AddRolesForUsersRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.AddRolesForUser(req.User, req.Roles)
}

func (m *Manager) RemovePermissions(c *cmd.ApplyRequest) error {
	req := &cmd.RemovePermissionsRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.RemovePermissions(req.Role, req.Permissions)
}

func (m *Manager) RevokeRolesForUser(c *cmd.ApplyRequest) error {
	req := &cmd.RevokeRolesForUserRequest{}
	if err := json.Unmarshal(c.SubCommand, req); err != nil {
		return fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	return m.authZ.RevokeRolesForUser(req.User, req.Roles...)
}
