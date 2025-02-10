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
	"strings"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
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

	// loop through updates until current version is reached
UPDATE_LOOP:
	for {
		switch req.Version {
		case cmd.RBACCommandPolicyVersionV0:
			for roleName, policies := range req.Roles {
				permissions := []*authorization.Policy{}
				for _, p := range policies {
					permissions = append(permissions, &p)
				}
				// remove old permissions
				if err := m.authZ.RemovePermissions(roleName, permissions); err != nil {
					return err
				}

				// create new permissions
				for idx := range policies {
					if req.Roles[roleName][idx].Domain == authorization.SchemaDomain {
						parts := strings.Split(req.Roles[roleName][idx].Resource, "/")
						if len(parts) < 3 {
							// shall never happens
							return fmt.Errorf("invalid schema path")
						}
						req.Roles[roleName][idx].Resource = authorization.CollectionsMetadata(parts[2])[0]
					}

					if req.Roles[roleName][idx].Domain == authorization.RolesDomain &&
						req.Roles[roleName][idx].Verb == conv.CRUD {
						// this will override any role was created before 1.28
						// to reset default to
						req.Roles[roleName][idx].Verb = authorization.ROLE_SCOPE_MATCH
					}
				}
			}
		case cmd.RBACCommandPolicyVersionV1:
			for roleName, policies := range req.Roles {
				permissions := []*authorization.Policy{}
				for _, p := range policies {
					permissions = append(permissions, &p)
				}
				// remove old permissions
				if err := m.authZ.RemovePermissions(roleName, permissions); err != nil {
					return err
				}

				// create new permissions
				for idx := range policies {
					if req.Roles[roleName][idx].Domain == authorization.RolesDomain &&
						req.Roles[roleName][idx].Verb == conv.CRUD {
						// this will override any role was created before 1.28
						// to reset default to
						req.Roles[roleName][idx].Verb = authorization.ROLE_SCOPE_MATCH
					}
				}
			}
		case cmd.RBACCommandPolicyVersionV2:
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

			req.Roles = migrateV2(req.Roles)

		default:
			break UPDATE_LOOP
		}
		req.Version += 1

	}

	return m.authZ.UpsertRolesPermissions(req.Roles)
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

	switch req.Version {
	case cmd.RBACCommandPolicyVersionV0:
		// keep to remove old formats
		if err := m.authZ.RemovePermissions(req.Role, req.Permissions); err != nil {
			return err
		}
		// remove any added with new format after migration
		for idx := range req.Permissions {
			if req.Permissions[idx].Domain != authorization.SchemaDomain {
				continue
			}
			parts := strings.Split(req.Permissions[idx].Resource, "/")
			if len(parts) < 3 {
				// shall never happens
				return fmt.Errorf("invalid schema path")
			}
			req.Permissions[idx].Resource = authorization.CollectionsMetadata(parts[2])[0]
		}
	default:
		// do nothing
	}

	return m.authZ.RemovePermissions(req.Role, req.Permissions)
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
