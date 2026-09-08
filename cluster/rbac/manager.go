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

package rbac

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"

	"github.com/sirupsen/logrus"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/config"
	usecasesNamespaces "github.com/weaviate/weaviate/usecases/namespaces"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

var ErrBadRequest = errors.New("bad request")

type Manager struct {
	authZ       *rbac.Manager
	authNconfig config.Authentication
	logger      logrus.FieldLogger
}

func NewManager(authZ *rbac.Manager, authNconfig config.Authentication, logger logrus.FieldLogger) *Manager {
	return &Manager{authZ: authZ, authNconfig: authNconfig, logger: logger}
}

func (m *Manager) GetRoles(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		return json.Marshal(cmd.QueryGetRolesResponse{})
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

func (m *Manager) GetUsersOrGroupsWithRoles(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		payload, _ := json.Marshal(cmd.QueryGetAllUsersOrGroupsWithRolesResponse{})
		return payload, nil
	}
	subCommand := cmd.QueryGetAllUsersOrGroupsWithRolesRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	usersOrGroups, err := m.authZ.GetUsersOrGroupsWithRoles(subCommand.IsGroup, subCommand.AuthType)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryGetAllUsersOrGroupsWithRolesResponse{UsersOrGroups: usersOrGroups}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetRolesForUserOrGroup(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		payload, _ := json.Marshal(cmd.QueryGetRolesForUserOrGroupResponse{})
		return payload, nil
	}
	subCommand := cmd.QueryGetRolesForUserOrGroupRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	roles, err := m.authZ.GetRolesForUserOrGroup(subCommand.User, subCommand.UserType, subCommand.IsGroup)
	if err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	response := cmd.QueryGetRolesForUserOrGroupResponse{Roles: roles}
	payload, err := json.Marshal(response)
	if err != nil {
		return []byte{}, fmt.Errorf("could not marshal query response: %w", err)
	}
	return payload, nil
}

func (m *Manager) GetUsersForRole(req *cmd.QueryRequest) ([]byte, error) {
	if m.authZ == nil {
		return json.Marshal(cmd.QueryGetUsersForRoleResponse{})
	}

	subCommand := cmd.QueryGetUsersForRoleRequest{}
	if err := json.Unmarshal(req.SubCommand, &subCommand); err != nil {
		return []byte{}, fmt.Errorf("%w: %w", ErrBadRequest, err)
	}

	users, err := m.authZ.GetUsersOrGroupForRole(subCommand.Role, subCommand.UserType, subCommand.IsGroup)
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
		return json.Marshal(cmd.QueryHasPermissionResponse{})
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

	// Scan all roles, not just the exact names, to enforce short-name
	// uniqueness across namespaces. The handler's pre-check read is not atomic
	// with this write; applies run serially, so this is the authoritative guard.
	if req.RoleCreation {
		allRoles, err := m.authZ.GetRoles()
		if err != nil {
			return err
		}
		existing := maps.Keys(allRoles)
		for name := range req.Roles {
			if namespacing.FindShortNameConflict(existing, name) != namespacing.NoRoleConflict {
				return fmt.Errorf("%w: roles already exist", ErrBadRequest)
			}
		}
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

	return m.authZ.UpdateRolesPermissions(reqMigrated.Roles) // update is upsert, naming is to satisfy interface
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

	reqs, err := migrateAssignRoles(req, m.authNconfig)
	if err != nil {
		return fmt.Errorf("migrateAssign: %w", err)
	}
	for _, req := range reqs {
		if err := m.authZ.AddRolesForUser(req.User, req.Roles); err != nil {
			return err
		}
	}
	return nil
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

	reqs, err := migrateRevokeRoles(req)
	if err != nil {
		return fmt.Errorf("migrateRevoke: %w", err)
	}
	for _, req := range reqs {
		if err := m.authZ.RevokeRolesForUser(req.User, req.Roles...); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) Snapshot() ([]byte, error) {
	if m.authZ == nil {
		return nil, nil
	}
	return m.authZ.Snapshot()
}

func (m *Manager) Restore(b []byte) error {
	if m.authZ == nil {
		return nil
	}
	if err := m.authZ.Restore(b, false); err != nil {
		return err
	}
	m.logger.Info("successfully restored rbac from snapshot")
	return nil
}

// ValidateBackupSnapshot checks the backup's roles without changing anything.
// If this cluster uses namespaces, every namespace the roles name must exist
// and not be deleting. Suspended and resuming namespaces are accepted because
// they keep their rows, so restoring those rows is legal and must not block a
// cluster-wide restore.
func (m *Manager) ValidateBackupSnapshot(req *cmd.RestoreRolesAndUsersRequest, ns usecasesNamespaces.Exister) error {
	if m.authZ == nil || len(req.Roles) == 0 {
		return nil
	}
	staticAPIKeyUsers := rbac.StaticAPIKeyUsers(m.authNconfig)
	if err := rbac.ValidateSnapshot(req.Roles, req.StripNamespaces, staticAPIKeyUsers); err != nil {
		return err
	}
	if req.StripNamespaces {
		// This cluster has namespaces turned off, so there is no namespace here
		// that could be active. The check above covers this case instead.
		return nil
	}
	if err := rbac.RequireReferencedNamespacesExist(req.Roles, staticAPIKeyUsers, ns); err != nil {
		return fmt.Errorf("restore roles: %w", err)
	}
	return nil
}

// RestoreFromBackup replaces every role with the ones from the backup.
// Not to be confused with Restore, which loads roles when a node starts up.
func (m *Manager) RestoreFromBackup(req *cmd.RestoreRolesAndUsersRequest) error {
	if m.authZ == nil || len(req.Roles) == 0 {
		return nil
	}
	if err := m.authZ.Restore(req.Roles, req.StripNamespaces); err != nil {
		// The restore wipes the old roles before the part that can fail, so this
		// node may now have no custom roles at all while every other node
		// succeeded. Log a fixed word so this is easy to search for.
		m.logger.WithField("action", "restore_roles_from_backup").
			Errorf("rbac_restore_torn: role store may be cleared on this node only: %v", err)
		return err
	}
	m.logger.WithField("action", "restore_roles_from_backup").
		WithField("strip_namespaces", req.StripNamespaces).
		Info("replaced rbac state from backup")
	return nil
}
