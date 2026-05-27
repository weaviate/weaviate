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

package rest

import (
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/usecases/auth/authentication"

	cerrors "github.com/weaviate/weaviate/adapters/handlers/rest/errors"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/users"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	authzConv "github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

type authNHandlers struct {
	authzController authorization.Controller
	rbacConfig      rbacconf.Config
	logger          logrus.FieldLogger
}

func setupAuthnHandlers(api *operations.WeaviateAPI, controller authorization.Controller, rbacConfig rbacconf.Config, logger logrus.FieldLogger,
) {
	h := &authNHandlers{authzController: controller, logger: logger, rbacConfig: rbacConfig}
	// user handlers
	api.UsersGetOwnInfoHandler = users.GetOwnInfoHandlerFunc(h.getOwnInfo)
}

func (h *authNHandlers) getOwnInfo(_ users.GetOwnInfoParams, principal *models.Principal) middleware.Responder {
	if principal == nil {
		return users.NewGetOwnInfoUnauthorized()
	}

	var roles []*models.Role
	rolenames := map[string]struct{}{}
	if h.rbacConfig.Enabled {
		existingRoles, err := h.authzController.GetRolesForUserOrGroup(principal.Username, authentication.AuthType(principal.UserType), false)
		if err != nil {
			return users.NewGetOwnInfoInternalServerError()
		}
		for _, group := range principal.Groups {
			groupRoles, err := h.authzController.GetRolesForUserOrGroup(group, authentication.AuthType(principal.UserType), true)
			if err != nil {
				return users.NewGetOwnInfoInternalServerError()
			}
			for roleName, policies := range groupRoles {
				existingRoles[roleName] = policies
			}
		}
		for roleName, policies := range existingRoles {
			perms, err := authzConv.PoliciesToPermission(policies...)
			if err != nil {
				return users.NewGetOwnInfoInternalServerError().WithPayload(cerrors.ErrPayloadFromSingleErr(principal, err))
			}
			roles = append(roles, &models.Role{
				Name:        &roleName,
				Permissions: perms,
			})
			rolenames[roleName] = struct{}{}
		}
	}

	h.logger.WithFields(logrus.Fields{
		"action":    "get_own_info",
		"component": "authN",
		"user":      principal.Username,
	}).Info("own info requested")

	username := namespacing.StripOwnNamespace(principal, principal.Username)
	return users.NewGetOwnInfoOK().WithPayload(&models.UserOwnInfo{
		Groups:   principal.Groups,
		Roles:    stripRolesForCaller(principal, roles),
		Username: &username,
	})
}

// stripRolesForCaller returns roles with each permission's namespace-bearing
// resource string stripped of the caller's own namespace prefix. It never
// mutates the input or its sub-structs in place: PoliciesToPermission may
// point sub-structs at package-level singletons (authorization.AllCollections,
// etc.), and mutating those would corrupt every other caller. Phase-1 stored
// `p`/`g` policies are unqualified templates, so this is usually a no-op
// today; doing the strip anyway keeps Phase-2 namespaced roles safe to expose
// without revisiting this handler.
func stripRolesForCaller(principal *models.Principal, roles []*models.Role) []*models.Role {
	if principal == nil || principal.Namespace == "" || len(roles) == 0 {
		return roles
	}
	out := make([]*models.Role, len(roles))
	for i, role := range roles {
		if role == nil {
			continue
		}
		copied := *role
		if len(role.Permissions) > 0 {
			perms := make([]*models.Permission, len(role.Permissions))
			for j, p := range role.Permissions {
				perms[j] = stripPermissionForCaller(principal, p)
			}
			copied.Permissions = perms
		}
		out[i] = &copied
	}
	return out
}

// stripPermissionForCaller returns a permission with its namespace-bearing
// sub-structs replaced (not mutated) by fresh copies that strip the caller's
// own namespace prefix. Strip set: collection/alias only;
// users/roles/tenants/replicate.shard/groups/namespaces fields untouched.
func stripPermissionForCaller(principal *models.Principal, p *models.Permission) *models.Permission {
	if p == nil {
		return nil
	}
	out := *p
	if p.Collections != nil && p.Collections.Collection != nil {
		stripped := namespacing.StripOwnNamespace(principal, *p.Collections.Collection)
		out.Collections = &models.PermissionCollections{Collection: &stripped}
	}
	if p.Data != nil && p.Data.Collection != nil {
		stripped := namespacing.StripOwnNamespace(principal, *p.Data.Collection)
		fresh := *p.Data
		fresh.Collection = &stripped
		out.Data = &fresh
	}
	if p.Nodes != nil && p.Nodes.Collection != nil {
		stripped := namespacing.StripOwnNamespace(principal, *p.Nodes.Collection)
		fresh := *p.Nodes
		fresh.Collection = &stripped
		out.Nodes = &fresh
	}
	if p.Tenants != nil && p.Tenants.Collection != nil {
		stripped := namespacing.StripOwnNamespace(principal, *p.Tenants.Collection)
		fresh := *p.Tenants
		fresh.Collection = &stripped
		out.Tenants = &fresh
	}
	if p.Backups != nil && p.Backups.Collection != nil {
		stripped := namespacing.StripOwnNamespace(principal, *p.Backups.Collection)
		out.Backups = &models.PermissionBackups{Collection: &stripped}
	}
	if p.Replicate != nil && p.Replicate.Collection != nil {
		stripped := namespacing.StripOwnNamespace(principal, *p.Replicate.Collection)
		fresh := *p.Replicate
		fresh.Collection = &stripped
		out.Replicate = &fresh
	}
	if p.Aliases != nil {
		aliases := *p.Aliases
		changed := false
		if p.Aliases.Collection != nil {
			stripped := namespacing.StripOwnNamespace(principal, *p.Aliases.Collection)
			aliases.Collection = &stripped
			changed = true
		}
		if p.Aliases.Alias != nil {
			stripped := namespacing.StripOwnNamespace(principal, *p.Aliases.Alias)
			aliases.Alias = &stripped
			changed = true
		}
		if changed {
			out.Aliases = &aliases
		}
	}
	return &out
}
