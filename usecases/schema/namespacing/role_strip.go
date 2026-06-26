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

package namespacing

import "github.com/weaviate/weaviate/entities/models"

// StripRolesForCaller returns roles with the role name and each permission's
// namespace-bearing resource string stripped of the caller's own namespace
// prefix. It never mutates the input or its sub-structs in place: a permission's
// sub-structs may point at package-level singletons (authorization.AllCollections,
// etc.), and mutating those would corrupt every other caller.
func StripRolesForCaller(principal *models.Principal, roles []*models.Role) []*models.Role {
	if ConfinedNamespace(principal) == "" || len(roles) == 0 {
		return roles
	}
	out := make([]*models.Role, len(roles))
	for i, role := range roles {
		if role == nil {
			continue
		}
		copied := *role
		if role.Name != nil {
			stripped := StripOwnNamespace(principal, *role.Name)
			copied.Name = &stripped
		}
		if len(role.Permissions) > 0 {
			perms := make([]*models.Permission, len(role.Permissions))
			for j, p := range role.Permissions {
				perms[j] = StripPermissionForCaller(principal, p)
			}
			copied.Permissions = perms
		}
		out[i] = &copied
	}
	return out
}

// StripPermissionForCaller returns a permission with its namespace-bearing
// sub-structs replaced (not mutated) by fresh copies that strip the caller's
// own namespace prefix. Strip set: collection, alias, user-ref and role-ref
// names. Group refs are left untouched (groups are globally named), and the
// namespace identifier itself is never stripped.
func StripPermissionForCaller(principal *models.Principal, p *models.Permission) *models.Permission {
	if p == nil {
		return nil
	}
	out := *p
	if p.Collections != nil && p.Collections.Collection != nil {
		stripped := StripOwnNamespace(principal, *p.Collections.Collection)
		out.Collections = &models.PermissionCollections{Collection: &stripped}
	}
	if p.Data != nil && p.Data.Collection != nil {
		stripped := StripOwnNamespace(principal, *p.Data.Collection)
		fresh := *p.Data
		fresh.Collection = &stripped
		out.Data = &fresh
	}
	if p.Nodes != nil && p.Nodes.Collection != nil {
		stripped := StripOwnNamespace(principal, *p.Nodes.Collection)
		fresh := *p.Nodes
		fresh.Collection = &stripped
		out.Nodes = &fresh
	}
	if p.Tenants != nil && p.Tenants.Collection != nil {
		stripped := StripOwnNamespace(principal, *p.Tenants.Collection)
		fresh := *p.Tenants
		fresh.Collection = &stripped
		out.Tenants = &fresh
	}
	if p.Backups != nil && p.Backups.Collection != nil {
		stripped := StripOwnNamespace(principal, *p.Backups.Collection)
		out.Backups = &models.PermissionBackups{Collection: &stripped}
	}
	if p.Replicate != nil && p.Replicate.Collection != nil {
		stripped := StripOwnNamespace(principal, *p.Replicate.Collection)
		fresh := *p.Replicate
		fresh.Collection = &stripped
		out.Replicate = &fresh
	}
	if p.Aliases != nil {
		aliases := *p.Aliases
		changed := false
		if p.Aliases.Collection != nil {
			stripped := StripOwnNamespace(principal, *p.Aliases.Collection)
			aliases.Collection = &stripped
			changed = true
		}
		if p.Aliases.Alias != nil {
			stripped := StripOwnNamespace(principal, *p.Aliases.Alias)
			aliases.Alias = &stripped
			changed = true
		}
		if changed {
			out.Aliases = &aliases
		}
	}
	if p.Users != nil && p.Users.Users != nil {
		stripped := StripOwnNamespace(principal, *p.Users.Users)
		out.Users = &models.PermissionUsers{Users: &stripped}
	}
	if p.Roles != nil && p.Roles.Role != nil {
		stripped := StripOwnNamespace(principal, *p.Roles.Role)
		fresh := *p.Roles
		fresh.Role = &stripped
		out.Roles = &fresh
	}
	return &out
}
