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
// prefix. Permissions that name a foreign namespace are dropped, so a confined
// caller cannot learn another namespace exists via a global role assigned to it.
// It never mutates the input or its sub-structs in place: a permission's
// sub-structs may point at package-level singletons (authorization.AllCollections,
// etc.), and mutating those would corrupt every other caller.
func StripRolesForCaller(principal *models.Principal, roles []*models.Role) []*models.Role {
	ownNS := ConfinedNamespace(principal)
	if ownNS == "" || len(roles) == 0 {
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
			perms := make([]*models.Permission, 0, len(role.Permissions))
			for _, p := range role.Permissions {
				if permissionNamesForeignNamespace(ownNS, p) {
					continue
				}
				perms = append(perms, StripPermissionForCaller(principal, p))
			}
			copied.Permissions = perms
		}
		out[i] = &copied
	}
	return out
}

// permissionNamesForeignNamespace reports whether p references a namespace other
// than ownNS. A collection/user/role/alias name qualified with a different
// namespace (leading `<ns>:` segment), or a namespaces-domain grant naming one,
// must be hidden from a confined caller so it cannot learn another namespace
// exists. The field set mirrors StripPermissionForCaller's strip set; a new
// namespace-bearing field must be added to both.
func permissionNamesForeignNamespace(ownNS string, p *models.Permission) bool {
	if p == nil {
		return false
	}
	names := make([]*string, 0, 10)
	if p.Collections != nil {
		names = append(names, p.Collections.Collection)
	}
	if p.Data != nil {
		names = append(names, p.Data.Collection)
	}
	if p.Nodes != nil {
		names = append(names, p.Nodes.Collection)
	}
	if p.Tenants != nil {
		names = append(names, p.Tenants.Collection)
	}
	if p.Backups != nil {
		names = append(names, p.Backups.Collection)
	}
	if p.Replicate != nil {
		names = append(names, p.Replicate.Collection)
	}
	if p.Aliases != nil {
		names = append(names, p.Aliases.Collection, p.Aliases.Alias)
	}
	if p.Users != nil {
		names = append(names, p.Users.Users)
	}
	if p.Roles != nil {
		names = append(names, p.Roles.Role)
	}
	for _, n := range names {
		if n == nil {
			continue
		}
		// The leading segment is the namespace; a value with none, or with the
		// caller's own, names no foreign namespace.
		if ns := NamespaceFromQualified(*n); ns != "" && ns != ownNS {
			return true
		}
	}
	// The namespaces domain names a namespace directly. A specific foreign name
	// leaks; the wildcard and the caller's own namespace do not.
	if p.Namespaces != nil && p.Namespaces.Namespace != nil {
		if ns := *p.Namespaces.Namespace; ns != "" && ns != "*" && ns != ".*" && ns != ownNS {
			return true
		}
	}
	return false
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
		fresh := *p.Collections
		fresh.Collection = &stripped
		out.Collections = &fresh
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
		fresh := *p.Backups
		fresh.Collection = &stripped
		out.Backups = &fresh
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
		fresh := *p.Users
		fresh.Users = &stripped
		out.Users = &fresh
	}
	if p.Roles != nil && p.Roles.Role != nil {
		stripped := StripOwnNamespace(principal, *p.Roles.Role)
		fresh := *p.Roles
		fresh.Role = &stripped
		out.Roles = &fresh
	}
	return &out
}
