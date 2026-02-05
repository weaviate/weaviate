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
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
)

// NamespaceRoleType defines the type of namespace-scoped role
type NamespaceRoleType string

const (
	NamespaceRoleAdmin  NamespaceRoleType = "admin"
	NamespaceRoleEditor NamespaceRoleType = "editor"
	NamespaceRoleViewer NamespaceRoleType = "viewer"
)

// NamespaceRoleName returns the role name for a namespace and role type.
// Example: NamespaceRoleName("tenanta", NamespaceRoleAdmin) returns "namespace-admin-tenanta"
func NamespaceRoleName(namespace string, roleType NamespaceRoleType) string {
	return fmt.Sprintf("namespace-%s-%s", roleType, namespace)
}

// uppercaseFirst capitalizes the first letter of a string.
// This matches how Weaviate normalizes class names.
func uppercaseFirst(s string) string {
	if len(s) == 0 {
		return s
	}
	if len(s) == 1 {
		return strings.ToUpper(s)
	}
	return strings.ToUpper(string(s[0])) + s[1:]
}

// namespaceRolePrefix is the prefix used for namespace-scoped roles
const namespaceRolePrefix = "namespace-"

// IsNamespaceRole checks if a role name is namespace-scoped (e.g., "namespace-admin-tenanta")
func IsNamespaceRole(roleName string) bool {
	return strings.HasPrefix(roleName, namespaceRolePrefix)
}

// ParseNamespaceRole extracts the role type and namespace from a namespace-scoped role name.
// For example, "namespace-admin-tenanta" returns ("admin", "tenanta", true).
// Returns ("", "", false) if the role is not a namespace-scoped role.
func ParseNamespaceRole(roleName string) (roleType, namespace string, ok bool) {
	if !IsNamespaceRole(roleName) {
		return "", "", false
	}

	// Remove "namespace-" prefix
	remainder := strings.TrimPrefix(roleName, namespaceRolePrefix)

	// The format is "type-namespace" where type is one of admin, editor, viewer
	for _, rt := range []NamespaceRoleType{NamespaceRoleAdmin, NamespaceRoleEditor, NamespaceRoleViewer} {
		prefix := string(rt) + "-"
		if strings.HasPrefix(remainder, prefix) {
			namespace = strings.TrimPrefix(remainder, prefix)
			return string(rt), namespace, true
		}
	}

	return "", "", false
}

// CleanNamespaceRoleName returns a cleaned role name for display to namespace users.
// For namespace-scoped roles belonging to the user's namespace, it returns just the role type
// (e.g., "namespace-admin-tenanta" -> "admin" when userNamespace is "tenanta").
// For other roles, it returns the original name unchanged.
func CleanNamespaceRoleName(roleName, userNamespace string) string {
	roleType, namespace, ok := ParseNamespaceRole(roleName)
	if !ok {
		return roleName
	}

	// Only clean the name if it belongs to the user's namespace
	if namespace == userNamespace {
		return roleType
	}

	return roleName
}

// RoleBelongsToNamespace checks if a role belongs to a specific namespace.
// Returns true if the role is a namespace-scoped role for the given namespace.
func RoleBelongsToNamespace(roleName, namespace string) bool {
	_, roleNamespace, ok := ParseNamespaceRole(roleName)
	if !ok {
		return false
	}
	return roleNamespace == namespace
}

// CleanPermissionCollectionName strips the namespace prefix from a collection name.
// For example, "Tenanta__Articles" -> "Articles" when namespace is "tenanta".
// Returns the original name if it doesn't have the expected namespace prefix.
func CleanPermissionCollectionName(collectionName, namespace string) string {
	if namespace == "" {
		return collectionName
	}

	// The prefix is capitalized namespace + "__"
	prefix := uppercaseFirst(namespace) + "__"

	if strings.HasPrefix(collectionName, prefix) {
		return strings.TrimPrefix(collectionName, prefix)
	}

	// Also handle wildcard patterns like "Tenanta__*"
	if collectionName == prefix+"*" {
		return "*"
	}

	return collectionName
}

// CreateNamespaceAdminPolicies creates policies for namespace admin role.
// The policies grant full CRUD permissions for all namespace-scoped domains
// (schema, data, backups, aliases, replicate) for collections that belong
// to the given namespace, plus READ permissions for users and roles domains
// (filtering by namespace is handled at the handler level).
//
// The namespace prefix is capitalized to match Weaviate's class name normalization.
// For example, namespace "tenanta" creates patterns matching "Tenanta__*" collections.
func CreateNamespaceAdminPolicies(namespace string) []authorization.Policy {
	// Capitalize first letter to match Weaviate's class name normalization
	// e.g., "tenanta" -> "Tenanta" so pattern matches "Tenanta__Articles"
	prefix := uppercaseFirst(namespace) + "__"

	return []authorization.Policy{
		// Schema domain - collection-level operations (create/delete collection)
		// Uses "#" suffix which is required for collection-level authorization
		{
			Resource: fmt.Sprintf("%s/collections/%s.*/shards/#", authorization.SchemaDomain, prefix),
			Verb:     conv.CRUD,
			Domain:   authorization.SchemaDomain,
		},
		// Schema domain - tenant/shard-level operations (create/delete tenants)
		// Uses ".*" suffix for tenant-level authorization
		{
			Resource: fmt.Sprintf("%s/collections/%s.*/shards/.*", authorization.SchemaDomain, prefix),
			Verb:     conv.CRUD,
			Domain:   authorization.SchemaDomain,
		},
		// Data domain - objects (CRUD on data/collections/{namespace__*}/shards/*/objects/*)
		{
			Resource: fmt.Sprintf("%s/collections/%s.*/shards/.*/objects/.*", authorization.DataDomain, prefix),
			Verb:     conv.CRUD,
			Domain:   authorization.DataDomain,
		},
		// Backups domain (CRUD on backups/collections/{namespace__*})
		{
			Resource: fmt.Sprintf("%s/collections/%s.*", authorization.BackupsDomain, prefix),
			Verb:     conv.CRUD,
			Domain:   authorization.BackupsDomain,
		},
		// Aliases domain (CRUD on aliases/collections/{namespace__*}/aliases/*)
		{
			Resource: fmt.Sprintf("%s/collections/%s.*/aliases/.*", authorization.AliasesDomain, prefix),
			Verb:     conv.CRUD,
			Domain:   authorization.AliasesDomain,
		},
		// Replicate domain (CRUD on replicate/collections/{namespace__*}/shards/*)
		{
			Resource: fmt.Sprintf("%s/collections/%s.*/shards/.*", authorization.ReplicateDomain, prefix),
			Verb:     conv.CRUD,
			Domain:   authorization.ReplicateDomain,
		},
		// Users domain - READ permission (handler filters to namespace users only)
		{
			Resource: fmt.Sprintf("%s/*", authorization.UsersDomain),
			Verb:     authorization.READ,
			Domain:   authorization.UsersDomain,
		},
		// Roles domain - READ permission with ALL scope (handler filters to namespace roles only)
		{
			Resource: fmt.Sprintf("%s/*", authorization.RolesDomain),
			Verb:     authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL),
			Domain:   authorization.RolesDomain,
		},
	}
}
