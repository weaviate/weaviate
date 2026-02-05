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

// CreateNamespaceAdminPolicies creates policies for namespace admin role.
// The policies grant full CRUD permissions for all namespace-scoped domains
// (schema, data, backups, aliases, replicate) for collections that belong
// to the given namespace.
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
	}
}
