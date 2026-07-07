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

// Package rolevisibility decides whether a caller may see another subject's
// assigned role, given the role's stored name and policies. The rules are shared
// by every handler that lists a subject's roles so a role is hidden or revealed
// identically regardless of which endpoint returns it.
package rolevisibility

import (
	"context"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// CallerConfined reports whether the principal is a namespace-confined caller —
// its RBAC reads and writes scope to its own namespace. False for global callers
// and on NS-disabled clusters.
func CallerConfined(namespacesEnabled bool, principal *models.Principal) bool {
	return namespacesEnabled && namespacing.ConfinedNamespace(principal) != ""
}

// RoleHiddenFromCaller reports whether storedRoleName belongs to a namespace
// other than the caller's, so its very existence must not be revealed.
// Own-namespace and global roles are visible; only foreign-namespace roles are
// hidden. Global operators (and NS-disabled clusters) hide nothing.
func RoleHiddenFromCaller(namespacesEnabled bool, principal *models.Principal, storedRoleName string) bool {
	if !CallerConfined(namespacesEnabled, principal) {
		return false
	}
	ns := namespacing.NamespaceFromQualified(storedRoleName)
	return ns != "" && ns != principal.Namespace
}

// RoleOperatorReservedFromCaller hides an operator-reserved global role from any
// caller that is not a global operator. Additive to the permission-content gate:
// it can only hide more, never expose a role that gate would block.
func RoleOperatorReservedFromCaller(namespacesEnabled bool, principal *models.Principal, storedRoleName string) bool {
	if principal == nil {
		return false
	}
	// Operator status is IsGlobalOperator, not an empty namespace: a namespace-less
	// non-operator must still have reserved roles hidden.
	if !namespacesEnabled || principal.IsGlobalOperator {
		return false
	}
	return authorization.IsOperatorReservedRoleName(namespacing.StripQualification(storedRoleName))
}

// RolePoliciesVisibleToPrincipal reports whether the caller may see a role with
// the given policies: it holds ALL-scope on roles, or already holds every
// permission the role grants (projected into its own namespace when confined).
func RolePoliciesVisibleToPrincipal(ctx context.Context, authorizer authorization.Authorizer, namespacesEnabled bool, principal *models.Principal, policies []authorization.Policy) bool {
	if err := authorizer.Authorize(ctx, principal, authorization.VerbWithScope(authorization.READ, authorization.ROLE_SCOPE_ALL), authorization.Roles()...); err == nil {
		return true
	}
	namespaced := CallerConfined(namespacesEnabled, principal)
	for _, policy := range policies {
		// Permission-less roles carry this placeholder; it grants nothing.
		if policy.Resource == conv.InternalPlaceHolder {
			continue
		}
		resource := policy.Resource
		if namespaced {
			projected, err := namespacing.ProjectResourceForNamespace(policy.Resource, principal.Namespace)
			if err != nil {
				return false
			}
			resource = projected
		}
		if err := authorizer.AuthorizeSilent(ctx, principal, policy.Verb, resource); err != nil {
			return false
		}
	}
	return true
}

// RoleVisibleToCaller reports whether the caller may see another subject's role
// given its stored name and policies. A role is hidden when it belongs to another
// namespace, when it is an operator-reserved global role and the caller is not a
// global operator, or when the caller does not already hold every permission it
// grants. A caller's own roles are always visible, so callers skip this check for
// a self-read.
func RoleVisibleToCaller(ctx context.Context, authorizer authorization.Authorizer, namespacesEnabled bool, principal *models.Principal, storedRoleName string, policies []authorization.Policy) bool {
	if RoleHiddenFromCaller(namespacesEnabled, principal, storedRoleName) {
		return false
	}
	if RoleOperatorReservedFromCaller(namespacesEnabled, principal, storedRoleName) {
		return false
	}
	if namespacesEnabled && !RolePoliciesVisibleToPrincipal(ctx, authorizer, namespacesEnabled, principal, policies) {
		return false
	}
	return true
}
