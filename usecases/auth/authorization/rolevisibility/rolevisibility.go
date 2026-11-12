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
// role, and whether the caller holds every permission a set of roles grants.
// Handlers share these checks so every endpoint judges a role the same way.
package rolevisibility

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzerrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
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
	var namespace string
	if namespacesEnabled {
		namespace = namespacing.ConfinedNamespace(principal)
	}
	// List endpoints run this for every role, so stop at the first policy the caller lacks.
	for i := range policies {
		if AuthorizePolicies(ctx, authorizer, principal, namespace, policies[i:i+1]) != nil {
			return false
		}
	}
	return true
}

// AuthorizeRolePermissions returns an error unless the caller holds every
// permission the roles grant. A confined caller is checked against each
// permission as projected into targetNamespace, where the roles apply.
func AuthorizeRolePermissions(ctx context.Context, authorizer authorization.Authorizer, namespacesEnabled bool, principal *models.Principal, targetNamespace string, roles map[string][]authorization.Policy) error {
	var namespace string
	if CallerConfined(namespacesEnabled, principal) {
		namespace = targetNamespace
	}
	var errs error
	for _, policies := range roles {
		errs = errors.Join(errs, AuthorizePolicies(ctx, authorizer, principal, namespace, policies))
	}
	return errs
}

// AuthorizePolicies returns an error unless the caller holds every permission
// the policies grant, projecting each resource into a non-empty namespace first.
// A segment with regex syntax beyond '.' and '*' must pass as stored and as ".*".
// A wildcard passes both, and so can a pattern that excludes names, such as "[^a]*".
func AuthorizePolicies(ctx context.Context, authorizer authorization.Authorizer, principal *models.Principal, namespace string, policies []authorization.Policy) error {
	var errs error
	for _, policy := range policies {
		// Permission-less roles carry this placeholder; it grants nothing.
		if policy.Resource == conv.InternalPlaceHolder {
			continue
		}
		resource, err := namespacing.ProjectResourceForNamespace(policy.Resource, namespace)
		if err != nil {
			// A global role can name a user in another namespace, which a confined
			// caller cannot hold. Deny without echoing the resource back to the caller.
			errs = errors.Join(errs, errors.New("role grants a permission you do not hold"))
			continue
		}
		wildcard := regexSegmentsToWildcard(resource)
		// A caller pattern such as ".." matches the text ".*" without holding
		// every name, so the stored text must pass as well.
		if wildcard != resource {
			if err := authorizePolicy(ctx, authorizer, principal, policy.Verb, resource); err != nil {
				errs = errors.Join(errs, err)
				continue
			}
		}
		errs = errors.Join(errs, authorizePolicy(ctx, authorizer, principal, policy.Verb, wildcard))
	}
	return errs
}

// regexSyntax lists the regex characters a stored pattern can carry besides
// '.' and the ".*" conv writes for '*'.
const regexSyntax = `\+?()[]{}|^$`

// regexSegmentsToWildcard replaces each segment using regexSyntax with ".*",
// keeping its "<ns>:" qualifier. casbin treats the request as literal text, so
// "(eu|admin)" would pass for a caller holding "*eu*" and reach admin.
func regexSegmentsToWildcard(resource string) string {
	if !strings.ContainsAny(resource, regexSyntax) {
		return resource
	}
	segments := strings.Split(resource, "/")
	for i, segment := range segments {
		if !strings.ContainsAny(segment, regexSyntax) {
			continue
		}
		ns := namespacing.NamespaceFromQualified(segment)
		if strings.ContainsAny(ns, regexSyntax) {
			ns = ""
		}
		segments[i] = namespacing.QualifiedName(ns, ".*")
	}
	return strings.Join(segments, "/")
}

// authorizePolicy checks each verb of the policy on its own. A denied verb of a
// union such as manage_backups is reported once under the union's action, since
// no API action names a single verb of it.
func authorizePolicy(ctx context.Context, authorizer authorization.Authorizer, principal *models.Principal, verb, resource string) error {
	verbs := grantedVerbs(verb)
	for _, v := range verbs {
		err := authorizer.AuthorizeSilent(ctx, principal, v, resource)
		if err == nil {
			continue
		}
		var forbidden authzerrors.Forbidden
		if len(verbs) == 1 || !errors.As(err, &forbidden) {
			return err
		}
		perm, permErr := conv.PathToPermission(verb, resource)
		if permErr != nil {
			return err
		}
		prefix, ok := strings.CutSuffix(err.Error(), forbidden.Error())
		if !ok {
			return err
		}
		return fmt.Errorf("%s%w", prefix, forbidden.WithVerb(*perm.Action))
	}
	return nil
}

// grantedVerbs splits a stored verb union such as "(C)|(R)|(U)|(D)_MATCH" into
// single verbs, each keeping the scope suffix. casbin's unanchored match would
// pass the whole union for a caller holding any one of its verbs.
func grantedVerbs(verb string) []string {
	union, scope, scoped := strings.Cut(verb, "_")
	verbs := strings.Split(union, "|")
	for i, v := range verbs {
		v = strings.Trim(v, "()")
		if scoped {
			v = authorization.VerbWithScope(v, scope)
		}
		verbs[i] = v
	}
	return verbs
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
