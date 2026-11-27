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

package conv

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// keyMatch5BraceRe mirrors casbin's KeyMatch5, which rewrites a "{...}" token to
// "[^/]+" on the stored pattern before compiling it.
var keyMatch5BraceRe = regexp.MustCompile(`\{[^/]+\}`)

// MaxTargetLength bounds a free-form permission target so it can't bloat the
// policy store. It matches the longest name limit (class names, 255).
const MaxTargetLength = 256

// validateRegexTarget rejects a permission target that would not survive being
// stored as a casbin resource pattern: it is over-long, contains a '/' (which
// shifts the segment boundaries used on read-back), or does not compile as a
// regex. It compiles the target with the '*' wildcard expanded and KeyMatch5's
// "{...}" rewrite applied, so constructs the rewrite would break (e.g. "\p{L}",
// "\x{263a}") are caught here, not at enforce.
func validateRegexTarget(field, value string) error {
	if len(value) > MaxTargetLength {
		return fmt.Errorf("%s exceeds the maximum length of %d characters", field, MaxTargetLength)
	}
	if strings.Contains(value, "/") {
		return fmt.Errorf("%s '%s' must not contain '/'", field, value)
	}
	expanded := keyMatch5BraceRe.ReplaceAllString(strings.ReplaceAll(value, "*", ".*"), "[^/]+")
	if _, err := regexp.Compile(expanded); err != nil {
		return fmt.Errorf("%s '%s' is not a valid pattern: %w", field, value, err)
	}
	return nil
}

// PermissionCheck is the request a permission list comes from, which decides how
// strictly ValidatePermissions checks it.
type PermissionCheck int

const (
	CheckCreate PermissionCheck = iota // create accepts an empty list and checks the stored form
	CheckAdd                           // add needs one or more and checks the stored form
	CheckLookup                        // remove and has-permission accept rows already stored
)

// ValidatePermissions rejects a permission whose targets are over-long, contain a
// '/' or do not compile as patterns, or whose collection, alias or tenant names
// break their naming rule. It checks each target alone, so a caller that stores
// the permission must also run ValidateStorablePolicies on its policies.
func ValidatePermissions(namespacesEnabled bool, check PermissionCheck, permissions ...*models.Permission) error {
	if check != CheckCreate && len(permissions) == 0 {
		return fmt.Errorf("role has to have at least 1 permission")
	}

	for _, perm := range permissions {
		var multiErr error
		add := func(err error) {
			if err != nil {
				multiErr = errors.Join(multiErr, err)
			}
		}
		// Uppercased first, because conv stores the target uppercased and that is the
		// form the matcher sees. Only the first character is uppercased, so in an
		// alternation the later branches keep their case: "movies|books" is stored as
		// "Movies|books" and never matches "Books".
		className := func(name string) {
			name = schema.UppercaseClassName(name)
			err := validatePermissionClassName(namespacesEnabled, name)
			add(err)
			add(validateRegexTarget("collection", name))
		}
		tenantName := func(name string) {
			add(schema.ValidateTenantNameIncludesRegex(name))
			add(validateRegexTarget("tenant", name))
		}
		// aliasName checks the pattern as conv stores it, uppercased, against the
		// class-name rule. That refuses "ålias.*", which matches no alias, and
		// "[[:alpha:]]", stored as "[[:Alpha:]]", which does not compile. CheckLookup
		// skips the rule so remove can drop such stored rows.
		aliasName := func(name string) {
			if check == CheckLookup {
				return
			}
			if validatePermissionClassName(namespacesEnabled, schema.UppercaseClassName(name)) != nil {
				add(fmt.Errorf("'%s' is not a valid alias name", name))
			}
		}

		if p := perm.Collections; p != nil && p.Collection != nil {
			className(*p.Collection)
		}
		if p := perm.Tenants; p != nil {
			if p.Collection != nil {
				className(*p.Collection)
			}
			if p.Tenant != nil {
				tenantName(*p.Tenant)
			}
		}
		if p := perm.Data; p != nil {
			if p.Collection != nil {
				className(*p.Collection)
			}
			if p.Tenant != nil {
				tenantName(*p.Tenant)
			}
			// Data.Object is deprecated and ignored: the data resource wildcards its
			// objects segment, so the value never becomes a casbin pattern and there is
			// nothing left to validate. A value an old client still sends is a no-op.
		}
		if p := perm.Backups; p != nil && p.Collection != nil {
			className(*p.Collection)
		}
		if p := perm.Nodes; p != nil && p.Collection != nil {
			className(*p.Collection)
		}
		if p := perm.Replicate; p != nil {
			if p.Collection != nil {
				className(*p.Collection)
			}
			if p.Shard != nil {
				add(validateRegexTarget("shard", *p.Shard))
			}
		}
		if p := perm.Users; p != nil && p.Users != nil {
			add(validateRegexTarget("users", *p.Users))
		}
		if p := perm.Groups; p != nil && p.Group != nil {
			add(validateRegexTarget("group", *p.Group))
		}
		if p := perm.Roles; p != nil && p.Role != nil {
			add(validateRegexTarget("role", *p.Role))
		}
		if p := perm.Aliases; p != nil {
			if p.Collection != nil {
				className(*p.Collection)
			}
			if p.Alias != nil {
				add(validateRegexTarget("alias", *p.Alias))
				aliasName(*p.Alias)
			}
		}
		if p := perm.Namespaces; p != nil && p.Namespace != nil {
			add(validateRegexTarget("namespace", *p.Namespace))
		}

		if multiErr != nil {
			return multiErr
		}
	}

	return nil
}

// ValidateStorablePolicies rejects a permission whose resource would corrupt the
// policy file. It skips the length check because a resource joins several
// targets, and ValidatePermissions already caps each one at MaxTargetLength.
// It also compiles each resource as KeyMatch5 does, since RolesToPolicies stores
// a target such as "X|\Q" as "(X|\Q)", and there "\Q" quotes the closing ')'.
func ValidateStorablePolicies(policies []authorization.Policy) error {
	for _, p := range policies {
		if err := ValidateStorableCharacters(p.Resource); err != nil {
			return fmt.Errorf("permission %q %w", p.Resource, err)
		}
		pattern := "^" + keyMatch5BraceRe.ReplaceAllString(strings.ReplaceAll(p.Resource, "/*", "/.*"), "[^/]+") + "$"
		if _, err := regexp.Compile(pattern); err != nil {
			return fmt.Errorf("permission %q is not a valid pattern: %w", p.Resource, err)
		}
	}
	return nil
}

// validatePermissionClassName validates a class-name field in a permission. On
// namespace-enabled clusters it tolerates an optional "<namespace>:" qualifier so
// a global operator can check a namespace-local role's qualified rows; whether a
// given caller may submit a qualified name is enforced separately by
// ValidateNoQualifiedNamespaceInPolicies. The class part always follows the
// regular permission class-name rules.
func validatePermissionClassName(namespacesEnabled bool, name string) error {
	if namespacesEnabled {
		if ns, cls, ok := strings.Cut(name, schema.NamespaceSeparator); ok {
			if err := schema.ValidateNamespaceNameSyntax(ns); err != nil {
				return fmt.Errorf("'%s' is not a valid class name", name)
			}
			name = cls
		}
	}
	_, err := schema.ValidateClassNameIncludesRegex(name)
	return err
}

// ValidateNoQualifiedNamespaceInPolicies rejects role-definition policies that
// carry an explicit namespace qualification. On namespace-enabled clusters role
// definitions must stay namespace-relative templates; the matcher specializes
// them at enforce time. No-op when namespaces are disabled.
//
// A ':' in a users/<id> or groups/<...> id is part of the opaque id (e.g. an
// OIDC username), not a qualifier, so a global caller may use it. Namespaced
// callers must still submit short, unqualified forms — symmetric with the
// class-name rule that forbids them typing any "<namespace>:" prefix. So a
// namespaced caller cannot express a colon-bearing group id; acceptable while
// group grants are global-only. Revisit if namespaced callers gain them.
//
// allowGlobalQualified lifts the restriction for a global caller, used by the
// hasPermission read path so an operator can name a namespace-local role's
// qualified rows. Confined callers stay restricted.
func ValidateNoQualifiedNamespaceInPolicies(namespacesEnabled bool, principal *models.Principal, policies []authorization.Policy, allowGlobalQualified bool) error {
	if !namespacesEnabled {
		return nil
	}
	global := namespacing.ConfinedNamespace(principal) == ""
	if global && allowGlobalQualified {
		return nil
	}
	for _, p := range policies {
		if !ContainsNamespaceSeparator(p.Resource) {
			continue
		}
		if global && IsOpaqueIDResource(p.Resource) {
			continue
		}
		return fmt.Errorf("role permissions must not contain namespace-qualified resource paths; got %q", p.Resource)
	}
	return nil
}

// HasAllScopedRolePermission reports whether any policy grants role management
// at ALL scope (verb-suffix _ALL on a roles/ resource). On NS clusters such a
// permission cannot be stored in any role — cluster-wide role management exists
// only as root's wildcard, never as an assignable permission.
func HasAllScopedRolePermission(policies []authorization.Policy) bool {
	for _, p := range policies {
		if strings.HasPrefix(p.Resource, authorization.RolesDomain+"/") &&
			strings.HasSuffix(p.Verb, "_"+authorization.ROLE_SCOPE_ALL) {
			return true
		}
	}
	return false
}
