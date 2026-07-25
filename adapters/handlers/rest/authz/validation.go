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

package authz

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// keyMatch5BraceRe mirrors casbin's KeyMatch5, which rewrites a "{...}" token to
// "[^/]+" on the stored pattern before compiling it.
var keyMatch5BraceRe = regexp.MustCompile(`\{[^/]+\}`)

// maxTargetLength bounds a free-form permission target so it can't bloat the
// policy store. It matches the longest name limit (class names, 255).
const maxTargetLength = 256

// validateRegexTarget rejects a permission target that would not survive being
// stored as a casbin resource pattern: it is over-long, contains a '/' (which
// shifts the segment boundaries used on read-back), or does not compile as a
// regex (which panics the matcher at enforce time). It reproduces exactly what
// the matcher compiles — the '*' wildcard expanded and KeyMatch5's "{...}"
// rewrite applied — so constructs the rewrite would break (e.g. "\p{L}",
// "\x{263a}") are caught here, not at enforce.
func validateRegexTarget(field, value string) error {
	if len(value) > maxTargetLength {
		return fmt.Errorf("%s exceeds the maximum length of %d characters", field, maxTargetLength)
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

func validatePermissions(namespacesEnabled, allowEmpty bool, permissions ...*models.Permission) error {
	if !allowEmpty && len(permissions) == 0 {
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
			if p.Object != nil {
				add(validateRegexTarget("object", *p.Object))
			}
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

// validatePermissionClassName validates a class-name field in a permission. On
// namespace-enabled clusters it tolerates an optional "<namespace>:" qualifier so
// a global operator can check a namespace-local role's qualified rows; whether a
// given caller may submit a qualified name is enforced separately by
// validateNoQualifiedNamespaceInPolicies. The class part always follows the
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
