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

import (
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// SchemaManager is a single-method interface exposing alias resolution.
// It allows the resolver to look up aliases without depending on the full
// schema reader.
type SchemaManager interface {
	ResolveAlias(alias string) string
}

// QualifiedName joins a namespace and a name with NamespaceSeparator. If
// namespace is empty, name is returned unchanged. Used to qualify class
// names, alias names, DB user IDs, and OIDC usernames — anything keyed by
// "<namespace>:<entity>" on namespace-enabled clusters.
func QualifiedName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + schema.NamespaceSeparator + name
}

// qualify prepends principal.Namespace to name.
func qualify(principal *models.Principal, name string) string {
	if principal == nil {
		return name
	}
	return QualifiedName(principal.Namespace, name)
}

// Resolve is the read-side entry point used everywhere a user-supplied
// class/alias name needs to become an internal class name:
//
//  1. Uppercase the class portion of the input so storage lookups hit the
//     canonical case. UppercaseClassName preserves a leading "<namespace>:"
//     prefix verbatim and only touches the class portion.
//  2. If namespaces are enabled cluster-wide, qualify the (now-uppercased)
//     input with the principal's namespace. When NS is disabled the
//     qualification step is skipped.
//  3. Look the (possibly qualified) name up as an alias via the existing
//     in-memory resolver; if it matches an alias, return the alias target.
//
// Returns (class, originalAlias). originalAlias is the qualified alias name
// used for lookup when an alias was hit (i.e. namespace-prefixed for
// namespaced principals, raw for global principals), "" otherwise — used by
// the objects layer to preserve existing alias-aware flows. Sites that do
// not need this can ignore the second return value.
func Resolve(principal *models.Principal, sm SchemaManager, nsEnabled bool, name string) (class, originalAlias string) {
	qualified := schema.UppercaseClassName(name)
	if nsEnabled {
		qualified = qualify(principal, qualified)
	}

	// Check if the qualified name is an alias
	if resolvedClass := sm.ResolveAlias(qualified); resolvedClass != "" {
		return resolvedClass, qualified
	}

	// Not an alias, return the qualified name
	return qualified, ""
}

// QualifyClass uppercases the class portion of name and prepends the
// principal's namespace when namespaces are enabled. Aliases are not
// resolved.
func QualifyClass(principal *models.Principal, nsEnabled bool, name string) string {
	qualified := schema.UppercaseClassName(name)
	if nsEnabled {
		qualified = qualify(principal, qualified)
	}
	return qualified
}

// StripOwnNS removes the principal's own namespace prefix from name when
// present. Returns name unchanged when principal.Namespace is empty (global
// principal; also the case on NS-disabled clusters, where principals never
// carry a namespace) or when the prefix does not match (foreign prefix or
// short input). A foreign prefix is left intact so downstream
// ValidateClassName fails closed on the embedded ":".
func StripOwnNS(principal *models.Principal, name string) string {
	if principal == nil || principal.Namespace == "" {
		return name
	}
	return strings.TrimPrefix(name, principal.Namespace+schema.NamespaceSeparator)
}
