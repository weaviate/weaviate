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
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// SchemaManager is a single-method interface exposing alias resolution.
// It allows the resolver to look up aliases without depending on the full
// schema reader.
type SchemaManager interface {
	ResolveAlias(alias string) string
}

// qualify prepends principal.Namespace to name.
func qualify(principal *models.Principal, name string) string {
	if principal == nil || principal.Namespace == "" {
		return name
	}
	return principal.Namespace + schema.NamespaceSeparator + name
}

// Resolve is the read-side entry point used everywhere a user-supplied
// class/alias name needs to become an internal class name:
//
//  1. If namespaces are enabled cluster-wide, qualify the input with the
//     principal's namespace. When NS is disabled the qualification step is
//     skipped — a stray principal.Namespace on an NS-disabled cluster is
//     silently ignored here; rejecting that state is an upstream invariant
//     enforced by startup gating, DB-user create, and OIDC classification.
//  2. Look the (possibly qualified) name up as an alias via the existing
//     in-memory resolver; if it matches an alias, return the alias target.
//
// Returns (class, originalAlias). originalAlias is the qualified alias name
// used for lookup when an alias was hit (i.e. namespace-prefixed for
// namespaced principals, raw for global principals), "" otherwise — used by
// the objects layer to preserve existing alias-aware flows. Sites that do
// not need this can ignore the second return value.
func Resolve(principal *models.Principal, sm SchemaManager, nsEnabled bool, name string) (class, originalAlias string) {
	qualified := name
	if nsEnabled {
		qualified = qualify(principal, name)
	}

	// Check if the qualified name is an alias
	if resolvedClass := sm.ResolveAlias(qualified); resolvedClass != "" {
		return resolvedClass, qualified
	}

	// Not an alias, return the qualified name
	return qualified, ""
}
