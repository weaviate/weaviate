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

// Package namespacing provides the helpers used to qualify collection, alias,
// user, and role names (and RBAC resource paths) with their owning namespace,
// and to resolve them back. The helpers are pure syntax transforms except
// ResolveRoleName, which consults a caller-supplied existence callback to fall
// back from a namespace-local to a global role.
package namespacing

import (
	"errors"
	"fmt"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
)

// ErrCreateRequiresNamespace is returned by QualifyForCreate when a global
// (or anonymous) principal attempts a create on an NS-enabled cluster.
// Call sites translate this into authzerrors.NewNamespaceForbidden — the namespacing
// package stays free of auth vocabulary.
var ErrCreateRequiresNamespace = errors.New("create requires a namespaced principal on a namespaces-enabled cluster")

// ValidateNamespacePrefix rejects user-supplied class/alias names whose
// "<namespace>:" prefix is malformed. Returns nil when name has no separator.
// kind is the noun ("class" or "alias") used in the generic error message so
// the wording matches the field the caller is validating.
//
// The error wording depends on the caller's context so namespaces stay
// invisible to principals who shouldn't know about them:
//
//   - Namespaced principal, or NS-disabled cluster: returns a generic
//     "is not a valid <kind> name" error — namespaced users should never
//     send qualified names (the resolver adds their prefix automatically),
//     and on NS-disabled clusters namespaces simply don't exist as a
//     concept.
//   - Global principal on NS-enabled cluster: returns the specific
//     "invalid namespace prefix" error — these are the operators who
//     legitimately type qualified names and benefit from an actionable
//     message about which part is wrong.
//
// Without this check, a casing variant (e.g. "Customer1:Foo" against the
// registered "customer1" namespace) sent by an operator propagates through
// QualifyClass/Resolve unchanged and the lookup hits a different key than
// the one the schema and data directory are stored under.
func ValidateNamespacePrefix(principal *models.Principal, namespacesEnabled bool, name, kind string) error {
	if !strings.Contains(name, schema.NamespaceSeparator) {
		return nil
	}
	if !namespacesEnabled || ConfinedNamespace(principal) != "" {
		return fmt.Errorf("'%s' is not a valid %s name", name, kind)
	}
	ns, _, _ := strings.Cut(name, schema.NamespaceSeparator)
	if err := schema.ValidateNamespaceNameSyntax(ns); err != nil {
		return fmt.Errorf("invalid namespace prefix in %q: %w", name, err)
	}
	return nil
}

// ShortNameMaxLength caps the raw (pre-qualification) name length for
// namespaced principals. The cap is computed from the maximum namespace
// length and the maximum class name length so it stays constant regardless
// of which namespace the caller is bound to — a `customer` user and a `c` user
// get the same limit.
const ShortNameMaxLength = schema.ClassNameMaxLength - schema.NamespaceMaxLength - len(schema.NamespaceSeparator)

// QualifyForCreate validates raw's prefix, naming kind in the rejection, and
// passes raw to q.QualifyForCreate. Call sites turn ErrCreateRequiresNamespace
// into a 403 and handle any other error from q as Qualifier describes.
func QualifyForCreate(principal *models.Principal, q Qualifier, raw, kind string) (string, error) {
	if err := ValidateNamespacePrefix(principal, q.NamespacesEnabled(), raw, kind); err != nil {
		return "", err
	}
	return q.QualifyForCreate(principal, raw)
}
