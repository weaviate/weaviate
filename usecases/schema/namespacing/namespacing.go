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

// Package namespacing provides the syntax-only helpers used to qualify
// collection and alias names with their owning namespace. The helpers are
// pure functions: no I/O, no validation beyond a length check on the
// pre-qualification short name.
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
	if !namespacesEnabled || (principal != nil && principal.Namespace != "") {
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

// QualifyForCreate is the create-path entry point: it enforces the NS-enabled
// policy, length-checks the raw short name, and returns the qualified form.
// On NS-enabled clusters, callers without a namespace are rejected with
// ErrCreateRequiresNamespace; the call site is responsible for translating
// that into a 403 with the appropriate verb and resources. kind is the noun
// ("class" or "alias") used in the prefix-rejection error so the wording
// matches the field the caller is validating.
func QualifyForCreate(principal *models.Principal, namespacesEnabled bool, raw, kind string) (string, error) {
	if err := ValidateNamespacePrefix(principal, namespacesEnabled, raw, kind); err != nil {
		return "", err
	}
	if !namespacesEnabled {
		return raw, nil
	}
	if principal == nil || principal.Namespace == "" {
		return "", ErrCreateRequiresNamespace
	}
	if len(raw) > ShortNameMaxLength {
		return "", fmt.Errorf("'%s' is too long: namespaced names must be at most %d characters before qualification", raw, ShortNameMaxLength)
	}
	return qualify(principal, raw), nil
}
