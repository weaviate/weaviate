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

// NamespaceFromQualified returns the namespace portion of a qualified name
// ("<ns>:<entity>"). Names without the separator are unnamespaced and
// return "".
func NamespaceFromQualified(name string) string {
	if ns, _, ok := strings.Cut(name, schema.NamespaceSeparator); ok {
		return ns
	}
	return ""
}

// StripQualification returns the entity portion of a qualified name
// ("<ns>:<entity>") — the substring after the first namespace separator.
// Names without the separator are returned unchanged. Used at write
// boundaries that must persist a short, namespace-portable form (e.g.
// cross-reference beacons) regardless of which form the caller submitted.
func StripQualification(name string) string {
	if _, entity, ok := strings.Cut(name, schema.NamespaceSeparator); ok {
		return entity
	}
	return name
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
//  1. Reject inputs whose "<namespace>:" prefix is not syntactically a valid
//     namespace name (lowercase letters/digits/hyphens, length bounds).
//  2. Uppercase the class portion of the input so storage lookups hit the
//     canonical case. UppercaseClassName preserves a leading "<namespace>:"
//     prefix verbatim and only touches the class portion.
//  3. If namespaces are enabled cluster-wide, qualify the (now-uppercased)
//     input with the principal's namespace. When NS is disabled the
//     qualification step is skipped.
//  4. Look the (possibly qualified) name up as an alias via the existing
//     in-memory resolver; if it matches an alias, return the alias target.
//
// Returns (class, qualifiedAlias, err). qualifiedAlias is the
// namespace-prefixed alias used for lookup when an alias was hit (raw for
// global principals), "" otherwise — used by the objects layer to preserve
// existing alias-aware flows.
func Resolve(principal *models.Principal, sm SchemaManager, namespacesEnabled bool, name string) (class, qualifiedAlias string, err error) {
	if err := ValidateNamespacePrefix(principal, namespacesEnabled, name, "class"); err != nil {
		return "", "", err
	}
	qualified := schema.UppercaseClassName(name)
	if namespacesEnabled {
		qualified = qualify(principal, qualified)
	}

	// Check if the qualified name is an alias
	if resolvedClass := sm.ResolveAlias(qualified); resolvedClass != "" {
		return resolvedClass, qualified, nil
	}

	// Not an alias, return the qualified name
	return qualified, "", nil
}

// QualifyClass uppercases the class portion of name and prepends the
// principal's namespace when namespaces are enabled. Aliases are not
// resolved. Rejects user-supplied names whose "<namespace>:" prefix is not
// syntactically valid.
func QualifyClass(principal *models.Principal, namespacesEnabled bool, name string) (string, error) {
	if err := ValidateNamespacePrefix(principal, namespacesEnabled, name, "class"); err != nil {
		return "", err
	}
	qualified := schema.UppercaseClassName(name)
	if namespacesEnabled {
		qualified = qualify(principal, qualified)
	}
	return qualified, nil
}

// QualifyPropertyDataTypes auto-qualifies cross-reference DataType class names
// with the principal's namespace on namespaces-enabled clusters. Primitive and
// nested-object DataTypes pass through unchanged. Already-qualified entries
// (containing the namespace separator) sent by a namespaced principal are
// rejected via ValidateNamespacePrefix — symmetric with QualifyClass.
//
// Mutates properties[i].DataType slices in place. No-op when namespaces are
// disabled or the principal has no namespace (global principals / NS-disabled
// clusters). Callers that construct *models.Property programmatically must
// pass short DataType names; an already-qualified value will be rejected.
//
// Scope: top-level properties only. NestedProperty.DataType cross-refs are
// rejected at usecases/schema/validation.go and never reach this path.
func QualifyPropertyDataTypes(
	principal *models.Principal,
	namespacesEnabled bool,
	properties []*models.Property,
) error {
	if !namespacesEnabled || principal == nil || principal.Namespace == "" {
		return nil
	}
	for _, p := range properties {
		if p == nil || len(p.DataType) == 0 {
			continue
		}
		if _, ok := schema.AsPrimitive(p.DataType); ok {
			continue
		}
		if _, ok := schema.AsNested(p.DataType); ok {
			continue
		}
		for i, dt := range p.DataType {
			if dt == "" {
				continue
			}
			if err := ValidateNamespacePrefix(principal, namespacesEnabled, dt, "class"); err != nil {
				return err
			}
			p.DataType[i] = QualifiedName(principal.Namespace, dt)
		}
	}
	return nil
}

// StripOwnNamespace removes the principal's own namespace prefix from name
// when present. Returns name unchanged when principal.Namespace is empty
// (global principal; also the case on NS-disabled clusters, where principals
// never carry a namespace) or when the prefix does not match (foreign prefix
// or short input). A foreign prefix is left intact so downstream
// ValidateClassName fails closed on the embedded ":".
func StripOwnNamespace(principal *models.Principal, name string) string {
	if principal == nil || principal.Namespace == "" {
		return name
	}
	return strings.TrimPrefix(name, principal.Namespace+schema.NamespaceSeparator)
}

// StripClassResponse returns a shallow copy of src with the top-level Class
// name and every property/nested-property DataType entry stripped of the
// principal's own namespace prefix. Returns src unchanged when the principal
// has no namespace, so global callers (and NS-disabled clusters) get a
// pass-through. The input is never mutated: callers can safely pass cached
// schema pointers without affecting concurrent readers.
func StripClassResponse(principal *models.Principal, src *models.Class) *models.Class {
	if src == nil || principal == nil || principal.Namespace == "" {
		return src
	}
	out := *src
	out.Class = StripOwnNamespace(principal, src.Class)
	if len(src.Properties) > 0 {
		out.Properties = make([]*models.Property, len(src.Properties))
		for i, p := range src.Properties {
			out.Properties[i] = StripPropertyResponse(principal, p)
		}
	}
	return &out
}

// StripPropertyResponse returns a shallow copy of src with every DataType
// entry and nested-property DataType entry stripped of the principal's own
// namespace prefix. Primitive types (text, int, …) never carry a namespace
// prefix, so StripOwnNamespace is a no-op on them. The input is never mutated.
func StripPropertyResponse(principal *models.Principal, src *models.Property) *models.Property {
	if src == nil || principal == nil || principal.Namespace == "" {
		return src
	}
	out := *src
	out.DataType = stripDataTypes(principal, src.DataType)
	if len(src.NestedProperties) > 0 {
		out.NestedProperties = make([]*models.NestedProperty, len(src.NestedProperties))
		for i, np := range src.NestedProperties {
			out.NestedProperties[i] = stripNestedPropertyResponse(principal, np)
		}
	}
	return &out
}

func stripNestedPropertyResponse(principal *models.Principal, src *models.NestedProperty) *models.NestedProperty {
	if src == nil {
		return nil
	}
	out := *src
	out.DataType = stripDataTypes(principal, src.DataType)
	if len(src.NestedProperties) > 0 {
		out.NestedProperties = make([]*models.NestedProperty, len(src.NestedProperties))
		for i, np := range src.NestedProperties {
			out.NestedProperties[i] = stripNestedPropertyResponse(principal, np)
		}
	}
	return &out
}

func stripDataTypes(principal *models.Principal, src []string) []string {
	if len(src) == 0 {
		return src
	}
	out := make([]string, len(src))
	for i, dt := range src {
		out[i] = StripOwnNamespace(principal, dt)
	}
	return out
}

// StripAliasResponse returns a shallow copy of src with both Alias and Class
// stripped of the principal's own namespace prefix. The input is never
// mutated: callers can safely pass cached schema pointers without affecting
// concurrent readers.
func StripAliasResponse(principal *models.Principal, src *models.Alias) *models.Alias {
	if src == nil || principal == nil || principal.Namespace == "" {
		return src
	}
	out := *src
	out.Alias = StripOwnNamespace(principal, src.Alias)
	out.Class = StripOwnNamespace(principal, src.Class)
	return &out
}

// StripObjectResponseClass mutates obj.Class in place to remove the
// principal's own namespace prefix. Objects flow per-request from the
// objects manager — there are no shared pointers to protect — so in-place
// mutation is safe.
func StripObjectResponseClass(principal *models.Principal, obj *models.Object) {
	if obj == nil || principal == nil || principal.Namespace == "" {
		return
	}
	obj.Class = StripOwnNamespace(principal, obj.Class)
}

// StripErrorMessage removes every occurrence of the principal's own
// "<namespace>:" prefix from msg. Returns msg unchanged when principal is
// nil or has no namespace.
func StripErrorMessage(principal *models.Principal, msg string) string {
	if principal == nil || principal.Namespace == "" {
		return msg
	}
	return strings.ReplaceAll(msg, principal.Namespace+schema.NamespaceSeparator, "")
}

// StripErrForPrincipal returns an error whose Error() has the principal's
// own namespace prefix removed. The original error is held via Unwrap so
// errors.As keeps matching. Returns err unchanged when nothing was stripped.
func StripErrForPrincipal(principal *models.Principal, err error) error {
	if err == nil {
		return nil
	}
	msg := err.Error()
	stripped := StripErrorMessage(principal, msg)
	if stripped == msg {
		return err
	}
	return &strippedErr{msg: stripped, orig: err}
}

// strippedErr overrides Error() while keeping the original reachable via Unwrap.
type strippedErr struct {
	msg  string
	orig error
}

func (e *strippedErr) Error() string { return e.msg }
func (e *strippedErr) Unwrap() error { return e.orig }
