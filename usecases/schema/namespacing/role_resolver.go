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
	"errors"
	"fmt"
	"iter"
	"strings"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// ErrRoleNotFound is returned by ResolveRoleName when no local or global role
// matches; call sites translate it into a 404.
var ErrRoleNotFound = errors.New("role not found")

// QualifyRoleNameForCreate returns the stored name for a freshly created role.
// A namespaced caller creates a local role auto-prefixed with its namespace; a
// global caller creates an unprefixed global role. Neither may carry a ':' in
// the raw name. NS-disabled: passthrough.
func QualifyRoleNameForCreate(principal *models.Principal, namespacesEnabled bool, raw string) (string, error) {
	if !namespacesEnabled {
		return raw, nil
	}
	if strings.Contains(raw, schema.NamespaceSeparator) {
		return "", fmt.Errorf("'%s' is not a valid role name", raw)
	}
	if principal == nil || principal.Namespace == "" {
		return raw, nil
	}
	return QualifiedName(principal.Namespace, raw), nil
}

// RoleExistsFunc reports whether a role with the given stored name exists.
type RoleExistsFunc func(storedName string) (bool, error)

// ResolveRoleName maps a caller-supplied role name to its stored name for
// get/update/delete/assign/revoke/list. A namespaced caller's bare name
// resolves to its local role first, then falls back to a global role of the
// same short name (':' input is rejected). A global caller's name is returned
// as-is with no existence check — a missing-role 404 is the caller's job.
// NS-disabled: passthrough. isLocal reports a namespace-qualified result.
func ResolveRoleName(principal *models.Principal, namespacesEnabled bool, raw string, exists RoleExistsFunc) (stored string, isLocal bool, err error) {
	if !namespacesEnabled {
		return raw, false, nil
	}
	qualified := strings.Contains(raw, schema.NamespaceSeparator)
	if principal == nil || principal.Namespace == "" {
		return raw, qualified, nil
	}
	if qualified {
		return "", false, fmt.Errorf("'%s' is not a valid role name", raw)
	}
	local := QualifiedName(principal.Namespace, raw)
	switch ok, err := exists(local); {
	case err != nil:
		return "", false, err
	case ok:
		return local, true, nil
	}
	switch ok, err := exists(raw); {
	case err != nil:
		return "", false, err
	case ok:
		return raw, false, nil
	}
	return "", false, ErrRoleNotFound
}

// QualifyRolePoliciesForCreate auto-prefixes the namespace-bearing segments of a
// namespaced caller's role permissions (collection/alias/user/role names and
// `*`) with its namespace, in place. An already-':'-qualified segment is
// rejected. No-op for global principals and NS-disabled clusters.
func QualifyRolePoliciesForCreate(principal *models.Principal, namespacesEnabled bool, policies []authorization.Policy) error {
	if !namespacesEnabled || principal == nil || principal.Namespace == "" {
		return nil
	}
	prefix := principal.Namespace + schema.NamespaceSeparator
	for i := range policies {
		qualified, err := qualifyResourceForCreate(prefix, policies[i].Resource)
		if err != nil {
			return err
		}
		policies[i].Resource = qualified
	}
	return nil
}

// qualifyResourceForCreate prefixes the namespace-bearing segment(s) of a
// single policy resource. Resources that are not namespaceable pass through.
func qualifyResourceForCreate(prefix, resource string) (string, error) {
	start, end, hasAlias := FindNamespaceSegments(resource)
	if end == 0 {
		return resource, nil
	}
	var b strings.Builder
	b.Grow(len(resource) + 2*len(prefix))
	b.WriteString(resource[:start])
	if err := writeQualifiedSegment(&b, prefix, resource, start, end); err != nil {
		return "", err
	}
	if !hasAlias {
		b.WriteString(resource[end:])
		return b.String(), nil
	}
	aliasStart := end + len(AliasesMidSeg)
	aliasEnd := len(resource)
	b.WriteString(resource[end:aliasStart])
	if err := writeQualifiedSegment(&b, prefix, resource, aliasStart, aliasEnd); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeQualifiedSegment(b *strings.Builder, prefix, resource string, start, end int) error {
	if SegmentHasSeparator(resource, start, end) {
		return fmt.Errorf("'%s' is not a valid name", resource[start:end])
	}
	b.WriteString(prefix)
	b.WriteString(resource[start:end])
	return nil
}

// ProjectResourceForNamespace specializes a role policy resource to a target
// namespace for an assignment ≤-effective check, e.g. for namespace "customer1":
//
//	data/collections/Movies/...            -> data/collections/customer1:Movies/...  (bare segment prefixed)
//	data/collections/customer1:Movies/...  -> unchanged                              (already names the target)
//	data/collections/customer2:Movies/...  -> error                                  (bound to another namespace)
//	cluster/*                              -> unchanged                              (not namespaceable)
//
// An empty namespace passes through.
func ProjectResourceForNamespace(resource, namespace string) (string, error) {
	if namespace == "" {
		return resource, nil
	}
	start, end, hasAlias := FindNamespaceSegments(resource)
	if end == 0 {
		return resource, nil
	}
	prefix := namespace + schema.NamespaceSeparator
	var b strings.Builder
	b.Grow(len(resource) + 2*len(prefix))
	b.WriteString(resource[:start])
	if err := writeProjectedSegment(&b, prefix, namespace, resource, start, end); err != nil {
		return "", err
	}
	if !hasAlias {
		b.WriteString(resource[end:])
		return b.String(), nil
	}
	aliasStart := end + len(AliasesMidSeg)
	aliasEnd := len(resource)
	b.WriteString(resource[end:aliasStart])
	if err := writeProjectedSegment(&b, prefix, namespace, resource, aliasStart, aliasEnd); err != nil {
		return "", err
	}
	return b.String(), nil
}

func writeProjectedSegment(b *strings.Builder, prefix, namespace, resource string, start, end int) error {
	if SegmentHasSeparator(resource, start, end) {
		if NamespaceFromQualified(resource[start:end]) != namespace {
			return fmt.Errorf("role permission %q targets a different namespace", resource)
		}
		b.WriteString(resource[start:end])
		return nil
	}
	b.WriteString(prefix)
	b.WriteString(resource[start:end])
	return nil
}

// RoleShortNameConflict describes why a candidate role name collides with an
// existing role under the rule that role short names are unique per namespace
// and a global name reserves its short name across every namespace.
type RoleShortNameConflict string

const (
	NoRoleConflict        RoleShortNameConflict = ""
	RoleConflictGlobal    RoleShortNameConflict = "global"    // candidate is local; a global role of the same short name exists
	RoleConflictLocal     RoleShortNameConflict = "local"     // candidate is global; a namespace-local role of the same short name exists
	RoleConflictDuplicate RoleShortNameConflict = "duplicate" // a role with the exact same stored name exists
)

// FindShortNameConflict reports whether candidateFull collides with an existing
// stored role name: a local `<ns>:R` clashes with a global `R` or an identical
// `<ns>:R`; a global `R` clashes with any `<other>:R` or an identical `R`. The
// same short name in different namespaces coexists.
func FindShortNameConflict(existing iter.Seq[string], candidateFull string) RoleShortNameConflict {
	candNS := NamespaceFromQualified(candidateFull)
	candShort := StripQualification(candidateFull)
	for e := range existing {
		if e == candidateFull {
			return RoleConflictDuplicate
		}
		if StripQualification(e) != candShort {
			continue
		}
		eNS := NamespaceFromQualified(e)
		if candNS == "" && eNS != "" {
			return RoleConflictLocal
		}
		if candNS != "" && eNS == "" {
			return RoleConflictGlobal
		}
	}
	return NoRoleConflict
}
