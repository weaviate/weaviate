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

package rbac

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// stripRBACSnapshot rewrites a namespaced RBAC snapshot for restore into a
// namespace-disabled cluster (graduation): every "<namespace>:" qualifier whose
// namespace appears in the snapshot's own role names is dropped. casbin merges
// colliding rows silently on Restore, so a strip that would fuse two namespaces'
// roles or subjects (or overwrite a built-in) is rejected here rather than
// applied. Returns the rewritten snapshot, or an aggregated collision error.
func stripRBACSnapshot(s snapshot) (snapshot, error) {
	// Pass 1: derive the namespace set N from role names only. A role name's
	// "<ns>:" prefix is unambiguously a namespace (roles are qualified at
	// create), unlike an OIDC subject or user-resource whose ':' may be
	// intrinsic, so N, not a blind first-segment split, decides what strips.
	namespaces := map[string]struct{}{}
	addNamespace := func(roleKey string) {
		if ns := namespacing.NamespaceFromQualified(conv.TrimRoleNamePrefix(roleKey)); ns != "" {
			namespaces[ns] = struct{}{}
		}
	}
	for _, p := range s.Policy {
		if len(p) > 0 {
			addNamespace(p[0])
		}
	}
	for _, g := range s.GroupingPolicy {
		if len(g) > 1 {
			addNamespace(g[1])
		}
	}

	// stripSeg drops a segment's "<ns>:" prefix only when ns ∈ N. A global
	// identity (e.g. an OIDC "a:b" whose "a" is not a namespace) is left intact;
	// so is the db:wv_internal_empty placeholder (namespace "").
	stripSeg := func(seg string) string {
		if _, ok := namespaces[namespacing.NamespaceFromQualified(seg)]; ok {
			return namespacing.StripQualification(seg)
		}
		return seg
	}
	stripRole := func(roleKey string) string {
		return conv.PrefixRoleName(stripSeg(conv.TrimRoleNamePrefix(roleKey)))
	}
	// stripSubject rewrites a grouping subject. db/oidc principals may be
	// namespaced ("db:ns:bob"); group subjects are global by design and left
	// untouched, as is any subject we cannot parse.
	stripSubject := func(subject string) string {
		user, prefix, err := conv.GetUserAndPrefix(subject)
		if err != nil {
			return subject
		}
		switch prefix {
		case string(authentication.AuthTypeDb), string(authentication.AuthTypeOIDC):
			return prefix + conv.PREFIX_SEPARATOR + stripSeg(user)
		default:
			return subject
		}
	}

	// stripped role/subject -> the distinct source names that landed on it.
	// casbin would collapse these silently; we count them to fail loud instead.
	roleSources := map[string]map[string]struct{}{}
	subjectSources := map[string]map[string]struct{}{}
	track := func(m map[string]map[string]struct{}, stripped, source string) {
		set, ok := m[stripped]
		if !ok {
			set = map[string]struct{}{}
			m[stripped] = set
		}
		set[source] = struct{}{}
	}

	out := snapshot{Version: s.Version}
	out.Policy = make([][]string, len(s.Policy))
	for i, p := range s.Policy {
		row := slices.Clone(p)
		if len(row) > 0 {
			stripped := stripRole(row[0])
			track(roleSources, stripped, row[0])
			row[0] = stripped
		}
		if len(row) > 1 {
			resource, err := namespacing.RewriteNamespaceSegments(row[1], func(seg string) (string, error) {
				return stripSeg(seg), nil
			})
			if err != nil {
				return snapshot{}, fmt.Errorf("rewrite resource %q: %w", row[1], err)
			}
			row[1] = resource
		}
		out.Policy[i] = row
	}

	out.GroupingPolicy = make([][]string, len(s.GroupingPolicy))
	for i, g := range s.GroupingPolicy {
		row := slices.Clone(g)
		if len(row) > 0 {
			stripped := stripSubject(row[0])
			track(subjectSources, stripped, row[0])
			row[0] = stripped
		}
		if len(row) > 1 {
			stripped := stripRole(row[1])
			track(roleSources, stripped, row[1])
			row[1] = stripped
		}
		out.GroupingPolicy[i] = row
	}

	var errs []string
	for stripped, sources := range roleSources {
		shortName := conv.TrimRoleNamePrefix(stripped)
		if slices.Contains(authorization.BuiltInRoles, shortName) {
			// class 3: a custom "<ns>:viewer" strips onto a built-in; restore's
			// applyPredefinedRoles would then wipe the custom perms and retarget
			// its assignments at the canonical built-in.
			if hasNamespacedSource(sources, stripped) {
				errs = append(errs, fmt.Sprintf("roles %v strip to built-in role %q", sortedKeys(sources), shortName))
			}
			continue
		}
		// classes 1/2: two distinct namespaced roles fuse into one (union of
		// perms, or a silent dedupe when the rows are identical).
		if len(sources) > 1 {
			errs = append(errs, fmt.Sprintf("roles %v strip to the same name %q", sortedKeys(sources), shortName))
		}
	}
	// class 4: two namespaced principals fuse into one, merging their role sets.
	for stripped, sources := range subjectSources {
		if len(sources) > 1 {
			errs = append(errs, fmt.Sprintf("subjects %v strip to the same name %q", sortedKeys(sources), stripped))
		}
	}
	if len(errs) > 0 {
		slices.Sort(errs)
		return snapshot{}, fmt.Errorf("namespace strip would collide RBAC entities: %s", strings.Join(errs, "; "))
	}

	return out, nil
}

// hasNamespacedSource reports whether any source name differs from its stripped
// form, i.e. it carried a namespace that N stripped. A source equal to stripped
// was already unqualified (a genuine built-in row) and is not a collision.
func hasNamespacedSource(sources map[string]struct{}, stripped string) bool {
	for s := range sources {
		if s != stripped {
			return true
		}
	}
	return false
}

func sortedKeys(m map[string]struct{}) []string {
	xs := slices.Collect(maps.Keys(m))
	slices.Sort(xs)
	return xs
}

// ValidateNamespaceStrip dry-runs the stripNamespaces arm of [Manager.Restore]
// against blob without mutating any state: it decodes the snapshot and attempts
// the namespace strip, returning the exact collision error a real
// namespace-stripping restore would hit. It needs no casbin store, so the backup
// coordinator can reject a doomed restore before any node stages data, even on
// clusters where RBAC is otherwise untouched.
func ValidateNamespaceStrip(blob []byte) error {
	// Restore treats an empty snapshot as a no-op.
	if len(blob) == 0 {
		return nil
	}
	snap := snapshot{}
	if err := json.Unmarshal(blob, &snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %w", err)
	}
	_, err := stripRBACSnapshot(snap)
	return err
}
