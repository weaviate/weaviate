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

// stripRBACSnapshot rewrites a namespaced RBAC snapshot so it can be restored
// into a cluster that has namespaces disabled, which the rest of the codebase
// calls graduation. It drops the "<namespace>:" prefix from names, using only
// the namespaces named by the snapshot's own roles.
//
// Taking the namespaces from role names alone means the strip is complete only
// while every namespace in the blob is named by some role in it. That holds
// because a resource is qualified with its role's namespace when it is created,
// and subjects reach the blob only through their role assignments. If a future
// path lets a resource or subject carry a namespace no role name mentions,
// those rows stay qualified and nothing reports it.
//
// A snapshot is rejected instead of applied when stripping would damage it, for
// either of two reasons. Two different names may strip to the same name, which
// casbin would merge on Restore with no error reported. A single namespaced role
// may strip onto a built-in name. On restore, applyPredefinedRoles would wipe
// that role's permissions, and its assignments would either transfer to the
// built-in or be reset from configuration depending on which built-in it hit.
// Returns the rewritten snapshot, or an error listing everything that was
// rejected.
func stripRBACSnapshot(s snapshot) (snapshot, error) {
	// Collect the namespaces from role names only. Roles are qualified when they
	// are created, so a role name's "<ns>:" prefix really is a namespace. Other
	// names cannot be split that way: an OIDC subject or a resource can contain a
	// ':' of its own.
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

	// stripSeg drops a "<ns>:" prefix only when ns is one of the namespaces
	// collected above. Global names are left intact: an OIDC subject "a:b" whose
	// "a" is not a namespace, and the db:wv_internal_empty placeholder.
	stripSeg := func(seg string) string {
		if _, ok := namespaces[namespacing.NamespaceFromQualified(seg)]; ok {
			return namespacing.StripQualification(seg)
		}
		return seg
	}
	stripRole := func(roleKey string) string {
		return conv.PrefixRoleName(stripSeg(conv.TrimRoleNamePrefix(roleKey)))
	}
	// db and oidc principals may be namespaced ("db:ns:bob"). Group subjects are
	// global by design, so they are left alone, as is any subject we cannot parse.
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

	// Stripped name -> the distinct original names that produced it. More than one
	// role or subject name means casbin would silently merge them. A single role
	// name is still rejected when it strips onto a built-in name. Both checks run
	// below.
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
			// A custom role such as "<ns>:viewer" strips onto a built-in name. On
			// restore, applyPredefinedRoles would then wipe the custom permissions.
			// The assignments move to the real built-in for viewer and admin, and
			// are reset from configuration for root and read-only.
			if hasNamespacedSource(sources, stripped) {
				errs = append(errs, fmt.Sprintf("roles %v strip to built-in role %q", sortedKeys(sources), shortName))
			}
			continue
		}
		// Two different namespaced roles would become one, either merging their
		// permissions or being deduped when the rows happen to be identical.
		if len(sources) > 1 {
			errs = append(errs, fmt.Sprintf("roles %v strip to the same name %q", sortedKeys(sources), shortName))
		}
	}
	// Two namespaced principals would become one, merging their role sets.
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

// hasNamespacedSource reports whether the strip actually changed any of the
// source names. A source that already equals the stripped name was unqualified
// to begin with, so it is a genuine built-in row and not a collision.
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

// ValidateNamespaceStrip runs the stripNamespaces path of [Manager.Restore]
// against blob without changing any state, returning the same collision error a
// real strip-restore would hit. It needs no casbin store, so the backup
// coordinator can reject a doomed restore before any node stages data.
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
