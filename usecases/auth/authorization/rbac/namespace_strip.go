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
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// stripRBACSnapshot rewrites a namespaced RBAC snapshot so it can be restored
// into a cluster that has namespaces disabled, which the rest of the codebase
// calls graduation. It drops the "<namespace>:" prefix from names.
//
// Role names, resources and OIDC subjects are stripped only when the namespace
// is one the snapshot's own roles name, because a ':' in those can also be part
// of a global name. That is complete only while every namespace in the blob is
// named by some role in it, which holds because a resource is qualified with its
// role's namespace when it is created. If a future path lets a resource carry a
// namespace no role name mentions, those rows stay qualified and nothing reports
// it.
//
// A db subject is handled by a different rule, because it is one of exactly two
// things. It is either a static API key user, whose name is taken verbatim from
// configuration and may contain a ':' of its own, or a dynamic user, whose name
// is validated on create against a pattern that admits no ':'. So a db subject
// that the cluster lists as a static API key user is left exactly as it is, and
// any other db subject is stripped unconditionally. The namespace set must not
// be consulted for the second case: a namespaced user whose only role is global
// carries a namespace no role name mentions, and a cluster with namespaces
// disabled refuses to start while any db grouping subject is still qualified.
//
// staticAPIKeyUsers is the restoring cluster's own configured list. A static
// user that exists on the source cluster but not on the target is therefore
// treated as a dynamic user and stripped.
//
// A snapshot is rejected instead of applied when stripping would damage it, for
// any of three reasons. Two different names may strip to the same name, which
// casbin would merge on Restore with no error reported. A single namespaced role
// may strip onto a built-in name. On restore, applyPredefinedRoles would wipe
// that role's permissions, and its assignments would either transfer to the
// built-in or be reset from configuration depending on which built-in it hit. A
// db subject may strip onto a name the cluster configures as a static API key
// user, which would hand that operator key the roles the namespaced user held.
// Returns the rewritten snapshot, or an error listing everything that was
// rejected.
func stripRBACSnapshot(s snapshot, staticAPIKeyUsers []string) (snapshot, error) {
	staticUsers := make(map[string]struct{}, len(staticAPIKeyUsers))
	for _, user := range staticAPIKeyUsers {
		staticUsers[user] = struct{}{}
	}

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
	// collected above, so a global name such as the OIDC subject "a:b", whose "a"
	// is not a namespace, is left intact.
	stripSeg := func(seg string) string {
		if _, ok := namespaces[namespacing.NamespaceFromQualified(seg)]; ok {
			return namespacing.StripQualification(seg)
		}
		return seg
	}
	stripRole := func(roleKey string) string {
		return conv.PrefixRoleName(stripSeg(conv.TrimRoleNamePrefix(roleKey)))
	}
	// db and oidc principals may be namespaced ("db:ns:bob"), and the two are not
	// stripped the same way. A db name that the cluster configures as a static API
	// key user is a global identity and is kept verbatim, ':' and all. Every other
	// db name belongs to a dynamic user, which cannot contain a ':' of its own, so
	// any ':' in one is a namespace qualifier and is dropped whatever the role
	// names say. An OIDC name may contain a ':' of its own and no configured list
	// tells those apart, so it stays on the namespace-set rule. Group subjects are
	// global by design, so they are left alone, as is any subject we cannot parse.
	stripSubject := func(subject string) string {
		user, prefix, err := conv.GetUserAndPrefix(subject)
		if err != nil {
			return subject
		}
		switch prefix {
		case string(authentication.AuthTypeDb):
			if _, ok := staticUsers[user]; ok {
				return subject
			}
			return prefix + conv.PREFIX_SEPARATOR + namespacing.StripQualification(user)
		case string(authentication.AuthTypeOIDC):
			return prefix + conv.PREFIX_SEPARATOR + stripSeg(user)
		default:
			return subject
		}
	}

	// staticTakeover reports the static API key user a db subject landed on. Such
	// a user is a principal of the target cluster and need not appear in the blob
	// at all, so no comparison between two blob rows can see the clash. The
	// subject must have changed for this to count: a blob subject that arrived
	// unqualified and matches a static user is that same user, and restoring it
	// gives that user nothing it did not already hold.
	staticTakeover := func(source, stripped string) (string, bool) {
		if source == stripped {
			return "", false
		}
		user, prefix, err := conv.GetUserAndPrefix(stripped)
		if err != nil || prefix != string(authentication.AuthTypeDb) {
			return "", false
		}
		_, ok := staticUsers[user]
		return user, ok
	}

	// Stripped name -> the distinct original names that produced it. More than one
	// role or subject name means casbin would silently merge them. A single role
	// name is still rejected when it strips onto a built-in name, and a single db
	// subject when it strips onto a static API key user. All the checks run below.
	roleSources := map[string]map[string]struct{}{}
	subjectSources := map[string]map[string]struct{}{}
	takenOverStaticUsers := map[string]map[string]struct{}{}
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
			if user, ok := staticTakeover(row[0], stripped); ok {
				track(takenOverStaticUsers, user, row[0])
			}
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
	// A namespaced principal would become an operator key the target already has.
	for user, sources := range takenOverStaticUsers {
		errs = append(errs, fmt.Sprintf("subjects %v strip to %q, which this cluster configures as a static API key user", sortedKeys(sources), user))
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

// StaticAPIKeyUsers returns the static API key user names the namespace strip
// must treat as global identities, which is none at all while API keys are
// turned off. A configuration file can populate the list and disable API keys at
// once, and on that path the names are never checked for format, so a disabled
// list may hold anything. Every other reader of the list gates on the
// same flag, and the strip has to agree with them or it would keep a row whole
// for a principal the cluster will never authenticate.
func StaticAPIKeyUsers(conf config.Authentication) []string {
	if !conf.APIKey.Enabled {
		return nil
	}
	return conf.APIKey.Users
}

// ValidateNamespaceStrip runs the stripNamespaces path of [Manager.Restore]
// against blob without changing any state, returning the same collision error a
// real strip-restore would hit. It needs no casbin store, so the backup
// coordinator can reject a doomed restore before any node stages data.
//
// staticAPIKeyUsers must be the same list the nodes will strip with, which is
// the restoring cluster's configured static API key users.
func ValidateNamespaceStrip(blob []byte, staticAPIKeyUsers []string) error {
	// Restore treats an empty snapshot as a no-op.
	if len(blob) == 0 {
		return nil
	}
	snap := snapshot{}
	if err := json.Unmarshal(blob, &snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %w", err)
	}
	_, err := stripRBACSnapshot(snap, staticAPIKeyUsers)
	return err
}
