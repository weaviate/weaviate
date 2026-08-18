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

// stripRBACSnapshot rewrites a namespaced RBAC snapshot for restore into a
// cluster with namespaces disabled, which this codebase calls graduation. It
// drops the "<namespace>:" prefix from names, or refuses the snapshot.
//
// Two rules apply, because a ':' is not always a namespace separator. Role
// names, resources and OIDC subjects strip only when the prefix is a namespace
// some role in the snapshot names, since each of those may hold a ':' of its
// own.
//
// A db subject cannot use that rule. A namespaced user whose only role is
// global carries a namespace no role name mentions, and the target refuses to
// start while any db grouping subject is still qualified. A dynamic user name
// admits no ':' of its own, so a db subject strips unconditionally unless the
// restoring cluster configures its full name as a static API key user. Those
// names are taken from configuration unchecked and may hold a ':'. A users
// resource names an identity rather than a schema name, so it keeps a
// configured static name whole as well.
//
// staticAPIKeyUsers is the restoring cluster's own list, not the source's, so a
// static user the target does not configure is treated as a dynamic one.
//
// Stripping is refused rather than applied when two entities would collapse
// into one, because casbin merges colliding rows on Restore and reports
// nothing. The error names every rejection.
func stripRBACSnapshot(s snapshot, staticAPIKeyUsers []string) (snapshot, error) {
	staticUsers := make(map[string]struct{}, len(staticAPIKeyUsers))
	for _, user := range staticAPIKeyUsers {
		staticUsers[user] = struct{}{}
	}

	// The snapshot's own list wins, because the source cluster knew which prefixes
	// were namespaces. A snapshot carrying none falls back to role names, the only
	// names whose ':' is reliably a qualifier. The fallback misses a namespace no
	// role name mentions, leaving that resource or OIDC subject qualified with
	// nothing reporting it. Re-taking the backup is the fix.
	namespaces := make(map[string]struct{}, len(s.Namespaces))
	for _, ns := range s.Namespaces {
		namespaces[ns] = struct{}{}
	}
	if len(namespaces) == 0 {
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
			// A users resource names an identity, so a static API key user name is
			// kept whole here exactly as it is for that identity's db subject. The
			// callback sees one segment and cannot tell which resource kind it came
			// from, so the path is tested out here. Collection, alias and role
			// segments name no identity and must keep stripping unconditionally.
			isUserResource := strings.HasPrefix(row[1], namespacing.UsersPrefix)
			isRoleResource := strings.HasPrefix(row[1], namespacing.RolesPrefix)
			resource, err := namespacing.RewriteNamespaceSegments(row[1], func(seg string) (string, error) {
				if isUserResource {
					if _, ok := staticUsers[seg]; ok {
						return seg, nil
					}
				}
				stripped := stripSeg(seg)
				// A roles resource names a role, so it goes through the same collision
				// check role names do. Nothing stops a namespaced caller holding a
				// permission on "<ns>:viewer": the built-in guard runs on role names at
				// create time, and this path never sees one.
				if isRoleResource {
					track(roleSources, conv.PrefixRoleName(stripped), conv.PrefixRoleName(seg))
				}
				return stripped, nil
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
				errs = append(errs, fmt.Sprintf("roles %v strip to built-in role %q", slices.Sorted(maps.Keys(sources)), shortName))
			}
			continue
		}
		// Two different namespaced roles would become one, either merging their
		// permissions or being deduped when the rows happen to be identical.
		if len(sources) > 1 {
			errs = append(errs, fmt.Sprintf("roles %v strip to the same name %q", slices.Sorted(maps.Keys(sources)), shortName))
		}
	}
	// Two namespaced principals would become one, merging their role sets.
	for stripped, sources := range subjectSources {
		if len(sources) > 1 {
			errs = append(errs, fmt.Sprintf("subjects %v strip to the same name %q", slices.Sorted(maps.Keys(sources)), stripped))
		}
	}
	// A namespaced principal would become an operator key the target already has.
	for user, sources := range takenOverStaticUsers {
		errs = append(errs, fmt.Sprintf("subjects %v strip to %q, which this cluster configures as a static API key user", slices.Sorted(maps.Keys(sources)), user))
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

// ValidateNamespaceStrip runs the strip path of [Manager.Restore] against blob
// without changing state, returning the collision error a real strip would hit.
// staticAPIKeyUsers is the restoring cluster's configured list.
func ValidateNamespaceStrip(blob []byte, staticAPIKeyUsers []string) error {
	return ValidateSnapshot(blob, true, staticAPIKeyUsers)
}

// ValidateSnapshot runs the checks [Manager.Restore] makes before it clears
// the policy store, without clearing it: the decode, plus the strip when
// stripNamespaces is set. Restore has no version check of its own, so with the
// strip off only the decode runs.
func ValidateSnapshot(blob []byte, stripNamespaces bool, staticAPIKeyUsers []string) error {
	// Restore treats an empty snapshot as a no-op.
	if len(blob) == 0 {
		return nil
	}
	snap := snapshot{}
	if err := json.Unmarshal(blob, &snap); err != nil {
		return fmt.Errorf("restore snapshot: decode json: %w", err)
	}
	if !stripNamespaces {
		return nil
	}
	_, err := stripRBACSnapshot(snap, staticAPIKeyUsers)
	return err
}

// ReferencedNamespaces returns every namespace the snapshot names: the
// snapshot's own Namespaces list, plus the prefixes of its "db:" user
// subjects. The list is written by the source cluster, the only side that can
// tell a namespace prefix from a colon inside a global id such as the OIDC
// subject "urn:foo", and it covers role names, resource paths, and OIDC
// subjects. It leaves db subjects out (see referencedNamespaces), so they are
// read here: a dynamic user name cannot contain a colon, so a colon there
// always marks a namespace. The restoring cluster's static API key users are
// the exception. A blob from before the list existed gets no checking beyond
// its db subjects; namespaced backups predating the list are unsupported.
func ReferencedNamespaces(blob []byte, staticAPIKeyUsers []string) ([]string, error) {
	if len(blob) == 0 {
		return nil, nil
	}
	snap := snapshot{}
	if err := json.Unmarshal(blob, &snap); err != nil {
		return nil, fmt.Errorf("restore snapshot: decode json: %w", err)
	}

	staticUsers := make(map[string]struct{}, len(staticAPIKeyUsers))
	for _, user := range staticAPIKeyUsers {
		staticUsers[user] = struct{}{}
	}

	seen := map[string]struct{}{}
	add := func(name string) {
		if ns := namespacing.NamespaceFromQualified(name); ns != "" {
			seen[ns] = struct{}{}
		}
	}

	for _, g := range snap.GroupingPolicy {
		if len(g) == 0 {
			continue
		}
		user, prefix, err := conv.GetUserAndPrefix(g[0])
		if err != nil || prefix != string(authentication.AuthTypeDb) {
			continue
		}
		if _, ok := staticUsers[user]; ok {
			continue
		}
		add(user)
	}

	for _, ns := range snap.Namespaces {
		if ns != "" {
			seen[ns] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(seen)), nil
}
