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
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	casbinutil "github.com/casbin/casbin/v2/util"
	"github.com/pkg/errors"

	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

const DEFAULT_POLICY_VERSION = "1.29.0"

const (
	// MODEL is the used model for casbin to store roles, permissions, users and comparisons patterns.
	// docs: https://casbin.org/docs/syntax-for-models
	MODEL = `
	[request_definition]
	r = sub, obj, act, ns

	[policy_definition]
	p = sub, obj, act, dom

	[role_definition]
	g = _, _

	[policy_effect]
	e = some(where (p.eft == allow))

	[matchers]
	m = g(r.sub, p.sub) && namespaceAwareMatcher(r.obj, p.obj, r.ns) && regexMatch(r.act, p.act)
`
)

// Init builds the in-memory enforcer. It attaches no persistence adapter: RAFT snapshot restore and log replay rebuild the RBAC state on every start.
func Init(conf rbacconf.Config, policyPath string, authNconf config.Authentication, namespacesEnabled bool) (*casbin.SyncedCachedEnforcer, error) {
	m, err := model.NewModelFromString(MODEL)
	if err != nil {
		return nil, fmt.Errorf("load rbac model: %w", err)
	}

	enforcer, err := casbin.NewSyncedCachedEnforcer(m)
	if err != nil {
		return nil, fmt.Errorf("failed to create enforcer: %w", err)
	}
	enforcer.EnableCache(true)
	// Set a TTL to prevent unbounded cache growth. Runtime policy updates via
	// the Manager call InvalidateCache(); this TTL is an additional safeguard,
	// including for init/upgrade paths that may modify policy without
	// explicitly invalidating the cache.
	enforcer.SetExpireTime(1 * time.Hour)

	if err := seedFromLegacyPolicyFile(enforcer, rbacDirIn(policyPath), authNconf); err != nil {
		return nil, err
	}

	// docs: https://casbin.org/docs/function/
	enforcer.AddFunction("namespaceAwareMatcher", makeNamespaceAwareMatcherFunc(namespacesEnabled))

	if err := applyPredefinedRoles(enforcer, conf, authNconf, namespacesEnabled); err != nil {
		return nil, errors.Wrapf(err, "apply env config")
	}

	return enforcer, nil
}

// rbacDirIn returns the dir under storagePath where earlier releases wrote policy.csv and version.
func rbacDirIn(storagePath string) string {
	return filepath.Join(storagePath, "rbac")
}

// seedFromLegacyPolicyFile loads the policy.csv that earlier releases wrote
// under rbacDir, so a cluster upgraded from them keeps its roles until a RAFT
// snapshot carries RBAC state. A missing file is a no-op. The seed never writes:
// the file and its version stay as the earlier release left them, so the <1.30
// upgrade sees the same rows on every boot.
func seedFromLegacyPolicyFile(enforcer *casbin.SyncedCachedEnforcer, rbacDir string, authNconf config.Authentication) error {
	policyFile := filepath.Join(rbacDir, "policy.csv")
	if _, err := os.Stat(policyFile); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat legacy policy file %s: %w", policyFile, err)
	}

	enforcer.SetAdapter(fileadapter.NewAdapter(policyFile))
	err := enforcer.LoadPolicy()
	// casbin writes every mutation through an attached adapter, so detaching
	// it keeps all later changes in memory.
	enforcer.SetAdapter(nil)
	if err != nil {
		return fmt.Errorf("load legacy policy file %s: %w", policyFile, err)
	}

	// parse version string to check if upgrade is needed
	policyVersion, err := getVersion(rbacDir)
	if err != nil {
		return err
	}
	versionParts := strings.Split(policyVersion, ".")
	minorVersion, err := strconv.Atoi(versionParts[1])
	if err != nil {
		return err
	}

	if versionParts[0] == "1" && minorVersion < 30 {
		if err := upgradePoliciesFrom129(enforcer, false); err != nil {
			return err
		}

		if err := upgradeGroupingsFrom129(enforcer, authNconf); err != nil {
			return err
		}
	}
	return nil
}

// applyPredefinedRoles adds pre-defined roles (admin/viewer/root) and assigns them to the users provided in the
// local config
func applyPredefinedRoles(enforcer *casbin.SyncedCachedEnforcer, conf rbacconf.Config, authNconf config.Authentication, namespacesEnabled bool) error {
	if namespacesEnabled {
		if err := rejectNamespacedRootSubjects(conf); err != nil {
			return err
		}
	}

	// Wipe all built-in role policies before re-registering. The
	// canonical shape lives in code, so rows from a legacy seed or a
	// restored snapshot are replaced by it on every boot and restore.
	for _, role := range authorization.BuiltInRoles {
		if _, err := enforcer.RemoveFilteredNamedPolicy("p", 0, conv.PrefixRoleName(role)); err != nil {
			return err
		}
	}
	// Only wipe groupings for env-var-only roles: those are reset from
	// config on every boot. Admin/viewer groupings are API-managed and
	// must survive restarts.
	for _, role := range authorization.EnvVarRoles {
		if _, err := enforcer.RemoveFilteredGroupingPolicy(1, conv.PrefixRoleName(role)); err != nil {
			return err
		}
	}

	// Register wildcard policies. On NS-disabled all four built-ins get
	// wildcards; on NS-enabled only root/read-only do — Casbin lacks deny
	// semantics, so admin/viewer must be registered per-permission to be
	// narrowable.
	wildcardRoles := authorization.WildcardRoles
	if namespacesEnabled {
		wildcardRoles = []string{authorization.Root, authorization.ReadOnly}
	}
	if err := addWildcardPolicies(enforcer, wildcardRoles, conv.BuiltInWildcardVerb); err != nil {
		return err
	}

	// The metadata reader is always registered per permission: a wildcard READ
	// would include read_data.
	metadataPolicies, err := conv.PermissionToPolicies(authorization.BuiltInPermissionsFor(namespacesEnabled)[authorization.MetadataReader]...)
	if err != nil {
		return fmt.Errorf("metadata reader policies: %w", err)
	}
	for _, p := range metadataPolicies {
		if _, err := enforcer.AddNamedPolicy("p", conv.PrefixRoleName(authorization.MetadataReader), p.Resource, p.Verb, p.Domain); err != nil {
			return fmt.Errorf("add metadata reader policy: %w", err)
		}
	}

	if namespacesEnabled {
		narrowed := authorization.BuiltInPermissionsFor(true)
		for _, role := range authorization.ApiManagedBuiltInRoles {
			policies, err := conv.PermissionToPolicies(narrowed[role]...)
			if err != nil {
				return fmt.Errorf("tenant-safe %s policies: %w", role, err)
			}
			for _, p := range policies {
				if _, err := enforcer.AddNamedPolicy("p", conv.PrefixRoleName(role), p.Resource, p.Verb, p.Domain); err != nil {
					return fmt.Errorf("add tenant-safe policy: %w", err)
				}
			}
		}
	}

	for i := range conf.RootUsers {
		if strings.TrimSpace(conf.RootUsers[i]) == "" {
			continue
		}

		if authNconf.APIKey.Enabled && slices.Contains(authNconf.APIKey.Users, conf.RootUsers[i]) {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.RootUsers[i], authentication.AuthTypeDb), conv.PrefixRoleName(authorization.Root)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}

		if authNconf.OIDC.Enabled {
			if _, err := enforcer.AddRoleForUser(conv.UserNameWithTypeFromId(conf.RootUsers[i], authentication.AuthTypeOIDC), conv.PrefixRoleName(authorization.Root)); err != nil {
				return fmt.Errorf("add role for user: %w", err)
			}
		}
	}

	// The boot writer in adapters/handlers/rest submits the same grants as RAFT
	// entries. This in-memory add makes them effective before RAFT is up.
	for _, g := range AdminViewerGrants(conf, authNconf) {
		if _, err := enforcer.AddRoleForUser(g.Subject, conv.PrefixRoleName(g.Role)); err != nil {
			return fmt.Errorf("add role for user: %w", err)
		}
	}

	for _, group := range conf.RootGroups {
		if strings.TrimSpace(group) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(group), conv.PrefixRoleName(authorization.Root)); err != nil {
			return fmt.Errorf("add role for group %s: %w", group, err)
		}
	}

	for _, viewerGroup := range conf.ReadOnlyGroups {
		if strings.TrimSpace(viewerGroup) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(viewerGroup), conv.PrefixRoleName(authorization.ReadOnly)); err != nil {
			return fmt.Errorf("add viewer role for group %s: %w", viewerGroup, err)
		}
	}

	for _, metadataGroup := range conf.MetadataGroups {
		if strings.TrimSpace(metadataGroup) == "" {
			continue
		}
		if _, err := enforcer.AddRoleForUser(conv.PrefixGroupName(metadataGroup), conv.PrefixRoleName(authorization.MetadataReader)); err != nil {
			return fmt.Errorf("add metadata reader role for group %s: %w", metadataGroup, err)
		}
	}

	return nil
}

// RoleGrant assigns the built-in role Role (unprefixed) to Subject (a
// db: or oidc: prefixed user name).
type RoleGrant struct {
	Subject string
	Role    string
}

// AdminViewerGrants returns the admin and viewer assignments conf.AdminUsers
// and conf.ViewerUsers configure: a db: subject for each user listed in an
// enabled APIKey config, and an oidc: subject for each user when OIDC is
// enabled. Blank entries grant nothing.
func AdminViewerGrants(conf rbacconf.Config, authNconf config.Authentication) []RoleGrant {
	var grants []RoleGrant
	// temporary to enable import of existing keys to WCD (Admin + readonly)
	for _, byRole := range []struct {
		role  string
		users []string
	}{
		{authorization.Admin, conf.AdminUsers},
		{authorization.Viewer, conf.ViewerUsers},
	} {
		for _, user := range byRole.users {
			if strings.TrimSpace(user) == "" {
				continue
			}
			if authNconf.APIKey.Enabled && slices.Contains(authNconf.APIKey.Users, user) {
				grants = append(grants, RoleGrant{Subject: conv.UserNameWithTypeFromId(user, authentication.AuthTypeDb), Role: byRole.role})
			}
			if authNconf.OIDC.Enabled {
				grants = append(grants, RoleGrant{Subject: conv.UserNameWithTypeFromId(user, authentication.AuthTypeOIDC), Role: byRole.role})
			}
		}
	}
	return grants
}

var (
	// Operator-only domain prefixes; see operatorOnlyResource.
	backupsPrefix    = authorization.BackupsDomain + "/"
	nodesPrefix      = authorization.NodesDomain + "/"
	replicatePrefix  = authorization.ReplicateDomain + "/"
	clusterPrefix    = authorization.ClusterDomain + "/"
	namespacesPrefix = authorization.NamespacesDomain + "/"
	groupsPrefix     = authorization.GroupsDomain + "/"
)

// Nodes resource path and prefix; see weaviateKeyMatch.
var (
	nodesMinimalResource = authorization.NodesDomain + "/verbosity/minimal"
	nodesVerbosePrefix   = authorization.NodesDomain + "/verbosity/verbose/"
)

// addWildcardPolicies registers a single "*" policy per role with the role's
// wildcard verb. It fails closed: an empty verb regex-matches every action in
// casbin, so a role without a wildcard verb would silently gain full access.
func addWildcardPolicies(enforcer *casbin.SyncedCachedEnforcer, roles []string, verbs map[string]string) error {
	for _, role := range roles {
		verb := verbs[role]
		if verb == "" {
			return fmt.Errorf("built-in role %q has no wildcard verb", role)
		}
		if _, err := enforcer.AddNamedPolicy("p", conv.PrefixRoleName(role), "*", verb, "*"); err != nil {
			return fmt.Errorf("add policy: %w", err)
		}
	}
	return nil
}

// rejectNamespacedRootSubjects fails startup when a namespace-qualified subject
// is configured for the root role: a namespaced principal must never inherit
// cluster-wide root. read-only has no static user list (groups only, deferred
// to namespaced groups), so root users are the only static subject to guard.
func rejectNamespacedRootSubjects(conf rbacconf.Config) error {
	for _, u := range conf.RootUsers {
		if strings.Contains(u, schema.NamespaceSeparator) {
			return fmt.Errorf("RBAC root user %q is namespace-qualified; a namespaced principal cannot inherit the root role", u)
		}
	}
	return nil
}

// anyNamespacePattern matches exactly one `<ns>:` prefix.
var anyNamespacePattern = "[^/" + schema.NamespaceSeparator + "]+" + schema.NamespaceSeparator

// errForeignSegment aborts a fixed-ns rewrite when an already-qualified policy
// segment names a different namespace; it maps to a cross-namespace deny.
var errForeignSegment = errors.New("policy segment names a different namespace")

// rewritePolicy specializes (fixedNs=true, prefix=`<ns>:`) or widens
// (fixedNs=false, prefix=anyNamespacePattern) the namespace-bearing segments
// of policy. Returns ok=false in fixedNs mode if any already-qualified
// segment names a different namespace.
func rewritePolicy(policy, prefix string, fixedNs bool) (string, bool) {
	rewritten, err := namespacing.RewriteNamespaceSegments(policy, func(segment string) (string, error) {
		if strings.Contains(segment, schema.NamespaceSeparator) {
			if fixedNs && !strings.HasPrefix(segment, prefix) {
				return "", errForeignSegment
			}
			return segment, nil
		}
		return prefix + segment, nil
	})
	if err != nil {
		return "", false
	}
	return rewritten, true
}

// operatorOnlyResource reports whether path addresses an operator-only domain
// (backups, nodes, replicate, cluster, namespaces, groups). A namespaced caller
// is denied these regardless of any role granting them. Groups are global:
// their ids are not namespace-bearing, so a namespaced caller must never reach
// them. roles is excluded: role resources are namespace-bearing via qualified
// names, so a blanket prefix-deny would be wrong for them.
func operatorOnlyResource(path string) bool {
	return strings.HasPrefix(path, backupsPrefix) ||
		strings.HasPrefix(path, nodesPrefix) ||
		strings.HasPrefix(path, replicatePrefix) ||
		strings.HasPrefix(path, clusterPrefix) ||
		strings.HasPrefix(path, namespacesPrefix) ||
		strings.HasPrefix(path, groupsPrefix)
}

// weaviateKeyMatch runs the `/shards/#` vs `/shards/.*` carve-out then
// KeyMatch5: a collection-level request must not match a per-tenant policy.
//
// A verbose nodes policy on any collection also satisfies the minimal nodes request.
func weaviateKeyMatch(reqObj, polObj string) bool {
	if strings.HasSuffix(reqObj, "/shards/#") && strings.HasSuffix(polObj, "/shards/.*") {
		return false
	}
	if reqObj == nodesMinimalResource && strings.HasPrefix(polObj, nodesVerbosePrefix) {
		return true
	}
	return casbinutil.KeyMatch5(reqObj, polObj)
}

// namespaceAwareMatcher reports whether policy resource pattern polObj
// authorizes a request for reqObj issued in namespace ns ("" for
// global/operator callers).
//
//   - ns != "": unqualified policy segments specialize to `<ns>:<segment>`;
//     already-qualified segments must start with `<ns>:` exactly.
//   - ns == "" with a qualified request segment: unqualified policy segments
//     widen with anyNamespacePattern; qualified segments stay fixed.
//   - ns == "" with an unqualified request, a users/<id> resource, or a
//     non-namespaceable shape: no rewrite.
//
// Namespaced callers are denied the operator-only domains (backups, nodes,
// replicate, cluster, namespaces, groups) outright.
func namespaceAwareMatcher(reqObj, polObj, ns string) bool {
	// Namespaced callers have no access to operator-only domains, whatever the policy.
	if ns != "" && operatorOnlyResource(reqObj) {
		return false
	}

	// Trivial passthrough: a global caller that needs no widening matches the
	// policy literally. Only collection/data/aliases/roles segments treat ':'
	// as a namespace boundary.
	if ns == "" && !namespacing.GlobalCallerWidens(reqObj) {
		return weaviateKeyMatch(reqObj, polObj)
	}

	// Normalize `/*` to `/.*` before rewriting. KeyMatch5 applies this
	// transform itself, but only at slash boundaries; once the rewrite
	// prepends `<ns>:` to a bare `*` segment the `*` is no longer
	// slash-bounded and KeyMatch5's transform stops firing. Producers like
	// CollectionsMetadata, ShardsMetadata, and Objects emit literal `*`
	// segments — without this normalization they would never match a
	// qualified request after the rewrite.
	polObj = strings.ReplaceAll(polObj, "/*", "/.*")

	// Reaching this point means a rewrite is needed: either ns != "" (the
	// caller is namespaced) or ns == "" with a qualified request (a global
	// caller addressing a namespace-qualified resource).
	//
	// Casbin's KeyMatch5 has no notion of namespaces — it runs a plain regex
	// match. The strategy is to rewrite the *policy* so its namespace-bearing
	// segments speak in the same shape as the request, then hand the
	// rewritten string to KeyMatch5. The request itself is never rewritten.
	//
	// Worked example, policy "schema/collections/Movies.*/shards/#" (operator
	// template) vs request "schema/collections/customer1:Movies/shards/#":
	//   - ns="customer1" → policy becomes "schema/collections/customer1:Movies.*/shards/#"
	//     (specialize); KeyMatch5 matches.
	//   - ns=""          → policy becomes "schema/collections/[^/:]+:Movies.*/shards/#"
	//     (widen);       KeyMatch5 matches any namespace prefix.

	// 1. Locate the collection segment in each path. FindNamespaceSegments
	//    returns its [start, end) bounds and a flag set on aliases paths,
	//    which carry a *second* namespace-bearing segment after "/aliases/".
	//    The request side only needs end+hasAlias (for the shape check
	//    below); we don't rewrite the request, so its start is discarded.
	_, polColEnd, polHasAlias := namespacing.FindNamespaceSegments(polObj)
	_, reqColEnd, reqHasAlias := namespacing.FindNamespaceSegments(reqObj)

	// 2. Shape check. If either path isn't a namespaceable shape (end==0),
	//    or the two disagree on alias-ness (one is schema/data, the other
	//    aliases), there's nothing meaningful to rewrite — defer to plain
	//    KeyMatch5 (which returns false on a shape mismatch anyway).
	if reqColEnd == 0 || polColEnd == 0 || reqHasAlias != polHasAlias {
		return weaviateKeyMatch(reqObj, polObj)
	}

	// 3a. Fixed-ns specialization. Unqualified policy segments become
	//     "<ns>:<seg>". An already-qualified policy segment must start with
	//     "<ns>:" exactly; otherwise the policy names a *different*
	//     namespace and rewritePolicy returns ok=false → cross-namespace
	//     deny.
	if ns != "" {
		rewritten, ok := rewritePolicy(polObj, ns+schema.NamespaceSeparator, true)
		if !ok {
			return false
		}
		return weaviateKeyMatch(reqObj, rewritten)
	}

	// 3b. Any-ns widening. Unqualified policy segments become
	//     "anyNamespacePattern + <seg>" so they regex-match any single
	//     namespace prefix; already-qualified segments stay fixed.
	//
	// 4.  Final KeyMatch5, with the /shards/# carve-out so a
	//     collection-level request doesn't satisfy a per-tenant policy.
	rewritten, _ := rewritePolicy(polObj, anyNamespacePattern, false)
	return weaviateKeyMatch(reqObj, rewritten)
}

func makeNamespaceAwareMatcherFunc(namespacesEnabled bool) func(args ...any) (any, error) {
	return func(args ...any) (any, error) {
		reqObj := args[0].(string)
		polObj := args[1].(string)
		ns := args[2].(string)
		// NS-disabled empty-ns path: a ':' here is an OIDC username char, not a namespace prefix, so match plainly.
		if !namespacesEnabled && ns == "" {
			return weaviateKeyMatch(reqObj, polObj), nil
		}
		return namespaceAwareMatcher(reqObj, polObj, ns), nil
	}
}

func getVersion(path string) (string, error) {
	filePath := path + "/version"
	_, err := os.Stat(filePath)
	if err != nil { // file exists
		return DEFAULT_POLICY_VERSION, nil
	}
	b, err := os.ReadFile(filePath)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
