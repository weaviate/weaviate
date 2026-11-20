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
	"errors"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/schema"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/schema/namespacing"
)

// storeAs returns the policies createRole would store for perm when submitted
// by a caller confined to ns ("" = global operator), or an error if refused. It
// runs createRole's permission checks in order except authorizeRoleScopes, so
// it assumes the caller already holds every permission perm grants.
func storeAs(ns string, perm *models.Permission) ([]authorization.Policy, error) {
	if err := conv.ValidatePermissions(true, conv.CheckCreate, perm); err != nil {
		return nil, err
	}
	name := "r"
	byRole, err := conv.RolesToPolicies(&models.Role{Name: &name, Permissions: []*models.Permission{perm}})
	if err != nil {
		return nil, err
	}
	policies := byRole[name]
	if err := conv.ValidateStorablePolicies(policies); err != nil {
		return nil, err
	}
	principal := &models.Principal{Namespace: ns, IsGlobalOperator: ns == ""}
	if err := conv.ValidateNoQualifiedNamespaceInPolicies(true, principal, policies, false); err != nil {
		return nil, err
	}
	if err := namespacing.QualifyRolePoliciesForCreate(principal, true, policies); err != nil {
		return nil, err
	}
	if conv.HasAllScopedRolePermission(policies) {
		return nil, errors.New("role grants cluster-wide role management")
	}
	return policies, nil
}

var keyMatch5BraceRe = regexp.MustCompile(`\{[^/]+\}`)

// keyMatch5Pattern is the regex casbin's KeyMatch5 compiles for a policy.
func keyMatch5Pattern(pol string) string {
	return "^" + keyMatch5BraceRe.ReplaceAllString(strings.ReplaceAll(pol, "/*", "/.*"), "[^/]+") + "$"
}

// confinedTo reports whether a caller confined to ns may be granted req. That
// holds for mcp and for a namespace-bearing shape whose every such segment
// starts with "<ns>:".
func confinedTo(req, ns string) bool {
	if req == authorization.McpDomain {
		return true
	}
	if _, end, _ := namespacing.FindNamespaceSegments(req); end == 0 {
		return false
	}
	_, err := namespacing.RewriteNamespaceSegments(req, func(seg string) (string, error) {
		if !strings.HasPrefix(seg, ns+schema.NamespaceSeparator) {
			return "", errForeignSegment
		}
		return seg, nil
	})
	return err == nil
}

// isolationRequests builds requests with the types.go builders for names
// qualified by every namespace in nss ("" = unqualified/global), plus
// cross-namespace alias pairs and the fixed operator-only and wildcard shapes.
func isolationRequests(nss []string, cls, tenant, alias, user, role string) []string {
	out := []string{
		authorization.Cluster(), authorization.Nodes("minimal")[0], authorization.Mcp(),
		authorization.Groups(authentication.AuthTypeOIDC, "g")[0], authorization.Namespaces("*")[0],
		authorization.CollectionsMetadata()[0], authorization.ShardsMetadata("")[0], authorization.Objects("", ""),
		authorization.Aliases("")[0], authorization.Users()[0], authorization.Roles()[0], authorization.Backups()[0],
	}
	for _, ns := range nss {
		q := func(n string) string { return namespacing.QualifiedName(ns, n) }
		out = append(out,
			authorization.CollectionsMetadata(q(cls))[0], authorization.ShardsMetadata(q(cls))[0],
			authorization.ShardsMetadata(q(cls), tenant)[0], authorization.CollectionsData(q(cls))[0],
			authorization.ShardsData(q(cls), tenant)[0], authorization.Aliases(q(cls))[0],
			authorization.Aliases(q(cls), q(alias))[0], authorization.Users(q(user))[0],
			authorization.Roles(q(role))[0], authorization.Backups(q(cls))[0],
			authorization.Nodes("verbose", q(cls))[0], authorization.Replications(q(cls), tenant),
			authorization.Namespaces(ns)[0], authorization.Users(user)[0],
		)
		for _, other := range nss {
			out = append(out, authorization.Aliases(q(cls), namespacing.QualifiedName(other, alias))[0])
		}
	}
	return out
}

// checkIsolation stores perm as storer and asserts that, enforced for a caller
// confined to ns, it never matches a request outside ns. It also fails when a
// stored pattern does not compile at enforce time (a validation gap).
func checkIsolation(t *testing.T, perm *models.Permission, storer, ns string, reqs []string) (stored []authorization.Policy) {
	t.Helper()
	stored, err := storeAs(storer, perm)
	if err != nil {
		return nil
	}
	match := makeNamespaceAwareMatcherFunc(true)
	for _, p := range stored {
		for _, rewritten := range enforcePatterns(p.Resource, ns) {
			if _, err := regexp.Compile(keyMatch5Pattern(rewritten)); err != nil {
				t.Fatalf("validation gap: stored %q (storer %q) compiles at create but not at enforce as %q: %v", p.Resource, storer, rewritten, err)
			}
		}
		for _, req := range reqs {
			got, err := match(req, p.Resource, ns)
			if err != nil {
				t.Fatalf("matcher error: %v", err)
			}
			if got.(bool) && !confinedTo(req, ns) {
				t.Fatalf("CROSS-NAMESPACE MATCH: ns=%q storer=%q perm=%s stored=%q request=%q",
					ns, storer, permString(perm), p.Resource, req)
			}
		}
	}
	return stored
}

// enforcePatterns lists the policy strings the matcher can hand to KeyMatch5.
func enforcePatterns(pol, ns string) []string {
	pol = strings.ReplaceAll(pol, "/*", "/.*")
	out := []string{pol}
	if r, ok := rewritePolicy(pol, ns+schema.NamespaceSeparator, true); ok {
		out = append(out, r)
	}
	r, _ := rewritePolicy(pol, anyNamespacePattern, false)
	return append(out, r)
}

// buildIsolationPerm maps a fuzz kind and up to two targets to a permission.
// An empty target leaves the field nil, so conv fills in its "*" default.
func buildIsolationPerm(kind uint8, t1, t2 string) *models.Permission {
	p := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	a, b := p(t1), p(t2)
	act := func(s string) *string { return &s }
	switch kind % 13 {
	case 0:
		return &models.Permission{Action: act(authorization.ReadCollections), Collections: &models.PermissionCollections{Collection: a}}
	case 1:
		return &models.Permission{Action: act(authorization.ReadTenants), Tenants: &models.PermissionTenants{Collection: a, Tenant: b}}
	case 2:
		return &models.Permission{Action: act(authorization.ReadData), Data: &models.PermissionData{Collection: a, Tenant: b}}
	case 3:
		return &models.Permission{Action: act(authorization.ReadAliases), Aliases: &models.PermissionAliases{Collection: a, Alias: b}}
	case 4:
		return &models.Permission{Action: act(authorization.ReadUsers), Users: &models.PermissionUsers{Users: a}}
	case 5:
		return &models.Permission{Action: act(authorization.ReadRoles), Roles: &models.PermissionRoles{Role: a, Scope: act("match")}}
	case 6:
		return &models.Permission{Action: act(authorization.ReadBackups), Backups: &models.PermissionBackups{Collection: a}}
	case 7:
		return &models.Permission{Action: act(authorization.ReadNodes), Nodes: &models.PermissionNodes{Collection: a, Verbosity: act("verbose")}}
	case 8:
		return &models.Permission{Action: act(authorization.ReadReplicate), Replicate: &models.PermissionReplicate{Collection: a, Shard: b}}
	case 9:
		return &models.Permission{Action: act(authorization.ReadCluster)}
	case 10:
		return &models.Permission{Action: act(authorization.ReadMcp)}
	case 11:
		return &models.Permission{Action: act(authorization.ReadGroups), Groups: &models.PermissionGroups{Group: a, GroupType: models.GroupTypeOidc}}
	default:
		return &models.Permission{Action: act(authorization.ManageNamespaces), Namespaces: &models.PermissionNamespaces{Namespace: a}}
	}
}

func permString(p *models.Permission) string {
	b, _ := p.MarshalBinary()
	return string(b)
}

// isolationAttacks are targets built to escape the namespace prefix or the
// confinement group. Each is tried in every target slot of every kind.
var isolationAttacks = []string{
	"", "*", "Movies", "M.*", ".*", "Movies|Secret", "X|.*", "X)|(.*", `X\)|\(.*`, `X\Q`, `X\Q)|(.*`, `X\Q|\E.*`,
	"X{.*}", "X{|}", "X{)|(}Y", "X{1,2}", "X{}", "{x}", "{.*}", "X{a}|{b}", "X{)}|{(}",
	"X(?s).*", "X(?i)movies", "(?i)a", "(?s).*", "(?m)^.*$", "X(?m)$|^.*", "X(?U).*", "(?i)ABC.*",
	`X\x3a.*`, `\x3a.*`, `.\x3a.*`, `b\x3a.*`, `X\x2f.*`, `X\x{3a}.*`, `abc\x3a.*`, `xyz\x3a.*`, `[a-z]+\x3a.*`,
	"X[:]", "X[^a]*", "X[^/]*", "[^/]*", `X[\x00-\x{10FFFF}]*`, "X[[:alpha:]]", "X[[:^alpha:]]", `X\pL`, `X\p{L}`, `X\PL.*`,
	"X^", "X$", "^", "$", "|", "X|", "|X", "^.*$", "X$|^.*", "X^|.*", "T$|^schema.*", "X$|^users/.*",
	"X()", "X(())", "X(|)", "X((|))", "X(.*", "X)", "()", "X(?:|.*)", "X(?P<n>.*)",
	"X.*Y", "X..*", `X\b`, `X\A.*`, `X\z`, `X\C*`, `\C`,
	"x:y", "b:.*", "x|b:.*", "a:x|b:y", "abc:.*", "abcd:.*", "abc:x|abcd:y", "abc:(x|.*)", "xyz:.*",
	"auth0|123", "a@b.c", "x?y", "X+", "+X", "?X", "**", "X**", `\*`, `X\*.*`, `\`, `X\`, `X\E`, `\Q\E`,
	"T1", "T)|(.*", "T|.*", "T.*", "Xy",
	strings.Repeat(".*", 128), "M" + strings.Repeat("a", 254), "M" + strings.Repeat(".", 254),
	strings.Repeat("a", 256), strings.Repeat("a", 257), "M" + strings.Repeat("|.*", 85),
}

// isolationNamespaces are valid names chosen to overlap as prefixes.
var isolationNamespaces = []string{"abc", "abcd", "ab-c", "xyz"}

var isolationNames = [][5]string{ // class, tenant, alias, user, role
	{"Movies", "T1", "MoviesAlias", "alice", "editor"},
	{"X", "T", "X", "x", "X"},
	{"Xy", "Xy", "Xy", "auth0|123", "Movies"},
}

func isolationRequestSet(extra string) []string {
	nss := append([]string{""}, isolationNamespaces...)
	var reqs []string
	for _, n := range isolationNames {
		reqs = append(reqs, isolationRequests(nss, n[0], n[1], n[2], n[3], n[4])...)
	}
	if extra != "" && !strings.Contains(extra, "/") && conv.ValidateStorableValue(extra) == nil {
		reqs = append(reqs, isolationRequests(nss, extra, extra, extra, extra, extra)...)
	}
	return reqs
}

type isolationPermArgs struct {
	kind   uint8
	t1, t2 string
}

// distinctIsolationPerms lists the buildIsolationPerm arguments for every kind and
// attack, once per distinct permission. {"*", t1} tests the second slot alone, since
// a tenant pattern such as ".*" fails the class-name rule as a collection.
func distinctIsolationPerms() []isolationPermArgs {
	var out []isolationPermArgs
	seen := map[string]bool{}
	for kind := uint8(0); kind < 13; kind++ {
		for _, t1 := range isolationAttacks {
			for _, ts := range [][2]string{{t1, ""}, {t1, t1}, {"*", t1}} {
				key := permString(buildIsolationPerm(kind, ts[0], ts[1]))
				if !seen[key] {
					seen[key] = true
					out = append(out, isolationPermArgs{kind, ts[0], ts[1]})
				}
			}
		}
	}
	return out
}

// TestNamespaceIsolationAttackSeeds stores every attack target in every slot as
// abc, as a global operator and as abcd, the neighbour sharing abc's prefix. It
// then enforces each stored policy for a caller confined to abc.
func TestNamespaceIsolationAttackSeeds(t *testing.T) {
	reqs := isolationRequestSet("")
	accepted := map[string]int{}
	ns, other := isolationNamespaces[0], isolationNamespaces[1]
	for _, a := range distinctIsolationPerms() {
		perm := buildIsolationPerm(a.kind, a.t1, a.t2)
		for _, storer := range []string{ns, "", other} {
			if checkIsolation(t, perm, storer, ns, reqs) != nil {
				accepted[storer]++
			}
		}
	}
	t.Logf("accepted permissions per storer: %v; requests per check: %d", accepted, len(reqs))

	// Root and read-only store a bare "*", and admin, viewer and metadata reader
	// store per-permission rows. No built-in policy may match outside abc.
	builtIn := []string{"*"}
	for _, perms := range authorization.BuiltInPermissionsFor(true) {
		policies, err := conv.PermissionToPolicies(perms...)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range policies {
			builtIn = append(builtIn, p.Resource)
		}
	}
	match := makeNamespaceAwareMatcherFunc(true)
	for _, pol := range builtIn {
		for _, req := range reqs {
			if got, _ := match(req, pol, ns); got.(bool) && !confinedTo(req, ns) {
				t.Fatalf("CROSS-NAMESPACE MATCH: built-in policy %q matched %q for ns %q", pol, req, ns)
			}
		}
	}

	// The controls check that the harness still grants abc its own resources and
	// keeps abcd's wildcard out.
	for _, c := range []struct {
		perm     *models.Permission
		storer   string
		req      string
		expected bool
	}{
		{buildIsolationPerm(0, "Movies", ""), "abc", authorization.CollectionsMetadata("abc:Movies")[0], true},
		{buildIsolationPerm(0, "*", ""), "", authorization.CollectionsMetadata("abc:Secret")[0], true},
		{buildIsolationPerm(2, "M.*", "T|.*"), "abc", authorization.Objects("abc:Movies", "T1"), true},
		{buildIsolationPerm(3, "*", "X|.*"), "abc", authorization.Aliases("abc:Movies", "abc:Xy")[0], true},
		{buildIsolationPerm(4, "x|.*", ""), "abc", authorization.Users("abc:alice")[0], true},
		{buildIsolationPerm(4, "abc:x|xyz:y", ""), "", authorization.Users("abc:xyz:y")[0], true},
		{buildIsolationPerm(5, "*", ""), "abc", authorization.Roles("abc:editor")[0], true},
		{buildIsolationPerm(10, "", ""), "abc", authorization.Mcp(), true},
		{buildIsolationPerm(0, "*", ""), "abcd", authorization.CollectionsMetadata("abc:Movies")[0], false},
		{buildIsolationPerm(1, "*", ".*"), "abc", authorization.ShardsMetadata("abc:Movies", "T1")[0], true},
	} {
		stored, err := storeAs(c.storer, c.perm)
		if err != nil {
			t.Fatalf("control %s refused: %v", permString(c.perm), err)
		}
		got, _ := match(c.req, stored[0].Resource, ns)
		if got.(bool) != c.expected {
			t.Errorf("control %s stored %q vs %q = %v, want %v", permString(c.perm), stored[0].Resource, c.req, got, c.expected)
		}
	}
}

// FuzzNamespaceIsolation fuzzes the permission targets, the storing caller,
// the enforcing namespace and an extra request name.
func FuzzNamespaceIsolation(f *testing.F) {
	for _, a := range distinctIsolationPerms() {
		f.Add(a.kind, a.t1, a.t2, a.kind, "", "")
	}
	f.Add(uint8(4), `b\x3a.*`, "", uint8(1), "b:alice", "bcd")
	reqsBase := isolationRequestSet("")
	f.Fuzz(func(t *testing.T, kind uint8, t1, t2 string, sel uint8, reqName, victim string) {
		ns := isolationNamespaces[int(sel>>2)%len(isolationNamespaces)]
		storer := []string{ns, "", isolationNamespaces[(int(sel>>2)+1)%len(isolationNamespaces)], ns}[sel&3]
		reqs := reqsBase
		if reqName != "" {
			reqs = append(slices.Clone(reqsBase), isolationRequestSet(reqName)[len(reqsBase):]...)
		}
		if victim != ns && schema.ValidateNamespaceNameSyntax(victim) == nil {
			reqs = append(slices.Clone(reqs), isolationRequests([]string{victim, ns, ""}, "Movies", "T1", "X", "x", "X")...)
			if sel&3 == 3 {
				storer = victim
			}
		}
		checkIsolation(t, buildIsolationPerm(kind, t1, t2), storer, ns, reqs)
	})
}
