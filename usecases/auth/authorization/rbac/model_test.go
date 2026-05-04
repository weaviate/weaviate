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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/build"
	"github.com/weaviate/weaviate/usecases/config"
)

func testKeyMatch5(t *testing.T, key1, key2 string, expected bool) {
	t.Helper()
	if result := namespaceAwareMatcher(key1, key2, ""); result != expected {
		t.Errorf("namespaceAwareMatcher(%q, %q, %q) = %v; want %v", key1, key2, "", result, expected)
	}
}

func TestKeyMatch5AuthZ(t *testing.T) {
	tests := []struct {
		name     string
		key1     string
		key2     string
		expected bool
	}{
		// Allow all
		{"Allow all roles", authorization.Roles()[0], "*", true},
		{"Allow all collections", authorization.CollectionsMetadata()[0], "*", true},
		{"Allow all collections with ABC", authorization.CollectionsMetadata("ABC")[0], "*", true},
		{"Allow all shards", authorization.ShardsMetadata("")[0], "*", true},
		{"Allow all shards with ABC", authorization.ShardsMetadata("ABC", "ABC")[0], "*", true},
		{"Allow all objects", authorization.Objects("", "", ""), "*", true},
		{"Allow all objects with Tenant1", authorization.Objects("", "Tenant1", ""), "*", true},
		{"Allow all tenants", authorization.ShardsMetadata("")[0], "*", true},
		{"Allow all tenants with ABC", authorization.ShardsMetadata("ABC", "ABC")[0], "*", true},

		// Class level
		{"Class level collections ABC", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("*", "#"), true},
		{"Class level shards ABC", authorization.ShardsMetadata("ABC")[0], conv.CasbinSchema("*", "*"), true},
		{"Class level collections ABC exact", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC", "#"), true},
		{"Class level collections Class1 exact", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1", "#"), true},
		{"Class level collections Class2 mismatch", authorization.CollectionsMetadata("Class2")[0], conv.CasbinSchema("Class1", "#"), false},
		{"Class level shards ABC TenantX", authorization.ShardsMetadata("ABC", "TenantX")[0], conv.CasbinSchema("ABC", ""), true},
		{"Class level objects ABC TenantX objectY", authorization.Objects("ABC", "TenantX", "objectY"), conv.CasbinData("ABC", "*", "*"), true},
		{"Class level tenant ABC TenantX", authorization.ShardsMetadata("ABC", "TenantX")[0], conv.CasbinSchema("ABC", ""), true},

		// Tenants level
		{"Tenants level shards", authorization.ShardsMetadata("")[0], conv.CasbinSchema("*", "*"), true},
		{"Tenants level shards ABC Tenant1", authorization.ShardsMetadata("ABC", "Tenant1")[0], conv.CasbinSchema("*", "*"), true},
		{"Tenants level shards Class1 Tenant1", authorization.ShardsMetadata("Class1", "Tenant1")[0], conv.CasbinSchema("*", "Tenant1"), true},
		{"Tenants level objects Class1 Tenant1 ObjectY", authorization.Objects("Class1", "Tenant1", "ObjectY"), conv.CasbinData("*", "Tenant1", ""), true},
		{"Tenants level shards Class1 Tenant2 mismatch", authorization.ShardsMetadata("Class1", "Tenant2")[0], conv.CasbinSchema("*", "Tenant1"), false},
		{"Tenants level shards Class1 Tenant2 mismatch 2", authorization.ShardsMetadata("Class1", "Tenant2")[0], conv.CasbinSchema("Class2", "Tenant1"), false},
		{"Tenants level shards mismatch", authorization.ShardsMetadata("")[0], conv.CasbinSchema("Class1", ""), false},
		{"Tenants level collections Class1", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1", "#"), true},
		{"Tenants level shards Class1 tenant1", authorization.ShardsMetadata("Class1", "tenant1")[0], conv.CasbinSchema("Class1", ""), true},

		// Objects level
		{"Objects level all", authorization.Objects("", "", ""), conv.CasbinData(".*", ".*", ".*"), true},
		{"Objects level ABC Tenant1", authorization.Objects("ABC", "Tenant1", ""), conv.CasbinData("*", "*", "*"), true},
		{"Objects level ABC Tenant1 exact", authorization.Objects("ABC", "Tenant1", ""), conv.CasbinData("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc exact", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("*", "Tenant1", "*"), true},
		{"Objects level ABC Tenant1 abc exact 2", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("*", "*", "abc"), true},
		{"Objects level ABC Tenant1 abc exact 3", authorization.Objects("ABC", "Tenant1", "abc"), conv.CasbinData("ABC", "Tenant1", "abc"), true},
		{"Objects level ABCD Tenant1 abc mismatch", authorization.Objects("ABCD", "Tenant1", "abc"), conv.CasbinData("ABC", "Tenant1", "abc"), false},
		{"Objects level ABC Tenant1 abcd mismatch", authorization.Objects("ABC", "Tenant1", "abcd"), conv.CasbinData("ABC", "Tenant1", "abc"), false},
		{"Objects level ABC bar abcd", authorization.Objects("ABC", "bar", "abcd"), conv.CasbinData("*", "bar", ""), true},

		// Tenants
		{"Tenants level tenant", authorization.ShardsMetadata("")[0], conv.CasbinSchema("*", "*"), true},
		{"Tenants level tenant ABC Tenant1", authorization.ShardsMetadata("ABC", "Tenant1")[0], conv.CasbinSchema("*", "*"), true},
		{"Tenants level tenant Class1 Tenant1", authorization.ShardsMetadata("Class1", "Tenant1")[0], conv.CasbinSchema("*", "Tenant1"), true},
		{"Tenants level objects Class1 Tenant1 ObjectY", authorization.Objects("Class1", "Tenant1", "ObjectY"), conv.CasbinData("*", "Tenant1", ""), true},
		{"Tenants level tenant Class1 Tenant2 mismatch", authorization.ShardsMetadata("Class1", "Tenant2")[0], conv.CasbinSchema("*", "Tenant1"), false},
		{"Tenants level tenant Class1 Tenant2 mismatch 2", authorization.ShardsMetadata("Class1", "Tenant2")[0], conv.CasbinSchema("Class2", "Tenant1"), false},
		{"Tenants level tenant mismatch", authorization.ShardsMetadata("")[0], conv.CasbinSchema("Class1", ""), false},
		{"Tenants level collections Class1", authorization.ShardsMetadata("Class1")[0], conv.CasbinSchema("Class1", ""), true},
		{"Tenants level tenant Class1 tenant1", authorization.ShardsMetadata("Class1", "tenant1")[0], conv.CasbinSchema("Class1", ""), true},

		// Regex
		{"Regex collections ABCD", authorization.CollectionsMetadata("ABCD")[0], conv.CasbinSchema("ABC", "#"), false},
		{"Regex shards ABC", authorization.ShardsMetadata("ABC", "")[0], conv.CasbinSchema("ABC", ""), true},
		{"Regex objects ABC", authorization.Objects("ABC", "", ""), conv.CasbinData("ABC", "*", "*"), true},
		{"Regex objects ABCD mismatch", authorization.Objects("ABCD", "", ""), conv.CasbinData("ABC", "*", "*"), false},
		{"Regex objects ABCD wildcard", authorization.Objects("ABCD", "", ""), conv.CasbinData("ABC.*", "*", "*"), true},
		{"Regex objects BCD mismatch", authorization.Objects("BCD", "", ""), conv.CasbinData("ABC", "*", "*"), false},
		{"Regex tenant ABC", authorization.ShardsMetadata("ABC", "")[0], conv.CasbinSchema("ABC", ""), true},

		{"Regex collections ABC wildcard", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC*", "#"), true},
		{"Regex collections ABC wildcard 2", authorization.CollectionsMetadata("ABC")[0], conv.CasbinSchema("ABC*", "#"), true},
		{"Regex collections ABCD wildcard", authorization.CollectionsMetadata("ABCD")[0], conv.CasbinSchema("ABC*", "#"), true},

		// ShardsMetadata read on collections level permissions
		{"ShardsMetadata read on collections level ABC", authorization.ShardsMetadata("ABC")[0], conv.CasbinSchema("ABC", ""), true},

		// some other cases
		{"Mismatched collection", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class2", "#"), false},
		{"Mismatched shard", authorization.ShardsMetadata("Class1", "Shard1")[0], conv.CasbinSchema("Class1", "Shard2"), false},
		{"Partial match role", authorization.Roles("anotherRole")[0], conv.CasbinRoles("ro*"), false},
		{"Partial match role", authorization.Roles("role")[0], conv.CasbinRoles("ro*"), true},
		{"Partial match collection", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Cla*", "#"), true},
		{"Partial match shard", authorization.ShardsMetadata("Class1", "Shard1")[0], conv.CasbinSchema("Class1", "Sha*"), true},
		{"Partial match object", authorization.Objects("Class1", "Shard1", "Object1"), conv.CasbinData("Class1", "Shard1", "Obj*"), true},
		{"Special character mismatch", authorization.Objects("Class1", "Shard1", "Object1"), "data/collections/Class1/shards/Shard1/objects/Object1!", false},
		{"Mismatched object", authorization.Objects("Class1", "Shard1", "Object1"), conv.CasbinData("Class1", "Shard1", "Object2"), false},
		{"Mismatched tenant", authorization.ShardsMetadata("Class1", "Tenant1")[0], conv.CasbinSchema("Class1", "Tenant2"), false},

		{"Collection check vs all shards", authorization.CollectionsMetadata("Class1")[0], conv.CasbinSchema("Class1", "*"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testKeyMatch5(t, tt.key1, tt.key2, tt.expected)
		})
	}
}

func TestKeyMatchTenant(t *testing.T) {
	tests := []struct {
		name     string
		key1     string
		key2     string
		expected bool
	}{
		// Tenant specific patterns
		{
			"Tenant specific path should not match wildcard",
			"schema/collections/Class1/shards/#",
			"schema/collections/Class1/shards/.*",
			false,
		},
		{
			"Tenant specific path should match exact #",
			"schema/collections/Class1/shards/#",
			"schema/collections/Class1/shards/#",
			true,
		},
		{
			"Regular shard should match wildcard",
			"schema/collections/Class1/shards/shard-1",
			"schema/collections/Class1/shards/.*",
			true,
		},
		{
			"Regular shard should not match tenant specific",
			"schema/collections/Class1/shards/shard-1",
			"schema/collections/Class1/shards/#",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := namespaceAwareMatcher(tt.key1, tt.key2, "")
			if result != tt.expected {
				t.Errorf("namespaceAwareMatcher(%s, %s, %q) = %v; want %v", tt.key1, tt.key2, "", result, tt.expected)
			}
		})
	}
}

func testNamespaceAwareMatcher(t *testing.T, reqObj, polObj, ns string, expected bool) {
	t.Helper()
	if got := namespaceAwareMatcher(reqObj, polObj, ns); got != expected {
		t.Errorf("namespaceAwareMatcher(%q, %q, %q) = %v; want %v", reqObj, polObj, ns, got, expected)
	}
}

func TestNamespaceAwareMatcher(t *testing.T) {
	tests := []struct {
		name     string
		reqObj   string
		polObj   string
		ns       string
		expected bool
	}{
		{
			"empty ns, qualified request, unqualified Movies* policy → match (any-ns widen)",
			"schema/collections/customer2:MoviesArchive/shards/#",
			conv.CasbinSchema("Movies*", "#"),
			"",
			true,
		},
		{
			"ns=customer1, in-ns request, unqualified Movies* policy → match (fixed-ns specialize)",
			"schema/collections/customer1:MoviesArchive/shards/#",
			conv.CasbinSchema("Movies*", "#"),
			"customer1",
			true,
		},
		{
			"ns=customer1, cross-ns request, unqualified Movies* policy → mismatch (cross-ns deny)",
			"schema/collections/customer2:MoviesArchive/shards/#",
			conv.CasbinSchema("Movies*", "#"),
			"customer1",
			false,
		},
		{
			"empty ns, qualified request, wildcard policy → match (any-ns widen)",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("*", "#"),
			"",
			true,
		},
		{
			"ns=customer1, in-ns request, wildcard policy → match (fixed-ns specialize)",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("*", "#"),
			"customer1",
			true,
		},
		{
			"ns=customer1, cross-ns request, wildcard policy → mismatch",
			"schema/collections/customer2:Movies/shards/#",
			conv.CasbinSchema("*", "#"),
			"customer1",
			false,
		},
		{
			"ns=customer1, in-ns request, exact-name unqualified policy → match",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("Movies", "#"),
			"customer1",
			true,
		},
		{
			"ns=customer1, in-ns Films request, exact-name Movies policy → mismatch",
			"schema/collections/customer1:Films/shards/#",
			conv.CasbinSchema("Movies", "#"),
			"customer1",
			false,
		},
		{
			"ns=customer1, qualified policy customer1:Movies → match",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("customer1:Movies", "#"),
			"customer1",
			true,
		},
		{
			"ns=customer1, qualified policy customer2:Movies → mismatch (cross-ns guard)",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("customer2:Movies", "#"),
			"customer1",
			false,
		},
		{
			"ns=customer1, data path, unqualified Movies* policy → match",
			"data/collections/customer1:Movies/shards/Tenant1/objects/obj-1",
			conv.CasbinData("Movies*", "*", "*"),
			"customer1",
			true,
		},
		{
			"ns=customer1, alias path with qualified col+alias, unqualified Movies/Films policy → match (both segments specialize)",
			"aliases/collections/customer1:Movies/aliases/customer1:Films",
			conv.CasbinAliases("Movies", "Films"),
			"customer1",
			true,
		},
		{
			"ns=customer1, /shards/# request, /shards/.* policy → mismatch (carve-out)",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("Movies", ""),
			"customer1",
			false,
		},
		{
			"empty ns, unqualified request, unqualified policy → passthrough match",
			"schema/collections/Movies/shards/#",
			conv.CasbinSchema("Movies", "#"),
			"",
			true,
		},
		{
			"empty ns, qualified request, exact unqualified policy → match (any-ns widen exact)",
			"schema/collections/customer1:Movies/shards/#",
			conv.CasbinSchema("Movies", "#"),
			"",
			true,
		},
		{
			"empty ns, qualified customer2 request, qualified customer1 policy → mismatch (qualified policy stays fixed)",
			"schema/collections/customer2:Movies/shards/#",
			conv.CasbinSchema("customer1:Movies", "#"),
			"",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testNamespaceAwareMatcher(t, tt.reqObj, tt.polObj, tt.ns, tt.expected)
		})
	}
}

// TestRewriteSegment locks the contract of the per-segment rewriter directly,
// so a regression in segment handling (e.g. double-prefixing, wrong cross-NS
// behavior) shows up here rather than being masked by KeyMatch5 still
// matching a malformed pattern.
func TestRewriteSegment(t *testing.T) {
	tests := []struct {
		name     string
		seg      string
		prefix   string
		fixedNs  bool
		wantOK   bool
		wantText string // builder contents on ok=true
	}{
		{
			name:     "unqualified seg, fixed-ns: prefix is prepended",
			seg:      "Movies.*",
			prefix:   "customer1:",
			fixedNs:  true,
			wantOK:   true,
			wantText: "customer1:Movies.*",
		},
		{
			name:     "unqualified seg, any-ns: regex prefix is prepended",
			seg:      "Movies.*",
			prefix:   "[^/:]+:",
			fixedNs:  false,
			wantOK:   true,
			wantText: "[^/:]+:Movies.*",
		},
		{
			name:     "qualified seg matching prefix, fixed-ns: seg verbatim",
			seg:      "customer1:Movies",
			prefix:   "customer1:",
			fixedNs:  true,
			wantOK:   true,
			wantText: "customer1:Movies",
		},
		{
			name:    "qualified seg with different namespace, fixed-ns: ok=false (cross-NS deny)",
			seg:     "customer2:Movies",
			prefix:  "customer1:",
			fixedNs: true,
			wantOK:  false,
		},
		{
			name:     "qualified seg, any-ns: seg verbatim (qualified stays fixed)",
			seg:      "customer1:Movies",
			prefix:   "[^/:]+:",
			fixedNs:  false,
			wantOK:   true,
			wantText: "customer1:Movies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var b strings.Builder
			ok := rewriteSegment(&b, tt.seg, 0, len(tt.seg), tt.prefix, tt.fixedNs)
			if ok != tt.wantOK {
				t.Fatalf("rewriteSegment ok = %v; want %v", ok, tt.wantOK)
			}
			if ok && b.String() != tt.wantText {
				t.Errorf("rewriteSegment wrote %q; want %q", b.String(), tt.wantText)
			}
		})
	}
}

// TestRewritePolicy asserts the exact rewritten string for representative
// schema/data/aliases shapes and the cross-namespace deny path on both the
// collection and alias segment.
func TestRewritePolicy(t *testing.T) {
	tests := []struct {
		name     string
		policy   string
		prefix   string
		fixedNs  bool
		wantOK   bool
		wantText string
	}{
		{
			name:     "schema path, fixed-ns specialize, unqualified col",
			policy:   "schema/collections/Movies.*/shards/#",
			prefix:   "customer1:",
			fixedNs:  true,
			wantOK:   true,
			wantText: "schema/collections/customer1:Movies.*/shards/#",
		},
		{
			name:     "data path, fixed-ns specialize, unqualified col",
			policy:   "data/collections/Movies.*/shards/.*/objects/.*",
			prefix:   "customer1:",
			fixedNs:  true,
			wantOK:   true,
			wantText: "data/collections/customer1:Movies.*/shards/.*/objects/.*",
		},
		{
			name:     "data path, any-ns widen, unqualified col",
			policy:   "data/collections/Movies.*/shards/.*/objects/.*",
			prefix:   anyNamespacePattern,
			fixedNs:  false,
			wantOK:   true,
			wantText: "data/collections/[^/:]+:Movies.*/shards/.*/objects/.*",
		},
		{
			name:     "schema path, any-ns widen, unqualified col",
			policy:   "schema/collections/Movies.*/shards/#",
			prefix:   anyNamespacePattern,
			fixedNs:  false,
			wantOK:   true,
			wantText: "schema/collections/[^/:]+:Movies.*/shards/#",
		},
		{
			name:     "schema path, fixed-ns, already-qualified matching col → policy unchanged",
			policy:   "schema/collections/customer1:Movies/shards/#",
			prefix:   "customer1:",
			fixedNs:  true,
			wantOK:   true,
			wantText: "schema/collections/customer1:Movies/shards/#",
		},
		{
			name:    "schema path, fixed-ns, already-qualified mismatching col → cross-NS deny",
			policy:  "schema/collections/customer2:Movies/shards/#",
			prefix:  "customer1:",
			fixedNs: true,
			wantOK:  false,
		},
		{
			name:     "aliases path, fixed-ns specialize, both segs unqualified",
			policy:   "aliases/collections/Movies/aliases/Films",
			prefix:   "customer1:",
			fixedNs:  true,
			wantOK:   true,
			wantText: "aliases/collections/customer1:Movies/aliases/customer1:Films",
		},
		{
			name:     "aliases path, any-ns widen, both segs unqualified",
			policy:   "aliases/collections/Movies/aliases/Films",
			prefix:   anyNamespacePattern,
			fixedNs:  false,
			wantOK:   true,
			wantText: "aliases/collections/[^/:]+:Movies/aliases/[^/:]+:Films",
		},
		{
			name:    "aliases path, fixed-ns, qualified alias names different NS → cross-NS deny on second segment",
			policy:  "aliases/collections/customer1:Movies/aliases/customer2:Films",
			prefix:  "customer1:",
			fixedNs: true,
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			colStart, colEnd, hasAlias := findNamespaceSegments(tt.policy)
			if colEnd == 0 {
				t.Fatalf("test setup: %q is not namespaceable", tt.policy)
			}
			got, ok := rewritePolicy(tt.policy, colStart, colEnd, hasAlias, tt.prefix, tt.fixedNs)
			if ok != tt.wantOK {
				t.Fatalf("rewritePolicy ok = %v; want %v (got=%q)", ok, tt.wantOK, got)
			}
			if ok && got != tt.wantText {
				t.Errorf("rewritePolicy = %q; want %q", got, tt.wantText)
			}
		})
	}
}

// freshPolicyDir creates a per-test rbac storage directory (cleaned up at
// test end). Returns the parent path that Init expects.
//
// Init writes build.Version into <dir>/rbac/version. In unit tests
// build.Version is empty, which corrupts the version file and breaks the
// minor-version parse on a second Init call against the same directory.
// Set a sentinel here so the restart-semantics tests can re-Init.
func freshPolicyDir(t *testing.T) string {
	t.Helper()
	tmp, err := os.MkdirTemp("", "rbac-init-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(tmp) })
	if build.Version == "" {
		build.Version = "1.30.0"
	}
	return tmp
}

// rolePolicies returns the resource/verb/domain rows currently registered for
// roleName.
func rolePolicies(t *testing.T, m *Manager, roleName string) [][]string {
	t.Helper()
	rows, err := m.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(roleName))
	require.NoError(t, err)
	return rows
}

// roleHasResourceVerb reports whether roleName has any policy with a
// matching resource pattern and verb.
func roleHasResourceVerb(rows [][]string, resourceContains, verb string) bool {
	for _, r := range rows {
		if strings.Contains(r[1], resourceContains) && r[2] == verb {
			return true
		}
	}
	return false
}

// TestApplyPredefinedRoles_NamespacesEnabled_AdminViewerNarrowed locks the
// per-permission registration of admin/viewer on NS-enabled clusters: their
// Casbin policy table contains the four namespace-bearing domains
// (collections/data/tenants/aliases) and nothing else.
func TestApplyPredefinedRoles_NamespacesEnabled_AdminViewerNarrowed(t *testing.T) {
	dir := freshPolicyDir(t)
	conf := rbacconf.Config{Enabled: true}
	enforcer, err := Init(conf, dir, config.Authentication{}, true)
	require.NoError(t, err)
	require.NotNil(t, enforcer)

	m := &Manager{casbin: enforcer, namespacesEnabled: true}

	adminRows := rolePolicies(t, m, authorization.Admin)
	viewerRows := rolePolicies(t, m, authorization.Viewer)
	rootRows := rolePolicies(t, m, authorization.Root)
	readOnlyRows := rolePolicies(t, m, authorization.ReadOnly)

	// admin: must contain CRUD over schema/data/aliases; tenant rows
	// share the schema domain. No backups/replicate/cluster/nodes/users/
	// roles/groups/namespaces/mcp.
	assert.True(t, roleHasResourceVerb(adminRows, "schema/collections/", authorization.CREATE))
	assert.True(t, roleHasResourceVerb(adminRows, "schema/collections/", authorization.READ))
	assert.True(t, roleHasResourceVerb(adminRows, "schema/collections/", authorization.UPDATE))
	assert.True(t, roleHasResourceVerb(adminRows, "schema/collections/", authorization.DELETE))
	assert.True(t, roleHasResourceVerb(adminRows, "data/collections/", authorization.READ))
	assert.True(t, roleHasResourceVerb(adminRows, "aliases/collections/", authorization.READ))
	for _, prohibited := range []string{
		"backups/", "cluster/", "nodes/", "users/", "roles/", "groups/", "namespaces/", "replicate/", "mcp",
	} {
		for _, row := range adminRows {
			assert.NotContains(t, row[1], prohibited, "admin (NS-enabled) must not have policy on %s domain", prohibited)
		}
	}

	// viewer: read-only over the same domains, no other verbs, no other
	// domains.
	for _, row := range viewerRows {
		assert.Equal(t, authorization.READ, row[2], "viewer (NS-enabled) must only have READ verb")
	}
	assert.True(t, roleHasResourceVerb(viewerRows, "schema/collections/", authorization.READ))
	assert.True(t, roleHasResourceVerb(viewerRows, "data/collections/", authorization.READ))

	// root and read-only keep wildcard cluster-wide policies.
	require.Len(t, rootRows, 1)
	assert.Equal(t, []string{conv.PrefixRoleName(authorization.Root), "*", conv.VALID_VERBS, "*"}, rootRows[0])
	require.Len(t, readOnlyRows, 1)
	assert.Equal(t, []string{conv.PrefixRoleName(authorization.ReadOnly), "*", authorization.READ, "*"}, readOnlyRows[0])
}

// TestApplyPredefinedRoles_RestartSurvivesAPIAssignment_NSDisabled is the
// NS-disabled control: the policy wipe-and-rebuild on every Init must not
// touch user→role groupings created via the API.
func TestApplyPredefinedRoles_RestartSurvivesAPIAssignment_NSDisabled(t *testing.T) {
	dir := freshPolicyDir(t)
	conf := rbacconf.Config{Enabled: true}

	enforcer, err := Init(conf, dir, config.Authentication{}, false)
	require.NoError(t, err)

	// API-assign admin to a DB user.
	user := conv.UserNameWithTypeFromId("alice", authentication.AuthTypeDb)
	_, err = enforcer.AddRoleForUser(user, conv.PrefixRoleName(authorization.Admin))
	require.NoError(t, err)
	require.NoError(t, enforcer.SavePolicy())

	// Simulate a restart: re-Init against the same on-disk policy CSV.
	enforcer2, err := Init(conf, dir, config.Authentication{}, false)
	require.NoError(t, err)

	roles, err := enforcer2.GetRolesForUser(user)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.Admin),
		"API-assigned admin grouping must survive restart on NS-disabled")
}

// TestApplyPredefinedRoles_RestartSurvivesAPIAssignment_NSEnabledAdmin
// covers the NS-enabled case: an API-assigned admin survives restart and
// the role policy table re-converges to the canonical narrowed shape after
// every Init.
func TestApplyPredefinedRoles_RestartSurvivesAPIAssignment_NSEnabledAdmin(t *testing.T) {
	dir := freshPolicyDir(t)
	conf := rbacconf.Config{Enabled: true}

	enforcer, err := Init(conf, dir, config.Authentication{}, true)
	require.NoError(t, err)

	beforeRows, err := enforcer.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Admin))
	require.NoError(t, err)
	require.NotEmpty(t, beforeRows)

	user := conv.UserNameWithTypeFromId("customer1:alice", authentication.AuthTypeDb)
	_, err = enforcer.AddRoleForUser(user, conv.PrefixRoleName(authorization.Admin))
	require.NoError(t, err)
	require.NoError(t, enforcer.SavePolicy())

	// Restart.
	enforcer2, err := Init(conf, dir, config.Authentication{}, true)
	require.NoError(t, err)

	roles, err := enforcer2.GetRolesForUser(user)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.Admin),
		"API-assigned admin grouping must survive restart on NS-enabled")

	afterRows, err := enforcer2.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Admin))
	require.NoError(t, err)
	assert.ElementsMatch(t, beforeRows, afterRows,
		"admin policy rows must re-converge to the canonical narrowed shape after restart")
}

// TestApplyPredefinedRoles_RestartSurvivesAPIAssignment_NSEnabledViewer
// mirrors the admin survival test for viewer.
func TestApplyPredefinedRoles_RestartSurvivesAPIAssignment_NSEnabledViewer(t *testing.T) {
	dir := freshPolicyDir(t)
	conf := rbacconf.Config{Enabled: true}

	enforcer, err := Init(conf, dir, config.Authentication{}, true)
	require.NoError(t, err)

	beforeRows, err := enforcer.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Viewer))
	require.NoError(t, err)
	require.NotEmpty(t, beforeRows)

	user := conv.UserNameWithTypeFromId("customer1:bob", authentication.AuthTypeDb)
	_, err = enforcer.AddRoleForUser(user, conv.PrefixRoleName(authorization.Viewer))
	require.NoError(t, err)
	require.NoError(t, enforcer.SavePolicy())

	enforcer2, err := Init(conf, dir, config.Authentication{}, true)
	require.NoError(t, err)

	roles, err := enforcer2.GetRolesForUser(user)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.Viewer),
		"API-assigned viewer grouping must survive restart on NS-enabled")

	afterRows, err := enforcer2.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(authorization.Viewer))
	require.NoError(t, err)
	assert.ElementsMatch(t, beforeRows, afterRows,
		"viewer policy rows must re-converge to the canonical narrowed shape after restart")
}

// TestApplyPredefinedRoles_RootReadOnlyGroupingsWipedOnRestart asserts the
// inverse of the admin/viewer survival tests: env-var-only role groupings
// (root, read-only) are wiped on every boot, so a stray grouping that isn't
// backed by config does not persist.
func TestApplyPredefinedRoles_RootReadOnlyGroupingsWipedOnRestart(t *testing.T) {
	for _, tc := range []struct {
		name string
		role string
	}{
		{"root", authorization.Root},
		{"read-only", authorization.ReadOnly},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := freshPolicyDir(t)
			conf := rbacconf.Config{Enabled: true}

			enforcer, err := Init(conf, dir, config.Authentication{}, false)
			require.NoError(t, err)

			user := conv.UserNameWithTypeFromId("alice", authentication.AuthTypeDb)
			_, err = enforcer.AddRoleForUser(user, conv.PrefixRoleName(tc.role))
			require.NoError(t, err)
			require.NoError(t, enforcer.SavePolicy())

			// Restart with the same (empty) config — the user is not in
			// RootUsers / ReadOnlyGroups, so the grouping should not be
			// re-applied.
			enforcer2, err := Init(conf, dir, config.Authentication{}, false)
			require.NoError(t, err)

			roles, err := enforcer2.GetRolesForUser(user)
			require.NoError(t, err)
			assert.NotContains(t, roles, conv.PrefixRoleName(tc.role),
				"grouping for env-var-only role %q must be wiped on restart when not present in config", tc.role)
		})
	}
}

// TestApplyPredefinedRoles_RootReadOnlyRemovedFromConfigDropAfterRestart
// asserts that when an env-var-driven assignment is dropped from config
// between boots, the next boot reflects that removal: the wipe runs, and
// the user/group is no longer in the (now-shorter) re-apply list.
func TestApplyPredefinedRoles_RootReadOnlyRemovedFromConfigDropAfterRestart(t *testing.T) {
	dir := freshPolicyDir(t)

	rootUser := "alice"
	confBefore := rbacconf.Config{
		Enabled:        true,
		RootUsers:      []string{rootUser},
		ReadOnlyGroups: []string{"auditors"},
	}
	authNconf := config.Authentication{
		APIKey: config.StaticAPIKey{Enabled: true, Users: []string{rootUser}},
	}

	enforcer, err := Init(confBefore, dir, authNconf, false)
	require.NoError(t, err)

	rootSubject := conv.UserNameWithTypeFromId(rootUser, authentication.AuthTypeDb)
	groupSubject := conv.PrefixGroupName("auditors")

	roles, err := enforcer.GetRolesForUser(rootSubject)
	require.NoError(t, err)
	require.Contains(t, roles, conv.PrefixRoleName(authorization.Root))

	roles, err = enforcer.GetRolesForUser(groupSubject)
	require.NoError(t, err)
	require.Contains(t, roles, conv.PrefixRoleName(authorization.ReadOnly))

	// Operator removes both env vars, restarts.
	confAfter := rbacconf.Config{Enabled: true}
	enforcer2, err := Init(confAfter, dir, authNconf, false)
	require.NoError(t, err)

	roles, err = enforcer2.GetRolesForUser(rootSubject)
	require.NoError(t, err)
	assert.NotContains(t, roles, conv.PrefixRoleName(authorization.Root),
		"root assignment removed from config must be gone after restart")

	roles, err = enforcer2.GetRolesForUser(groupSubject)
	require.NoError(t, err)
	assert.NotContains(t, roles, conv.PrefixRoleName(authorization.ReadOnly),
		"read-only group assignment removed from config must be gone after restart")
}

// TestApplyPredefinedRoles_AdminViewerEnvVarRemovalDoesNotPropagate locks
// the env-var add-only behaviour for admin/viewer on both NS-disabled and
// NS-enabled. Casbin grouping rows aren't tagged by source, so the wipe
// is restricted to env-var-only roles (root, read-only) to avoid
// clobbering API-assigned admin/viewer groupings — the trade-off is that
// env-var *removals* for admin/viewer don't take effect on restart.
// Operators must revoke via the API to undo an env-var-bootstrapped
// admin/viewer.
func TestApplyPredefinedRoles_AdminViewerEnvVarRemovalDoesNotPropagate(t *testing.T) {
	for _, tc := range []struct {
		name              string
		namespacesEnabled bool
	}{
		{"NS-disabled", false},
		{"NS-enabled", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := freshPolicyDir(t)

			user := "alice"
			confBefore := rbacconf.Config{
				Enabled:     true,
				AdminUsers:  []string{user},
				ViewerUsers: []string{user},
			}
			authNconf := config.Authentication{
				APIKey: config.StaticAPIKey{Enabled: true, Users: []string{user}},
			}

			enforcer, err := Init(confBefore, dir, authNconf, tc.namespacesEnabled)
			require.NoError(t, err)

			subject := conv.UserNameWithTypeFromId(user, authentication.AuthTypeDb)
			roles, err := enforcer.GetRolesForUser(subject)
			require.NoError(t, err)
			require.Contains(t, roles, conv.PrefixRoleName(authorization.Admin))
			require.Contains(t, roles, conv.PrefixRoleName(authorization.Viewer))

			// Operator drops the env vars and restarts.
			confAfter := rbacconf.Config{Enabled: true}
			enforcer2, err := Init(confAfter, dir, authNconf, tc.namespacesEnabled)
			require.NoError(t, err)

			roles, err = enforcer2.GetRolesForUser(subject)
			require.NoError(t, err)
			assert.Contains(t, roles, conv.PrefixRoleName(authorization.Admin),
				"admin env-var assignment must persist after restart even when removed from config (locked asymmetry)")
			assert.Contains(t, roles, conv.PrefixRoleName(authorization.Viewer),
				"viewer env-var assignment must persist after restart even when removed from config (locked asymmetry)")
		})
	}
}

// TestApplyPredefinedRoles_RootReadOnlyGroupingsReappliedFromConfig asserts
// the other half of the contract: env-var-driven assignments for root /
// read-only get re-applied on every boot from config, even after a wipe.
func TestApplyPredefinedRoles_RootReadOnlyGroupingsReappliedFromConfig(t *testing.T) {
	dir := freshPolicyDir(t)

	rootUser := "alice"
	readOnlyGroup := "auditors"
	conf := rbacconf.Config{
		Enabled:        true,
		RootUsers:      []string{rootUser},
		ReadOnlyGroups: []string{readOnlyGroup},
	}
	authNconf := config.Authentication{
		APIKey: config.StaticAPIKey{Enabled: true, Users: []string{rootUser}},
	}

	enforcer, err := Init(conf, dir, authNconf, false)
	require.NoError(t, err)

	rootSubject := conv.UserNameWithTypeFromId(rootUser, authentication.AuthTypeDb)
	roles, err := enforcer.GetRolesForUser(rootSubject)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.Root),
		"env-var root user must have root role on first boot")

	groupSubject := conv.PrefixGroupName(readOnlyGroup)
	roles, err = enforcer.GetRolesForUser(groupSubject)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.ReadOnly),
		"env-var read-only group must have read-only role on first boot")

	// Restart — the wipe runs, then the loops below re-add from config.
	enforcer2, err := Init(conf, dir, authNconf, false)
	require.NoError(t, err)

	roles, err = enforcer2.GetRolesForUser(rootSubject)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.Root),
		"env-var root user must still have root role after restart")

	roles, err = enforcer2.GetRolesForUser(groupSubject)
	require.NoError(t, err)
	assert.Contains(t, roles, conv.PrefixRoleName(authorization.ReadOnly),
		"env-var read-only group must still have read-only role after restart")
}
