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
	"bytes"
	"encoding/json"
	"sync"
	"testing"

	"github.com/weaviate/weaviate/usecases/auth/authentication"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/config"
)

func TestSnapshotAndRestore(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies func(*Manager) error
		wantErr       bool
	}{
		{
			name: "empty policies",
			setupPolicies: func(m *Manager) error {
				return nil
			},
		},
		{
			name: "with role and policy",
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("customAdmin"), "*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("customAdmin"))
				return err
			},
		},
		{
			name: "multiple roles and policies",
			setupPolicies: func(m *Manager) error {
				_, err := m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("customAdmin"), "*", authorization.READ, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("editor"), "collections/*", authorization.UPDATE, authorization.SchemaDomain)
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("customAdmin"))
				if err != nil {
					return err
				}
				_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("editor"))
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup logger with hook for testing
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			// Get initial policies before our test setup
			initialPolicies, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			initialGroupingPolicies, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)

			// Setup policies if needed
			if tt.setupPolicies != nil {
				err := tt.setupPolicies(m)
				require.NoError(t, err)
			}

			// Take snapshot
			snapshotData, err := m.Snapshot()
			require.NoError(t, err)
			require.NotNil(t, snapshotData)

			// Create a new manager for restore
			m2, err := setupTestManager(t, logger)
			require.NoError(t, err)

			// Restore from snapshot
			err = m2.Restore(snapshotData, false)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Get final policies after our test setup
			finalPolicies, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			finalGroupingPolicies, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)

			// Get restored policies
			restoredPolicies, err := m2.casbin.GetPolicy()
			require.NoError(t, err)
			restoredGroupingPolicies, err := m2.casbin.GetGroupingPolicy()
			require.NoError(t, err)

			// Compare only the delta of policies we added
			addedPolicies := getPolicyDelta(initialPolicies, finalPolicies)
			restoredAddedPolicies := getPolicyDelta(initialPolicies, restoredPolicies)
			assert.ElementsMatch(t, addedPolicies, restoredAddedPolicies)

			// Compare only the delta of grouping policies we added
			addedGroupingPolicies := getPolicyDelta(initialGroupingPolicies, finalGroupingPolicies)
			restoredAddedGroupingPolicies := getPolicyDelta(initialGroupingPolicies, restoredGroupingPolicies)
			assert.ElementsMatch(t, addedGroupingPolicies, restoredAddedGroupingPolicies)
		})
	}
}

// TestSnapshotRolesFilter covers Snapshot(roles...):
//   - with no args the result is the full snapshot, byte for byte
//   - naming roles keeps their p-rows (matched on p[0]) and g-rows (matched on
//     g[1]), which brings the assignments and the db:wv_internal_empty
//     placeholder along too
//   - an unknown role is an error, not a blob quietly missing a role
//   - a subset blob restores to exactly the roles that were named
func TestSnapshotRolesFilter(t *testing.T) {
	logger, _ := test.NewNullLogger()

	// seed builds a manager with two fully-populated custom roles and one role
	// that exists only through its placeholder g-row (no permissions).
	seed := func(t *testing.T) *Manager {
		m, err := setupTestManager(t, logger)
		require.NoError(t, err)
		_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("customAdmin"), "*", authorization.READ, authorization.SchemaDomain)
		require.NoError(t, err)
		_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("customAdmin"))
		require.NoError(t, err)
		_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("editor"), "collections/*", authorization.UPDATE, authorization.SchemaDomain)
		require.NoError(t, err)
		_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId("test-user", authentication.AuthTypeDb), conv.PrefixRoleName("editor"))
		require.NoError(t, err)
		_, err = m.casbin.AddRoleForUser(conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, authentication.AuthTypeDb), conv.PrefixRoleName("emptyRole"))
		require.NoError(t, err)
		return m
	}

	roleP := func(t *testing.T, m *Manager, role string) [][]string {
		p, err := m.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName(role))
		require.NoError(t, err)
		return p
	}
	roleG := func(t *testing.T, m *Manager, role string) [][]string {
		g, err := m.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName(role))
		require.NoError(t, err)
		return g
	}

	t.Run("zero args is the byte-identical full snapshot", func(t *testing.T) {
		m := seed(t)
		got, err := m.Snapshot()
		require.NoError(t, err)

		// Compare against the encoding casbin produces directly. Routing the
		// no-args path through the per-role filter would reorder and regroup the
		// rows, and it would no longer match this byte for byte.
		p, err := m.casbin.GetPolicy()
		require.NoError(t, err)
		g, err := m.casbin.GetGroupingPolicy()
		require.NoError(t, err)
		var buf bytes.Buffer
		require.NoError(t, json.NewEncoder(&buf).Encode(snapshot{Policy: p, GroupingPolicy: g, Version: SnapshotVersionLatest}))
		assert.Equal(t, buf.Bytes(), got)
	})

	t.Run("single role selects only its own p-rows and g-rows", func(t *testing.T) {
		m := seed(t)
		blob, err := m.Snapshot("customAdmin")
		require.NoError(t, err)

		var snap snapshot
		require.NoError(t, json.Unmarshal(blob, &snap))
		assert.ElementsMatch(t, roleP(t, m, "customAdmin"), snap.Policy)
		assert.ElementsMatch(t, roleG(t, m, "customAdmin"), snap.GroupingPolicy)
	})

	t.Run("empty role is selected via its placeholder g-row", func(t *testing.T) {
		m := seed(t)
		blob, err := m.Snapshot("emptyRole")
		require.NoError(t, err)

		var snap snapshot
		require.NoError(t, json.Unmarshal(blob, &snap))
		assert.Empty(t, snap.Policy)
		require.Len(t, snap.GroupingPolicy, 1)
		assert.Equal(t, conv.PrefixRoleName("emptyRole"), snap.GroupingPolicy[0][1])
	})

	t.Run("multiple roles accumulate both p-rows and g-rows", func(t *testing.T) {
		m := seed(t)
		blob, err := m.Snapshot("customAdmin", "editor")
		require.NoError(t, err)

		var snap snapshot
		require.NoError(t, json.Unmarshal(blob, &snap))
		want := append(append([][]string{}, roleP(t, m, "customAdmin")...), roleP(t, m, "editor")...)
		assert.ElementsMatch(t, want, snap.Policy)
	})

	t.Run("unknown role fails rather than shipping a partial blob", func(t *testing.T) {
		m := seed(t)
		_, err := m.Snapshot("ghost")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "not found in snapshot source")
	})

	t.Run("subset blob restores exactly the selected roles", func(t *testing.T) {
		src := seed(t)
		blob, err := src.Snapshot("customAdmin", "emptyRole")
		require.NoError(t, err)

		dst, err := setupTestManager(t, logger)
		require.NoError(t, err)
		require.NoError(t, dst.Restore(blob, false))

		assert.NotEmpty(t, roleP(t, dst, "customAdmin"))
		assert.NotEmpty(t, roleG(t, dst, "customAdmin"))
		assert.NotEmpty(t, roleG(t, dst, "emptyRole")) // placeholder survived
		// The unselected role must be absent after restoring a subset.
		assert.Empty(t, roleP(t, dst, "editor"))
		assert.Empty(t, roleG(t, dst, "editor"))
	})
}

// getPolicyDelta returns the policies that are in b but not in a
func getPolicyDelta(a, b [][]string) [][]string {
	delta := make([][]string, 0)
	for _, policyB := range b {
		found := false
		for _, policyA := range a {
			if equalPolicies(policyA, policyB) {
				found = true
				break
			}
		}
		if !found {
			delta = append(delta, policyB)
		}
	}
	return delta
}

// equalPolicies compares two policies for equality
func equalPolicies(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestStripRBACSnapshot covers the strip over a casbin blob: which namespaces
// strip (only ones a role name mentions, except on db subjects, which follow
// the configured static user list instead), that every column is rewritten
// (p[0], p[1], g[0], g[1]), and the four kinds of collision that are rejected
// rather than left for casbin to merge silently.
func TestStripRBACSnapshot(t *testing.T) {
	tests := []struct {
		name        string
		in          snapshot
		staticUsers []string // AUTHENTICATION_APIKEY_USERS on the restoring cluster
		wantP       [][]string
		wantG       [][]string
		wantErr     []string // substrings; empty means the strip must succeed
	}{
		{
			// The role, the resource, the subject and the role reference all lose
			// the ns1 prefix. The db:wv_internal_empty placeholder has no namespace,
			// so it survives and the role keeps a row.
			name: "clean single-namespace strip rewrites every column",
			in: snapshot{
				Policy: [][]string{{"role:ns1:editor", "data/collections/ns1:Movies/shards/*/objects/*", "R", "data"}},
				GroupingPolicy: [][]string{
					{"db:ns1:alice", "role:ns1:editor"},
					{"db:wv_internal_empty", "role:ns1:editor"},
				},
			},
			wantP: [][]string{{"role:editor", "data/collections/Movies/shards/*/objects/*", "R", "data"}},
			wantG: [][]string{
				{"db:alice", "role:editor"},
				{"db:wv_internal_empty", "role:editor"},
			},
		},
		{
			// An OIDC name may contain a ':' of its own, so an OIDC subject is
			// stripped only when a role names its namespace. "a" is not a namespace
			// and no role names ns2, so "oidc:a:b" and "oidc:ns2:erin" are both left
			// as they are. "db:ns2:dave" is not a configured static user, so it is a
			// dynamic user and strips whatever the role names say. Group subjects are
			// global and left alone.
			name: "an unnamed namespace survives on an OIDC subject but not on a db one",
			in: snapshot{
				Policy: [][]string{{"role:ns1:editor", "roles/ns1:editor", "R", "roles"}},
				GroupingPolicy: [][]string{
					{"oidc:ns1:carol", "role:ns1:editor"},
					{"oidc:a:b", "role:ns1:editor"},
					{"oidc:ns2:erin", "role:ns1:editor"},
					{"db:ns2:dave", "role:ns1:editor"},
					{"group:ns1:team", "role:ns1:editor"},
				},
			},
			wantP: [][]string{{"role:editor", "roles/editor", "R", "roles"}},
			wantG: [][]string{
				{"oidc:carol", "role:editor"},
				{"oidc:a:b", "role:editor"},
				{"oidc:ns2:erin", "role:editor"},
				{"db:dave", "role:editor"},
				{"group:ns1:team", "role:editor"},
			},
		},
		{
			// The subject's only role is a built-in, so no role name carries ns1 and
			// the namespace set is empty. The db subject is a dynamic user and must
			// strip anyway. A cluster with namespaces disabled refuses to start while
			// any db grouping subject is still qualified, so leaving this row would
			// turn a successful restore into a failure on the next boot.
			name: "a db subject holding only a global role still strips",
			in: snapshot{
				Policy:         [][]string{{"role:viewer", "*", "R", "*"}},
				GroupingPolicy: [][]string{{"db:ns1:alice", "role:viewer"}},
			},
			wantP: [][]string{{"role:viewer", "*", "R", "*"}},
			wantG: [][]string{{"db:alice", "role:viewer"}},
		},
		{
			// A static API key user is a global identity taken verbatim from
			// configuration, and its name may contain a ':'. Stripping it would move
			// the grant to the unrelated user "reporting", so it must survive whole.
			// No role names "svc", so the namespace set cannot save this row either:
			// only the static user list can.
			name: "a colon-bearing static user is kept whole",
			in: snapshot{
				Policy:         [][]string{{"role:viewer", "*", "R", "*"}},
				GroupingPolicy: [][]string{{"db:svc:reporting", "role:viewer"}},
			},
			staticUsers: []string{"svc:reporting", "reporting"},
			wantP:       [][]string{{"role:viewer", "*", "R", "*"}},
			wantG:       [][]string{{"db:svc:reporting", "role:viewer"}},
		},
		{
			// The conv layer round-trips a colon-bearing db id verbatim and treats it
			// as one global name, so the strip must agree with it wherever the id is
			// a configured static user. The cluster really does have a namespace
			// called customer1 here, named by the role, so the namespace set would
			// strip this subject: only the static user list keeps it whole.
			name: "a static user whose name starts with a real namespace is kept whole",
			in: snapshot{
				Policy:         [][]string{{"role:customer1:editor", "data/collections/customer1:Movies/shards/*/objects/*", "R", "data"}},
				GroupingPolicy: [][]string{{"db:customer1:alice", "role:customer1:editor"}},
			},
			staticUsers: []string{"customer1:alice"},
			wantP:       [][]string{{"role:editor", "data/collections/Movies/shards/*/objects/*", "R", "data"}},
			wantG:       [][]string{{"db:customer1:alice", "role:editor"}},
		},
		{
			// The same name is stripped once the restoring cluster does not configure
			// it, because then it can only be a dynamic user, whose own name can hold
			// no ':'. This is the rule's known limitation: a static user configured on
			// the source but not on the target loses its qualifier.
			name: "an unconfigured colon-bearing db subject strips",
			in: snapshot{
				Policy:         [][]string{{"role:ns1:editor", "data/collections/ns1:Movies/shards/*/objects/*", "R", "data"}},
				GroupingPolicy: [][]string{{"db:customer1:alice", "role:ns1:editor"}},
			},
			staticUsers: []string{"someone-else"},
			wantP:       [][]string{{"role:editor", "data/collections/Movies/shards/*/objects/*", "R", "data"}},
			wantG:       [][]string{{"db:alice", "role:editor"}},
		},
		{
			// A whole-cluster blob carries real built-in rows. They have no
			// namespace to lose, so they strip to themselves and must not be
			// reported as a collision.
			name: "built-in rows are a no-op alongside a namespaced role",
			in: snapshot{
				Policy: [][]string{
					{"role:viewer", "*", "R", "*"},
					{"role:ns1:editor", "data/collections/ns1:Movies/shards/*/objects/*", "R", "data"},
				},
				GroupingPolicy: [][]string{{"db:wv_internal_empty", "role:ns1:editor"}},
			},
			wantP: [][]string{
				{"role:viewer", "*", "R", "*"},
				{"role:editor", "data/collections/Movies/shards/*/objects/*", "R", "data"},
			},
			wantG: [][]string{{"db:wv_internal_empty", "role:editor"}},
		},
		{
			name: "two namespaces' roles with distinct perms fuse",
			in: snapshot{Policy: [][]string{
				{"role:ns1:editor", "data/collections/ns1:A", "R", "data"},
				{"role:ns2:editor", "data/collections/ns2:B", "R", "data"},
			}},
			wantErr: []string{"role:ns1:editor", "role:ns2:editor", `"editor"`},
		},
		{
			// Two different role names collapsing to one must fail even when the
			// stripped rows are identical, because casbin would dedupe them without
			// reporting anything.
			name: "two namespaces' roles with identical rows dedupe",
			in: snapshot{Policy: [][]string{
				{"role:ns1:editor", "data/collections/ns1:A", "R", "data"},
				{"role:ns2:editor", "data/collections/ns2:A", "R", "data"},
			}},
			wantErr: []string{"role:ns1:editor", "role:ns2:editor", `"editor"`},
		},
		{
			name: "a namespaced role strips onto a built-in name",
			in: snapshot{Policy: [][]string{
				{"role:ns1:viewer", "*", "R", "*"},
			}},
			wantErr: []string{"built-in role", `"viewer"`, "role:ns1:viewer"},
		},
		{
			// The two roles differ, so no role collision fires and the error can
			// only have come from the subjects.
			name: "two namespaced principals fuse into one subject",
			in: snapshot{GroupingPolicy: [][]string{
				{"db:ns1:bob", "role:ns1:editor"},
				{"db:ns2:bob", "role:ns2:auditor"},
			}},
			wantErr: []string{"db:ns1:bob", "db:ns2:bob", `"db:bob"`},
		},
		{
			// The target configures an operator key called alice, and nothing names
			// her in the blob, so no comparison between two blob rows can see this.
			// Restoring it would hand that key the namespaced user's role.
			name: "a namespaced principal strips onto a configured static user",
			in: snapshot{GroupingPolicy: [][]string{
				{"db:ns1:alice", "role:ns1:editor"},
			}},
			staticUsers: []string{"alice"},
			wantErr:     []string{"db:ns1:alice", `"alice"`, "static API key user"},
		},
		{
			// The same static user, but the blob's subject arrived unqualified, so it
			// is that same identity and the strip changed nothing. Rejecting this
			// would refuse every backup that holds a grant to an operator key.
			name: "an unqualified subject matching a static user is not a takeover",
			in: snapshot{GroupingPolicy: [][]string{
				{"db:alice", "role:ns1:editor"},
			}},
			staticUsers: []string{"alice"},
			wantG:       [][]string{{"db:alice", "role:editor"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := stripRBACSnapshot(tt.in, tt.staticUsers)
			if len(tt.wantErr) > 0 {
				require.Error(t, err)
				for _, want := range tt.wantErr {
					assert.Contains(t, err.Error(), want)
				}
				return
			}
			require.NoError(t, err)
			assert.ElementsMatch(t, tt.wantP, got.Policy)
			assert.ElementsMatch(t, tt.wantG, got.GroupingPolicy)
		})
	}
}

// TestValidateNamespaceStrip covers the coordinator's dry run: it returns the
// same collision error a real strip-restore would hit, needs no store to do it,
// and does nothing on an empty blob.
func TestValidateNamespaceStrip(t *testing.T) {
	marshal := func(t *testing.T, s snapshot) []byte {
		b, err := json.Marshal(s)
		require.NoError(t, err)
		return b
	}
	colliding := marshal(t, snapshot{Policy: [][]string{
		{"role:ns1:editor", "data/collections/ns1:A", "R", "data"},
		{"role:ns2:editor", "data/collections/ns2:B", "R", "data"},
	}})
	clean := marshal(t, snapshot{Policy: [][]string{
		{"role:ns1:editor", "data/collections/ns1:A", "R", "data"},
		{"role:ns1:auditor", "data/collections/ns1:B", "R", "data"},
	}})

	t.Run("CollidingSnapshotErrors", func(t *testing.T) {
		err := ValidateNamespaceStrip(colliding, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"editor"`)
	})
	t.Run("CleanSnapshotPasses", func(t *testing.T) {
		require.NoError(t, ValidateNamespaceStrip(clean, nil))
	})
	t.Run("EmptySnapshotIsNoOp", func(t *testing.T) {
		require.NoError(t, ValidateNamespaceStrip(nil, nil))
	})
	t.Run("MalformedSnapshotErrors", func(t *testing.T) {
		require.Error(t, ValidateNamespaceStrip([]byte("{"), nil))
	})

	// The dry run reaches the same verdict as the per-node strip only when it is
	// given the same static user list. Here "svc:reporting" and "reporting" are
	// two distinct global identities while it is configured, and become one name
	// as soon as it is not.
	t.Run("StaticUserListChangesTheVerdict", func(t *testing.T) {
		blob := marshal(t, snapshot{GroupingPolicy: [][]string{
			{"db:svc:reporting", "role:ns1:editor"},
			{"db:reporting", "role:ns1:editor"},
		}})
		require.NoError(t, ValidateNamespaceStrip(blob, []string{"svc:reporting"}))

		err := ValidateNamespaceStrip(blob, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), `"db:reporting"`)
	})
}

// TestRestoreStripNamespaces drives a stripping restore end to end through
// casbin. A namespaced backup restored with stripNamespaces=true must land the
// unqualified role, its resource and its assigned subject, and keep the
// empty-role placeholder.
func TestRestoreStripNamespaces(t *testing.T) {
	logger, _ := test.NewNullLogger()
	src, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)
	require.NoError(t, src.CreateRolesPermissions(map[string][]authorization.Policy{
		"ns1:editor": {{Resource: "data/collections/ns1:Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
	}))
	require.NoError(t, src.AddRolesForUser(conv.UserNameWithTypeFromId("ns1:alice", authentication.AuthTypeDb), []string{"ns1:editor"}))

	blob, err := src.Snapshot()
	require.NoError(t, err)

	dst, err := setupTestManager(t, logger) // namespacesEnabled=false, so Restore strips
	require.NoError(t, err)
	require.NoError(t, dst.Restore(blob, true))

	editorP, err := dst.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName("editor"))
	require.NoError(t, err)
	assert.Equal(t, [][]string{{"role:editor", "data/collections/Movies/shards/*/objects/*", "R", "data"}}, editorP)

	nsP, err := dst.casbin.GetFilteredNamedPolicy("p", 0, "role:ns1:editor")
	require.NoError(t, err)
	assert.Empty(t, nsP, "namespaced role name must not survive the strip")

	editorG, err := dst.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName("editor"))
	require.NoError(t, err)
	subjects := make([]string, len(editorG))
	for i, g := range editorG {
		subjects[i] = g[0]
	}
	assert.Contains(t, subjects, "db:alice", "stripped subject must keep its assignment")
	assert.Contains(t, subjects, "db:wv_internal_empty", "placeholder must survive")
}

// TestRestoreStripKeepsConfiguredStaticUser checks that Restore hands the
// cluster's own static API key users to the strip. A colon-bearing static user
// is a global identity, so its grant must survive a stripping restore whole.
// Pass an empty list into the strip instead and the subject becomes
// "db:reporting", handing the grant to a different user.
func TestRestoreStripKeepsConfiguredStaticUser(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dst, err := setupTestManagerWithStaticUsers(t, logger, "svc:reporting")
	require.NoError(t, err)

	blob, err := json.Marshal(snapshot{
		Policy: [][]string{{"role:ns1:editor", "data/collections/ns1:Movies/shards/*/objects/*", "R", "data"}},
		GroupingPolicy: [][]string{
			{"db:svc:reporting", "role:ns1:editor"},
			{"db:ns1:alice", "role:ns1:editor"},
		},
	})
	require.NoError(t, err)
	require.NoError(t, dst.Restore(blob, true))

	editorG, err := dst.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName("editor"))
	require.NoError(t, err)
	subjects := make([]string, len(editorG))
	for i, g := range editorG {
		subjects[i] = g[0]
	}
	assert.Contains(t, subjects, "db:svc:reporting", "a configured static user must survive whole")
	assert.NotContains(t, subjects, "db:reporting", "the grant must not move to another user")
	assert.Contains(t, subjects, "db:alice", "a dynamic user must still strip")
}

// TestRestoreStripIgnoresDisabledStaticUsers checks the strip treats a disabled
// API key configuration as having no static users. A configuration file can
// populate the list and disable API keys at once, and those names are never
// checked for format, so trusting the list here would keep a namespaced dynamic
// user qualified and make the next boot fatal.
func TestRestoreStripIgnoresDisabledStaticUsers(t *testing.T) {
	logger, _ := test.NewNullLogger()
	dst, err := setupTestManagerWithAPIKey(t, logger, config.StaticAPIKey{Enabled: false, Users: []string{"ns1:alice"}})
	require.NoError(t, err)

	blob, err := json.Marshal(snapshot{
		Policy:         [][]string{{"role:ns1:editor", "data/collections/ns1:Movies/shards/*/objects/*", "R", "data"}},
		GroupingPolicy: [][]string{{"db:ns1:alice", "role:ns1:editor"}},
	})
	require.NoError(t, err)
	require.NoError(t, dst.Restore(blob, true))

	editorG, err := dst.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName("editor"))
	require.NoError(t, err)
	subjects := make([]string, len(editorG))
	for i, g := range editorG {
		subjects[i] = g[0]
	}
	assert.Contains(t, subjects, "db:alice", "a disabled list must not keep a subject qualified")
	assert.NotContains(t, subjects, "db:ns1:alice")
}

// TestStaticAPIKeyUsers covers the accessor both strip entry points read the
// configured list through: it returns nothing while API keys are turned off.
// The per-node Restore and the coordinator's dry run share it so they cannot
// reach different verdicts on the same blob.
func TestStaticAPIKeyUsers(t *testing.T) {
	tests := []struct {
		name string
		in   config.StaticAPIKey
		want []string
	}{
		{
			name: "enabled with users",
			in:   config.StaticAPIKey{Enabled: true, Users: []string{"alice", "svc:reporting"}},
			want: []string{"alice", "svc:reporting"},
		},
		{
			name: "disabled with users",
			in:   config.StaticAPIKey{Enabled: false, Users: []string{"alice", "svc:reporting"}},
			want: nil,
		},
		{
			name: "enabled with no users",
			in:   config.StaticAPIKey{Enabled: true},
			want: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, StaticAPIKeyUsers(config.Authentication{APIKey: tt.in}))
		})
	}
}

// TestRestoreStripCollisionLeavesTargetIntact checks that a colliding strip is
// rejected without the target's policies being touched. That holds only because
// Restore calls stripRBACSnapshot before ClearPolicy. Swap the two and the
// restore wipes the target's roles and assignments, then reports the error.
// Nothing else in this suite catches that reordering.
func TestRestoreStripCollisionLeavesTargetIntact(t *testing.T) {
	logger, _ := test.NewNullLogger()
	src, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)
	// Two namespaces own a role that strips to the same name with different
	// permissions, so the strip must refuse the whole snapshot.
	require.NoError(t, src.CreateRolesPermissions(map[string][]authorization.Policy{
		"ns1:editor": {{Resource: "data/collections/ns1:Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
		"ns2:editor": {{Resource: "data/collections/ns2:Books/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
	}))

	blob, err := src.Snapshot()
	require.NoError(t, err)

	dst, err := setupTestManager(t, logger) // namespacesEnabled=false, so Restore strips
	require.NoError(t, err)
	require.NoError(t, dst.CreateRolesPermissions(map[string][]authorization.Policy{
		"incumbent": {{Resource: "data/collections/Archive/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
	}))
	require.NoError(t, dst.AddRolesForUser(conv.UserNameWithTypeFromId("resident", authentication.AuthTypeDb), []string{"incumbent"}))

	incumbentP, err := dst.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName("incumbent"))
	require.NoError(t, err)
	require.NotEmpty(t, incumbentP)
	incumbentG, err := dst.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName("incumbent"))
	require.NoError(t, err)
	require.NotEmpty(t, incumbentG)

	err = dst.Restore(blob, true)
	require.Error(t, err)
	for _, want := range []string{"role:ns1:editor", "role:ns2:editor", `"editor"`} {
		assert.Contains(t, err.Error(), want, "the error must name the collision, not just the role")
	}

	gotP, err := dst.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName("incumbent"))
	require.NoError(t, err)
	assert.ElementsMatch(t, incumbentP, gotP, "a rejected restore must not drop the target's permissions")
	gotG, err := dst.casbin.GetFilteredNamedGroupingPolicy("g", 1, conv.PrefixRoleName("incumbent"))
	require.NoError(t, err)
	assert.ElementsMatch(t, incumbentG, gotG, "a rejected restore must not drop the target's assignments")
}

// TestRestoreStripFalseUnchanged covers the RAFT path. With stripNamespaces=false
// the namespace prefixes stay intact, so RAFT snapshot restore, which always
// passes false, behaves exactly as it did before the strip was added.
func TestRestoreStripFalseUnchanged(t *testing.T) {
	logger, _ := test.NewNullLogger()
	src, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)
	require.NoError(t, src.CreateRolesPermissions(map[string][]authorization.Policy{
		"ns1:editor": {{Resource: "data/collections/ns1:Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
	}))

	blob, err := src.Snapshot()
	require.NoError(t, err)

	dst, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)
	require.NoError(t, dst.Restore(blob, false))

	nsP, err := dst.casbin.GetFilteredNamedPolicy("p", 0, "role:ns1:editor")
	require.NoError(t, err)
	assert.NotEmpty(t, nsP, "strip=false must leave the namespaced role qualified")

	strippedP, err := dst.casbin.GetFilteredNamedPolicy("p", 0, conv.PrefixRoleName("editor"))
	require.NoError(t, err)
	assert.Empty(t, strippedP, "strip=false must not synthesise a stripped role")
}

// TestManager_DeleteRoles_MultiRoleBatchPersistsAcrossReload pins that an
// already-absent role in a batch delete does not drop persistence of the roles
// (and their assignments) removed alongside it: the cleanup cascade calls
// DeleteRoles with many names, and a concurrent delete can leave one already
// gone. Reloading the policy from disk (a restart) must still see every removed
// role and assignment gone.
func TestManager_DeleteRoles_MultiRoleBatchPersistsAcrossReload(t *testing.T) {
	const (
		roleA = "batchRoleA"
		roleB = "batchRoleB"
		roleC = "batchRoleC"
	)
	allRoles := []string{roleA, roleB, roleC}
	user := conv.UserNameWithTypeFromId("batch-user", authentication.AuthTypeDb)

	tests := []struct {
		name       string
		preRemoved []string // roles deleted (and persisted) before the batch
	}{
		{name: "no pre-removed role", preRemoved: nil},
		{name: "some roles already absent", preRemoved: []string{roleB}},
		{name: "all roles already absent", preRemoved: allRoles},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			for _, r := range allRoles {
				require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
					r: {{Resource: "data/collections/Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
				}))
			}
			// Assign every role so the batch must also persist assignment removal.
			require.NoError(t, m.AddRolesForUser(user, allRoles))

			if len(tt.preRemoved) > 0 {
				require.NoError(t, m.DeleteRoles(tt.preRemoved...))
			}

			require.NoError(t, m.DeleteRoles(allRoles...))

			// Drop in-memory state and reload from the persisted policy file,
			// which is what a restart does.
			require.NoError(t, m.casbin.LoadPolicy())

			got, err := m.GetRoles()
			require.NoError(t, err)
			for _, r := range allRoles {
				_, exists := got[r]
				require.Falsef(t, exists, "role %q must stay deleted across reload", r)
			}

			assignments, err := m.GetRolesForUserOrGroup("batch-user", authentication.AuthTypeDb, false)
			require.NoError(t, err)
			require.Empty(t, assignments, "assignments must stay removed across reload")
		})
	}
}

// TestManager_CountNamespaceLocalRBAC pins the filtering: a namespace-local
// role and assignments to its direct principals count (even when the assigned
// role is global), while global roles and out-of-namespace subjects do not.
func TestManager_CountNamespaceLocalRBAC(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
		"customer1:editor": {{Resource: "data/collections/customer1:Movies/shards/*/objects/*", Verb: "R", Domain: authorization.DataDomain}},
		"auditor":          {{Resource: "data/collections/Movies/shards/*/objects/*", Verb: "R", Domain: authorization.DataDomain}},
	}))
	// Namespaced db user with a local role, namespaced oidc user with a global
	// role, and a global db user with the global role.
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("customer1:alice", authentication.AuthTypeDb), []string{"customer1:editor"}))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("customer1:carol", authentication.AuthTypeOIDC), []string{"auditor"}))
	require.NoError(t, m.AddRolesForUser(conv.UserNameWithTypeFromId("bob", authentication.AuthTypeDb), []string{"auditor"}))
	// A namespace-named group is still a global assignment: it must not count,
	// else the namespace could never be removed (the cascade leaves it).
	require.NoError(t, m.AddRolesForUser(conv.PrefixGroupName("customer1:team"), []string{"auditor"}))

	// 1 local role + 2 namespaced assignments (alice, carol); auditor + bob + group excluded.
	got, err := m.CountNamespaceLocalRBAC("customer1")
	require.NoError(t, err)
	assert.Equal(t, 3, got)

	// The cascade enumeration surfaces the two namespaced users, never the
	// namespace-named group: a group is global, so it must never be treated as a
	// namespace-local subject the cascade would try to revoke from.
	_, subjects, err := m.NamespaceLocalRBAC("customer1")
	require.NoError(t, err)
	ids := make([]string, len(subjects))
	for i, s := range subjects {
		ids[i] = s.ID
	}
	assert.ElementsMatch(t, []string{"customer1:alice", "customer1:carol"}, ids)

	other, err := m.CountNamespaceLocalRBAC("customer2")
	require.NoError(t, err)
	assert.Equal(t, 0, other)

	// Revoking a namespaced subject's global-role assignment — what the delete
	// cascade does — drives the gate down: carol holds only the global auditor
	// role, so revoking it drops the count by one.
	require.NoError(t, m.RevokeRolesForUser(conv.UserNameWithTypeFromId("customer1:carol", authentication.AuthTypeOIDC), "auditor"))
	got, err = m.CountNamespaceLocalRBAC("customer1")
	require.NoError(t, err)
	assert.Equal(t, 2, got)
}

// TestManager_NamespaceLocalRBAC_FailsClosedOnUnparseableRow pins the gate's
// fail-closed contract: an unparseable grouping subject must surface as an
// error, not be skipped — a silent undercount would let the removal-block gate
// read zero and remove a namespace while an assignment survives.
func TestManager_NamespaceLocalRBAC_FailsClosedOnUnparseableRow(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	// Inject a grouping row whose subject has no auth-type prefix, so
	// GetUserAndPrefix can't parse it.
	_, err = m.casbin.AddRoleForUser("malformed-no-prefix", conv.PrefixRoleName("auditor"))
	require.NoError(t, err)

	_, _, err = m.NamespaceLocalRBAC("customer1")
	require.Error(t, err)

	_, err = m.CountNamespaceLocalRBAC("customer1")
	require.Error(t, err)
}

func TestSnapshotNilCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := &Manager{
		casbin: nil,
		logger: logger,
	}

	snapshotData, err := m.Snapshot()
	require.NoError(t, err)
	assert.Nil(t, snapshotData)
}

func TestRestoreNilCasbin(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m := &Manager{
		casbin: nil,
		logger: logger,
	}

	err := m.Restore([]byte("{}"), false)
	require.NoError(t, err)
}

func TestRestoreInvalidData(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	// Test with invalid JSON
	err = m.Restore([]byte("invalid json"), false)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "decode json")

	// Test with empty data
	err = m.Restore([]byte("{}"), false)
	require.NoError(t, err)
}

func TestRestoreEmptyData(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	_, err = m.casbin.AddNamedPolicy("p", conv.PrefixRoleName("admin"), "*", authorization.READ, authorization.SchemaDomain)
	require.NoError(t, err)

	policies, err := m.casbin.GetPolicy()
	require.NoError(t, err)
	require.Len(t, policies, 5)

	err = m.Restore([]byte{}, false)
	require.NoError(t, err)

	// nothing overwritten
	policies, err = m.casbin.GetPolicy()
	require.NoError(t, err)
	require.Len(t, policies, 5)
}

// TestRestoreInvalidatesEnforceCache verifies that Restore() properly
// invalidates the enforce cache so that concurrent Enforce() calls during
// Restore() do not re-populate the cache with stale results that persist
// after Restore() completes.
func TestRestoreInvalidatesEnforceCache(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupTestManager(t, logger)
	require.NoError(t, err)

	principal := &models.Principal{
		Username: "cache-user",
		UserType: models.UserTypeInput(authentication.AuthTypeDb),
	}
	user := conv.UserNameWithTypeFromId("cache-user", authentication.AuthTypeDb)
	role := conv.PrefixRoleName("cache-role")
	resource := "collections/TestClass"

	// Add a policy granting READ (but no user assignment yet).
	_, err = m.casbin.AddNamedPolicy("p", role, resource, authorization.READ, authorization.SchemaDomain)
	require.NoError(t, err)

	// Snapshot before assigning the user — this snapshot has the policy but
	// no user-to-role mapping.
	data, err := m.Snapshot()
	require.NoError(t, err)

	// Now assign the user to the role and warm the enforce cache.
	_, err = m.casbin.AddRoleForUser(user, role)
	require.NoError(t, err)
	require.NoError(t, m.casbin.InvalidateCache())

	allowed, err := m.checkPermissions(principal, resource, authorization.READ)
	require.NoError(t, err)
	require.True(t, allowed)

	// Hammer checkPermissions() concurrently during Restore(). Without the
	// restoreLock in Restore() and checkPermissions(), a concurrent reader can
	// re-cache a stale "true" after LoadPolicy() clears the cache.
	const concurrentReaders = 10
	done := make(chan struct{})
	var wg sync.WaitGroup

	stopWorkers := func() {
		select {
		case <-done:
		default:
			close(done)
		}
		wg.Wait()
	}
	defer stopWorkers()

	for range concurrentReaders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				// Return values are intentionally ignored — we only care that
				// concurrent calls don't re-populate the cache with stale entries.
				m.checkPermissions(principal, resource, authorization.READ)
			}
		}()
	}

	err = m.Restore(data, false)
	require.NoError(t, err)

	stopWorkers()

	// After Restore and all concurrent readers have stopped, the user should
	// not have access. If the cache still holds a stale "true", this fails.
	allowed, err = m.checkPermissions(principal, resource, authorization.READ)
	require.NoError(t, err)
	assert.False(t, allowed, "enforce cache was not invalidated during Restore; stale cached result returned")
}

// TestPrettyPermissionsResources_NamespaceStripping exercises the pretty-
// printer used in audit logs on namespace enabled clusters: a namespace-bound
// principal sees short entity names (own namespace stripped), while a
// global principal (and any NS-disabled principal, since their Namespace
// is always "") sees the raw qualified names. A foreign prefix is left
// intact so the embedded ":" remains visible in the audit trail.
func TestPrettyPermissionsResources_NamespaceStripping(t *testing.T) {
	nsCaller := &models.Principal{Username: "customer1:alice", Namespace: "customer1"}
	globalCaller := &models.Principal{Username: "admin", IsGlobalOperator: true}

	strPtr := func(s string) *string { return &s }

	type row struct {
		domain     string
		perm       *models.Permission
		wantNS     string
		wantGlobal string
	}

	rows := []row{
		{
			domain:     "Collections",
			perm:       &models.Permission{Collections: &models.PermissionCollections{Collection: strPtr("customer1:Movies")}},
			wantNS:     "[Domain: collections, Collection: Movies]",
			wantGlobal: "[Domain: collections, Collection: customer1:Movies]",
		},
		{
			domain: "Aliases",
			perm: &models.Permission{Aliases: &models.PermissionAliases{
				Collection: strPtr("customer1:Movies"),
				Alias:      strPtr("customer1:Top10"),
			}},
			wantNS:     "[Domain: aliases, Collection: Movies, Alias: Top10]",
			wantGlobal: "[Domain: aliases, Collection: customer1:Movies, Alias: customer1:Top10]",
		},
		{
			domain:     "Users",
			perm:       &models.Permission{Users: &models.PermissionUsers{Users: strPtr("customer1:bob")}},
			wantNS:     "[Domain: users, User: bob]",
			wantGlobal: "[Domain: users, User: customer1:bob]",
		},
		{
			domain: "Replicate",
			perm: &models.Permission{Replicate: &models.PermissionReplicate{
				Collection: strPtr("customer1:Movies"),
				Shard:      strPtr("shard-1"),
			}},
			wantNS:     "[Domain: replicate, Collection: Movies, Shard: shard-1]",
			wantGlobal: "[Domain: replicate, Collection: customer1:Movies, Shard: shard-1]",
		},
		{
			domain:     "Backups",
			perm:       &models.Permission{Backups: &models.PermissionBackups{Collection: strPtr("customer1:Movies")}},
			wantNS:     "[Domain: backups,Collection: Movies]",
			wantGlobal: "[Domain: backups,Collection: customer1:Movies]",
		},
		{
			domain: "Nodes",
			perm: &models.Permission{Nodes: &models.PermissionNodes{
				Verbosity:  strPtr("verbose"),
				Collection: strPtr("customer1:Movies"),
			}},
			wantNS:     "[Domain: nodes, Verbosity: verbose, Collection: Movies]",
			wantGlobal: "[Domain: nodes, Verbosity: verbose, Collection: customer1:Movies]",
		},
		{
			domain:     "Roles",
			perm:       &models.Permission{Roles: &models.PermissionRoles{Role: strPtr("admin")}},
			wantNS:     "[Domain: roles, Role: admin]",
			wantGlobal: "[Domain: roles, Role: admin]",
		},
		{
			domain: "Tenants",
			perm: &models.Permission{Tenants: &models.PermissionTenants{
				Collection: strPtr("customer1:Movies"),
				Tenant:     strPtr("t1"),
			}},
			wantNS:     "[Domain: tenants, Collection: Movies, Tenant: t1]",
			wantGlobal: "[Domain: tenants, Collection: customer1:Movies, Tenant: t1]",
		},
		{
			// Collection nil while Tenant is set must not panic: the
			// block dereferenced *Collection while guarding only Tenant.
			domain: "Tenants_nil_collection",
			perm: &models.Permission{Tenants: &models.PermissionTenants{
				Tenant: strPtr("t1"),
			}},
			wantNS:     "[Domain: tenants, Tenant: t1]",
			wantGlobal: "[Domain: tenants, Tenant: t1]",
		},
	}

	for _, r := range rows {
		t.Run(r.domain+"/ns_caller_strips", func(t *testing.T) {
			require.Equal(t, r.wantNS, prettyPermissionsResources(nsCaller, r.perm))
		})
		t.Run(r.domain+"/global_caller_raw", func(t *testing.T) {
			require.Equal(t, r.wantGlobal, prettyPermissionsResources(globalCaller, r.perm))
		})
	}

	// Foreign-namespace prefix must remain intact so the embedded ":"
	// stays in the audit trail.
	t.Run("ns_caller_foreign_namespace_kept", func(t *testing.T) {
		perm := &models.Permission{Data: &models.PermissionData{
			Collection: strPtr("customer2:Movies"),
			Tenant:     strPtr("*"),
			Object:     strPtr("*"),
		}}
		require.Equal(t,
			"[Domain: data, Collection: customer2:Movies, Tenant: *, Object: *]",
			prettyPermissionsResources(nsCaller, perm),
		)
	})
}

func TestRemovePermissions(t *testing.T) {
	const role = "test-role"
	p1 := &authorization.Policy{Resource: "collections/Foo", Verb: authorization.READ, Domain: authorization.SchemaDomain}
	p2 := &authorization.Policy{Resource: "collections/Bar", Verb: authorization.READ, Domain: authorization.SchemaDomain}
	absentA := &authorization.Policy{Resource: "collections/AbsentA", Verb: authorization.READ, Domain: authorization.SchemaDomain}
	absentB := &authorization.Policy{Resource: "collections/AbsentB", Verb: authorization.READ, Domain: authorization.SchemaDomain}

	tests := []struct {
		name        string
		initial     []*authorization.Policy
		remove      []*authorization.Policy
		wantPresent []*authorization.Policy
		wantAbsent  []*authorization.Policy
	}{
		{
			// First permission absent must not abort the batch: P1 and P2 are
			// still requested and must be removed.
			name:       "first permission absent, rest present",
			initial:    []*authorization.Policy{p1, p2},
			remove:     []*authorization.Policy{absentA, p1, p2},
			wantAbsent: []*authorization.Policy{p1, p2},
		},
		{
			// A real removal followed by an absent permission must still be
			// persisted (SavePolicy must not be skipped).
			name:       "present then absent persists durably",
			initial:    []*authorization.Policy{p1},
			remove:     []*authorization.Policy{p1, absentA},
			wantAbsent: []*authorization.Policy{p1},
		},
		{
			name:        "all permissions absent is a no-op",
			initial:     []*authorization.Policy{p1},
			remove:      []*authorization.Policy{absentA, absentB},
			wantPresent: []*authorization.Policy{p1},
		},
		{
			name:       "all present happy path",
			initial:    []*authorization.Policy{p1, p2},
			remove:     []*authorization.Policy{p1, p2},
			wantAbsent: []*authorization.Policy{p1, p2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			initial := make([]authorization.Policy, len(tt.initial))
			for i, p := range tt.initial {
				initial[i] = *p
			}
			require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{role: initial}))

			require.NoError(t, m.RemovePermissions(role, tt.remove))

			assertPermissions := func(t *testing.T) {
				for _, p := range tt.wantAbsent {
					has, err := m.HasPermission(role, p)
					require.NoError(t, err)
					assert.False(t, has, "permission %v should be removed", p)
				}
				for _, p := range tt.wantPresent {
					has, err := m.HasPermission(role, p)
					require.NoError(t, err)
					assert.True(t, has, "permission %v should still be present", p)
				}
			}

			// In-memory state.
			assertPermissions(t)

			// Durable state: reload from the policy file. A skipped SavePolicy
			// would let removed permissions reappear here.
			require.NoError(t, m.casbin.LoadPolicy())
			assertPermissions(t)
		})
	}
}

func TestDeleteRoles(t *testing.T) {
	const (
		roleA   = "role-a"
		roleB   = "role-b"
		absentA = "absent-a"
		absentB = "absent-b"
	)
	perm := authorization.Policy{Resource: authorization.CollectionsMetadata("Foo")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}

	tests := []struct {
		name        string
		create      []string
		delete      []string
		wantAbsent  []string
		wantPresent []string
	}{
		{
			// An absent role early in the batch must not abort it: roleA and
			// roleB are still requested and must be deleted.
			name:       "absent role first, rest present",
			create:     []string{roleA, roleB},
			delete:     []string{absentA, roleA, roleB},
			wantAbsent: []string{roleA, roleB},
		},
		{
			// A real delete followed by an absent role must still be persisted
			// (SavePolicy/InvalidateCache must not be skipped).
			name:       "present then absent persists durably",
			create:     []string{roleA},
			delete:     []string{roleA, absentA},
			wantAbsent: []string{roleA},
		},
		{
			name:        "all roles absent is a no-op",
			create:      []string{roleA},
			delete:      []string{absentA, absentB},
			wantPresent: []string{roleA},
		},
		{
			name:       "all present happy path",
			create:     []string{roleA, roleB},
			delete:     []string{roleA, roleB},
			wantAbsent: []string{roleA, roleB},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			roles := make(map[string][]authorization.Policy, len(tt.create))
			for _, name := range tt.create {
				roles[name] = []authorization.Policy{perm}
			}
			require.NoError(t, m.CreateRolesPermissions(roles))

			require.NoError(t, m.DeleteRoles(tt.delete...))

			assertRoles := func(t *testing.T) {
				got, err := m.GetRoles(append(append([]string{}, tt.wantAbsent...), tt.wantPresent...)...)
				require.NoError(t, err)
				for _, name := range tt.wantAbsent {
					_, ok := got[name]
					assert.False(t, ok, "role %q should be deleted", name)
				}
				for _, name := range tt.wantPresent {
					_, ok := got[name]
					assert.True(t, ok, "role %q should still be present", name)
				}
			}

			// In-memory state.
			assertRoles(t)

			// Durable state: reload from the policy file. A skipped SavePolicy
			// would let deleted roles reappear here.
			require.NoError(t, m.casbin.LoadPolicy())
			assertRoles(t)
		})
	}
}

func TestSnapshotAndRestoreUpgrade(t *testing.T) {
	tests := []struct {
		name              string
		policiesInput     [][]string
		policiesExpected  [][]string
		groupingsInput    [][]string
		groupingsExpected [][]string
	}{
		{
			name: "assign users",
			policiesInput: [][]string{
				{"role:some_role", "users/.*", "U", "users"},
			},
			policiesExpected: [][]string{
				{"role:some_role", "users/.*", "A", "users"},
				// build-in roles are added after restore
				{"role:viewer", "*", authorization.READ, "*"},
				{"role:read-only", "*", authorization.READ, "*"},
				{"role:admin", "*", conv.VALID_VERBS, "*"},
				{"role:root", "*", conv.VALID_VERBS, "*"},
			},
		},
		{
			name: "build-in",
			policiesInput: [][]string{
				{"role:viewer", "*", "R", "*"},
				{"role:admin", "*", "(C)|(R)|(U)|(D)", "*"},
			},
			policiesExpected: [][]string{
				{"role:viewer", "*", "R", "*"},
				{"role:read-only", "*", "R", "*"},
				{"role:admin", "*", conv.VALID_VERBS, "*"},
				// build-in roles are added after restore
				{"role:root", "*", conv.VALID_VERBS, "*"},
			},
		},
		{
			name: "users",
			policiesInput: [][]string{
				{"role:admin", "*", "(C)|(R)|(U)|(D)", "*"}, // present to iterate over all roles in downgrade
			},
			policiesExpected: [][]string{
				{"role:admin", "*", "(C)|(R)|(U)|(D)|(A)", "*"},
				// build-in roles are added after restore
				{"role:viewer", "*", authorization.READ, "*"},
				{"role:read-only", "*", authorization.READ, "*"},
				{"role:root", "*", conv.VALID_VERBS, "*"},
			},
			groupingsInput: [][]string{
				{"user:test-user", "role:admin"},
			},
			groupingsExpected: [][]string{
				{"db:test-user", "role:admin"},
				{"oidc:test-user", "role:admin"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger, _ := test.NewNullLogger()
			m, err := setupTestManager(t, logger)
			require.NoError(t, err)

			sh := snapshot{Version: 0, GroupingPolicy: tt.groupingsInput, Policy: tt.policiesInput}

			bytes, err := json.Marshal(sh)
			require.NoError(t, err)

			err = m.Restore(bytes, false)
			require.NoError(t, err)

			finalPolicies, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			assert.ElementsMatch(t, finalPolicies, tt.policiesExpected)

			finalGroupingPolicies, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)
			assert.Equal(t, finalGroupingPolicies, tt.groupingsExpected)
		})
	}
}

// TestCheckPermissions_OperatorWithNamespaceTreatedAsGlobal pins that the
// enforce path derives confinement via namespacing.ConfinedNamespace: a global
// operator is unconfined even if a namespace is set on its principal, so it
// keeps access to operator-only domains. A namespace-bound (non-operator)
// principal with the same role is still denied those domains.
func TestCheckPermissions_OperatorWithNamespaceTreatedAsGlobal(t *testing.T) {
	logger, _ := test.NewNullLogger()
	m, err := setupNSEnabledTestManager(t, logger)
	require.NoError(t, err)

	const subject = "operator-user"
	_, err = m.casbin.AddRoleForUser(
		conv.UserNameWithTypeFromId(subject, authentication.AuthTypeDb),
		conv.PrefixRoleName(authorization.Root),
	)
	require.NoError(t, err)

	// cluster/* is an operator-only domain: denied to confined callers.
	resource := authorization.Cluster()

	tests := []struct {
		name      string
		principal *models.Principal
		want      bool
	}{
		{
			name:      "operator without namespace",
			principal: &models.Principal{Username: subject, UserType: models.UserTypeInputDb, IsGlobalOperator: true},
			want:      true,
		},
		{
			name:      "operator with stray namespace stays unconfined",
			principal: &models.Principal{Username: subject, UserType: models.UserTypeInputDb, IsGlobalOperator: true, Namespace: "customer1"},
			want:      true,
		},
		{
			name:      "namespaced non-operator stays confined",
			principal: &models.Principal{Username: subject, UserType: models.UserTypeInputDb, Namespace: "customer1"},
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allowed, err := m.checkPermissions(tt.principal, resource, authorization.READ)
			require.NoError(t, err)
			assert.Equal(t, tt.want, allowed)
		})
	}
}
