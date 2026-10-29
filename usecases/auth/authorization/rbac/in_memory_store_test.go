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
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	fileadapter "github.com/casbin/casbin/v2/persist/file-adapter"
	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enterrors "github.com/weaviate/weaviate/entities/errors"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	authzErrors "github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
)

// discardLogger drops every entry, so tests that authorize in a loop do not
// accumulate log entries in memory.
func discardLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(io.Discard)
	return logger
}

// dirContents maps every path under dir, relative to it, to the file's bytes.
// Directories map to an empty string.
func dirContents(t *testing.T, dir string) map[string]string {
	t.Helper()
	out := map[string]string{}
	require.NoError(t, filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			out[rel] = ""
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		out[rel] = string(b)
		return nil
	}))
	return out
}

var (
	moviesRead  = authorization.Policy{Resource: authorization.CollectionsMetadata("Movies")[0], Verb: authorization.READ, Domain: authorization.SchemaDomain}
	alice       = conv.UserNameWithTypeFromId("alice", authentication.AuthTypeDb)
	aliceAsUser = &models.Principal{Username: "alice", UserType: models.UserTypeInputDb}
)

func TestInitWithoutLegacyFiles(t *testing.T) {
	authNconf := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"root-user"}}}

	t.Run("Init on an empty dir creates nothing under it", func(t *testing.T) {
		dir := t.TempDir()
		_, err := New(dir, rbacconf.Config{Enabled: true}, authNconf, false, nil, discardLogger())
		require.NoError(t, err)
		assert.Empty(t, dirContents(t, dir))
	})

	t.Run("a config root user is authorized on the first call", func(t *testing.T) {
		m, err := New(t.TempDir(), rbacconf.Config{Enabled: true, RootUsers: []string{"root-user"}}, authNconf, false, nil, discardLogger())
		require.NoError(t, err)
		root := &models.Principal{Username: "root-user", UserType: models.UserTypeInputDb}
		require.NoError(t, m.Authorize(context.Background(), root, authorization.CREATE, authorization.CollectionsMetadata("Movies")...))
	})
}

func TestNewWithRBACDisabled(t *testing.T) {
	authNconf := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"alice"}}}

	t.Run("the store applies, snapshots and restores without writing files", func(t *testing.T) {
		dir := t.TempDir()
		m, err := New(dir, rbacconf.Config{}, authNconf, false, nil, discardLogger())
		require.NoError(t, err)

		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}}))
		require.NoError(t, m.AddRolesForUser(alice, []string{"reader"}))
		blob, err := m.Snapshot()
		require.NoError(t, err)
		require.NoError(t, m.DeleteRoles("reader"))
		require.NoError(t, m.RestoreRaftSnapshot(blob))

		roles, err := m.GetRolesForUserOrGroup("alice", authentication.AuthTypeDb, false)
		require.NoError(t, err)
		assert.Equal(t, map[string][]authorization.Policy{"reader": {moviesRead}}, roles)
		assert.Empty(t, dirContents(t, dir))
	})

	t.Run("a disabled config grants none of its users or groups", func(t *testing.T) {
		conf := rbacconf.Config{
			// A namespaced root user. Init fails on it when the conf is enabled on a namespaced cluster.
			RootUsers:      []string{"ns1:alice"},
			AdminUsers:     []string{"alice"},
			ViewerUsers:    []string{"alice"},
			RootGroups:     []string{"ops"},
			ReadOnlyGroups: []string{"auditors"},
			MetadataGroups: []string{"catalog"},
		}
		m, err := New(t.TempDir(), conf, authNconf, true, nil, discardLogger())
		require.NoError(t, err)

		groupings, err := m.casbin.GetGroupingPolicy()
		require.NoError(t, err)
		assert.Empty(t, groupings)
		roles, err := m.GetRoles()
		require.NoError(t, err)
		for _, role := range authorization.BuiltInRoles {
			assert.Contains(t, roles, role, "built-in roles are registered")
		}
	})
}

// TestMutationsWriteNoFiles runs its steps in order on one Manager, so each
// step mutates the state the previous one left.
func TestMutationsWriteNoFiles(t *testing.T) {
	dir := t.TempDir()
	m, err := New(dir, rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, discardLogger())
	require.NoError(t, err)

	restoreOwnSnapshot := func(restore func(m *Manager, blob []byte) error) func(m *Manager) error {
		return func(m *Manager) error {
			blob, err := m.Snapshot()
			if err != nil {
				return err
			}
			return restore(m, blob)
		}
	}
	steps := []struct {
		name   string
		mutate func(m *Manager) error
	}{
		{"create role", func(m *Manager) error {
			return m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}})
		}},
		{"assign role", func(m *Manager) error { return m.AddRolesForUser(alice, []string{"reader"}) }},
		{"RAFT snapshot restore", restoreOwnSnapshot(func(m *Manager, blob []byte) error { return m.RestoreRaftSnapshot(blob) })},
		{"backup restore", restoreOwnSnapshot(func(m *Manager, blob []byte) error { return m.Restore(blob, false) })},
		{"revoke role", func(m *Manager) error { return m.RevokeRolesForUser(alice, "reader") }},
		{"remove permissions", func(m *Manager) error { return m.RemovePermissions("reader", []*authorization.Policy{&moviesRead}) }},
		{"delete role", func(m *Manager) error { return m.DeleteRoles("reader") }},
	}
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			require.NoError(t, step.mutate(m))
			assert.Empty(t, dirContents(t, dir))
		})
	}
}

func TestSeedFromLegacyPolicyFile(t *testing.T) {
	// Rows as a 1.30+ node wrote them.
	const currentRows = "p, role:custom, schema/collections/Movies/shards/#, R, schema\n" +
		"g, db:wv_internal_empty, role:custom\n" +
		"g, db:alice, role:custom\n"
	// Rows as a 1.29 node wrote them: "user:" subjects and U for assign/revoke.
	const rows129 = "p, role:custom, users/*, U, users\n" +
		"g, user:alice, role:custom\n"
	authNconf := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"alice"}}}

	requireSeeded := func(t *testing.T, m *Manager) {
		t.Helper()
		require.NoError(t, m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource))
		roles, err := m.casbin.GetRolesForUser(alice)
		require.NoError(t, err)
		assert.Equal(t, []string{"role:custom"}, roles)
	}
	requireUpgraded := func(t *testing.T, m *Manager) {
		t.Helper()
		ps, err := m.casbin.GetFilteredNamedPolicy("p", 0, "role:custom")
		require.NoError(t, err)
		assert.Equal(t, [][]string{{"role:custom", "users/*", authorization.USER_AND_GROUP_ASSIGN_AND_REVOKE, authorization.UsersDomain}}, ps)
		gs, err := m.casbin.GetFilteredNamedGroupingPolicy("g", 1, "role:custom")
		require.NoError(t, err)
		assert.Equal(t, [][]string{{alice, "role:custom"}}, gs)
	}

	tests := []struct {
		name      string
		policyCSV string
		// version is the version file's content; empty means no version file.
		version string
		wantErr string
		check   func(t *testing.T, m *Manager)
	}{
		{
			name:      "a custom role and assignment are seeded",
			policyCSV: currentRows,
			version:   "1.30.0",
			check:     requireSeeded,
		},
		{
			name:      "an empty RAFT snapshot keeps the seeded state",
			policyCSV: currentRows,
			version:   "1.30.0",
			check: func(t *testing.T, m *Manager) {
				require.NoError(t, m.RestoreRaftSnapshot(nil))
				requireSeeded(t, m)
			},
		},
		{
			name:      "a version below 1.30 upgrades the rows",
			policyCSV: rows129,
			version:   "1.29.4",
			check:     requireUpgraded,
		},
		{
			name:      "version 1.30 or later leaves the rows unchanged",
			policyCSV: rows129,
			version:   "1.31.0",
			check: func(t *testing.T, m *Manager) {
				ps, err := m.casbin.GetFilteredNamedPolicy("p", 0, "role:custom")
				require.NoError(t, err)
				assert.Equal(t, [][]string{{"role:custom", "users/*", authorization.UPDATE, authorization.UsersDomain}}, ps)
				gs, err := m.casbin.GetFilteredNamedGroupingPolicy("g", 1, "role:custom")
				require.NoError(t, err)
				assert.Equal(t, [][]string{{"user:alice", "role:custom"}}, gs)
			},
		},
		{
			name:      "a missing version file acts as 1.29",
			policyCSV: rows129,
			check:     requireUpgraded,
		},
		{
			name:      "an unloadable file fails Init",
			policyCSV: "g, db:alice\n",
			version:   "1.30.0",
			wantErr:   "load legacy policy file",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			rbacDir := filepath.Join(dir, "rbac")
			require.NoError(t, os.MkdirAll(rbacDir, 0o755))
			require.NoError(t, os.WriteFile(filepath.Join(rbacDir, "policy.csv"), []byte(tt.policyCSV), 0o644))
			if tt.version != "" {
				require.NoError(t, os.WriteFile(filepath.Join(rbacDir, "version"), []byte(tt.version), 0o644))
			}
			before := dirContents(t, dir)

			m, err := New(dir, rbacconf.Config{Enabled: true}, authNconf, false, nil, discardLogger())
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				tt.check(t, m)
			}
			assert.Equal(t, before, dirContents(t, dir), "the seed must not write")
		})
	}
}

func TestRestoreRaftSnapshotRemovesLegacyFiles(t *testing.T) {
	const seededRows = "p, role:custom, schema/collections/Movies/shards/#, R, schema\n" +
		"g, db:alice, role:custom\n"
	authNconf := config.Authentication{APIKey: config.StaticAPIKey{Enabled: true, Users: []string{"alice"}}}

	// rbacSnapshot holds a role "writer" and no assignment for alice, so a
	// restore of it replaces the seeded state.
	source, err := New(t.TempDir(), rbacconf.Config{Enabled: true}, authNconf, false, nil, discardLogger())
	require.NoError(t, err)
	require.NoError(t, source.CreateRolesPermissions(map[string][]authorization.Policy{"writer": {moviesRead}}))
	rbacSnapshot, err := source.Snapshot()
	require.NoError(t, err)
	// The one-field grouping row passes validation and fails building role links,
	// so the restore fails inside replacePolicies.
	failingSnapshot, err := json.Marshal(snapshot{
		GroupingPolicy: [][]string{{"db:malformed"}},
		Version:        SnapshotVersionLatest,
	})
	require.NoError(t, err)

	requireSeeded := func(t *testing.T, m *Manager) {
		t.Helper()
		require.NoError(t, m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource))
	}
	requireRestored := func(t *testing.T, m *Manager) {
		t.Helper()
		roles, err := m.GetRoles()
		require.NoError(t, err)
		assert.Contains(t, roles, "writer")
		assert.NotContains(t, roles, "custom")
		err = m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource)
		require.ErrorAs(t, err, new(authzErrors.Forbidden))
	}

	tests := []struct {
		name        string
		plantFiles  bool
		snapshot    []byte
		backup      bool
		wantErr     bool
		wantKept    bool
		checkMemory func(t *testing.T, m *Manager)
	}{
		{
			name:        "an RBAC snapshot removes both files",
			plantFiles:  true,
			snapshot:    rbacSnapshot,
			checkMemory: requireRestored,
		},
		{
			name:        "an empty snapshot keeps them",
			plantFiles:  true,
			wantKept:    true,
			checkMemory: requireSeeded,
		},
		{
			name:       "a failed restore keeps them",
			plantFiles: true,
			snapshot:   failingSnapshot,
			wantErr:    true,
			wantKept:   true,
		},
		{
			name:        "a backup restore keeps them",
			plantFiles:  true,
			snapshot:    rbacSnapshot,
			backup:      true,
			wantKept:    true,
			checkMemory: requireRestored,
		},
		{
			name:        "no files present is a no-op",
			snapshot:    rbacSnapshot,
			checkMemory: requireRestored,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			if tt.plantFiles {
				rbacDir := filepath.Join(dir, "rbac")
				require.NoError(t, os.MkdirAll(rbacDir, 0o755))
				require.NoError(t, os.WriteFile(filepath.Join(rbacDir, "policy.csv"), []byte(seededRows), 0o644))
				require.NoError(t, os.WriteFile(filepath.Join(rbacDir, "version"), []byte("1.30.0"), 0o644))
			}
			before := dirContents(t, dir)
			m, err := New(dir, rbacconf.Config{Enabled: true}, authNconf, false, nil, discardLogger())
			require.NoError(t, err)
			if tt.plantFiles {
				requireSeeded(t, m)
			}

			if tt.backup {
				err = m.Restore(tt.snapshot, false)
			} else {
				err = m.RestoreRaftSnapshot(tt.snapshot)
			}
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			switch {
			case tt.wantKept:
				assert.Equal(t, before, dirContents(t, dir))
			case tt.plantFiles:
				// The rbac dir stays; only the two files go.
				assert.Equal(t, map[string]string{"rbac": ""}, dirContents(t, dir))
			default:
				assert.Empty(t, dirContents(t, dir))
			}
			if tt.checkMemory != nil {
				tt.checkMemory(t, m)
			}
		})
	}

	t.Run("a removal error does not fail the restore", func(t *testing.T) {
		dir := t.TempDir()
		logger, hook := test.NewNullLogger()
		m, err := New(dir, rbacconf.Config{Enabled: true}, authNconf, false, nil, logger)
		require.NoError(t, err)
		// os.Remove fails on a non-empty dir with an error other than not-exist.
		blocked := filepath.Join(dir, "rbac", "policy.csv")
		require.NoError(t, os.MkdirAll(blocked, 0o755))
		require.NoError(t, os.WriteFile(filepath.Join(blocked, "x"), nil, 0o644))

		require.NoError(t, m.RestoreRaftSnapshot(rbacSnapshot))

		requireRestored(t, m)
		warned := slices.ContainsFunc(hook.AllEntries(), func(e *logrus.Entry) bool {
			return e.Level == logrus.WarnLevel && e.Data["path"] == blocked
		})
		assert.True(t, warned, "the removal error is logged with its path")
	})
}

func TestRestoreRaftSnapshotDropsRevokedAssignment(t *testing.T) {
	requireAliceDenied := func(t *testing.T, m *Manager) {
		t.Helper()
		err := m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource)
		require.ErrorAs(t, err, new(authzErrors.Forbidden))
		roles, err := m.GetRolesForUserOrGroup("alice", authentication.AuthTypeDb, false)
		require.NoError(t, err)
		assert.Empty(t, roles)
	}

	t.Run("current snapshot", func(t *testing.T) {
		m, err := setupTestManager(t, discardLogger())
		require.NoError(t, err)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}}))
		withoutAssignment, err := m.Snapshot()
		require.NoError(t, err)
		require.NoError(t, m.AddRolesForUser(alice, []string{"reader"}))
		require.NoError(t, m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource))

		require.NoError(t, m.RestoreRaftSnapshot(withoutAssignment))

		requireAliceDenied(t, m)
	})

	t.Run("V0 snapshot upgraded from 1.29", func(t *testing.T) {
		// upgradeGroupingsFrom129 reads each role's users from the role manager and
		// re-adds every "user:" link it finds: as a db: row for a configured API-key
		// user, and as an oidc: row when OIDC is enabled. A stale link still in the
		// role manager when the upgrade runs becomes a grant.
		m, err := setupTestManagerWithStaticUsers(t, discardLogger(), "alice")
		require.NoError(t, err)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}}))
		blob, err := m.Snapshot()
		require.NoError(t, err)
		var v0 snapshot
		require.NoError(t, json.Unmarshal(blob, &v0))
		v0.Version = SnapshotVersionV0
		withoutAssignment, err := json.Marshal(v0)
		require.NoError(t, err)
		require.NoError(t, m.AddRolesForUser("user:alice", []string{"reader"}))

		require.NoError(t, m.RestoreRaftSnapshot(withoutAssignment))

		requireAliceDenied(t, m)
	})
}

func TestRestoreRaftSnapshotUnderConcurrentAuthorize(t *testing.T) {
	// hammer authorizes alice from several goroutines until the returned stop
	// is called; stop fails t if a reader panicked and returns how many calls
	// were denied.
	hammer := func(t *testing.T, m *Manager) (stop func() int64) {
		var denied atomic.Int64
		done := make(chan struct{})
		var exited []chan error
		for range 8 {
			exited = append(exited, enterrors.GoWrapperWithErrorCh(func() {
				for {
					select {
					case <-done:
						return
					default:
					}
					if m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource) != nil {
						denied.Add(1)
					}
				}
			}, m.logger))
		}
		return func() int64 {
			close(done)
			for _, ch := range exited {
				assert.NoError(t, <-ch, "a reader panicked and stopped counting denials")
			}
			return denied.Load()
		}
	}

	t.Run("no deny for a granted role during repeated restores", func(t *testing.T) {
		m, err := setupTestManager(t, discardLogger())
		require.NoError(t, err)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}}))
		require.NoError(t, m.AddRolesForUser(alice, []string{"reader"}))
		blob, err := m.Snapshot()
		require.NoError(t, err)

		stop := hammer(t, m)
		for range 50 {
			if err := m.RestoreRaftSnapshot(blob); err != nil {
				stop()
				require.NoError(t, err)
			}
		}
		assert.Zero(t, stop(), "a reader saw the granted role denied during a restore")
	})

	t.Run("a cached allow does not survive a restore that revokes the grant", func(t *testing.T) {
		m, err := setupTestManager(t, discardLogger())
		require.NoError(t, err)
		require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}}))
		withoutAssignment, err := m.Snapshot()
		require.NoError(t, err)
		require.NoError(t, m.AddRolesForUser(alice, []string{"reader"}))
		require.NoError(t, m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource))

		stop := hammer(t, m)
		err = m.RestoreRaftSnapshot(withoutAssignment)
		stop()
		require.NoError(t, err)

		err = m.Authorize(context.Background(), aliceAsUser, authorization.READ, moviesRead.Resource)
		require.ErrorAs(t, err, new(authzErrors.Forbidden))
	})
}

// TestRollbackRegeneratesPolicyFileFromSnapshot checks that a release which
// still writes policy.csv rebuilds the same rows from this release's RAFT
// snapshot: it replays the rows, saves them through the file adapter and
// loads them back.
func TestRollbackRegeneratesPolicyFileFromSnapshot(t *testing.T) {
	tests := []struct {
		name              string
		namespacesEnabled bool
		setup             func(t *testing.T, m *Manager)
		// wantGrouping is a row setup creates, so the comparison is never
		// between empty sets.
		wantGrouping []string
	}{
		{
			name: "custom role with assignment",
			setup: func(t *testing.T, m *Manager) {
				require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{"reader": {moviesRead}}))
				require.NoError(t, m.AddRolesForUser(alice, []string{"reader"}))
			},
			wantGrouping: []string{alice, "role:reader"},
		},
		{
			name:              "namespaced role",
			namespacesEnabled: true,
			setup: func(t *testing.T, m *Manager) {
				require.NoError(t, m.CreateRolesPermissions(map[string][]authorization.Policy{
					"ns1:editor": {{Resource: "data/collections/ns1:Movies/shards/*/objects/*", Verb: authorization.READ, Domain: authorization.DataDomain}},
				}))
				require.NoError(t, m.AddRolesForUser("db:ns1:bob", []string{"ns1:editor"}))
			},
			wantGrouping: []string{"db:ns1:bob", "role:ns1:editor"},
		},
		{
			name: "built-in admin assignment",
			setup: func(t *testing.T, m *Manager) {
				require.NoError(t, m.AddRolesForUser("db:carol", []string{authorization.Admin}))
			},
			wantGrouping: []string{"db:carol", conv.PrefixRoleName(authorization.Admin)},
		},
		{
			name: "OIDC group",
			setup: func(t *testing.T, m *Manager) {
				require.NoError(t, m.AddRolesForUser(conv.PrefixGroupName("devs"), []string{authorization.Viewer}))
			},
			wantGrouping: []string{conv.PrefixGroupName("devs"), conv.PrefixRoleName(authorization.Viewer)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var (
				m   *Manager
				err error
			)
			if tt.namespacesEnabled {
				m, err = setupNSEnabledTestManager(t, discardLogger(), "ns1")
			} else {
				m, err = setupTestManager(t, discardLogger())
			}
			require.NoError(t, err)
			tt.setup(t, m)

			blob, err := m.Snapshot()
			require.NoError(t, err)
			var s snapshot
			require.NoError(t, json.Unmarshal(blob, &s))

			path := filepath.Join(t.TempDir(), "policy.csv")
			writer := newModelEnforcer(t)
			_, err = writer.AddPolicies(s.Policy)
			require.NoError(t, err)
			_, err = writer.AddGroupingPolicies(s.GroupingPolicy)
			require.NoError(t, err)
			writer.SetAdapter(fileadapter.NewAdapter(path))
			require.NoError(t, writer.SavePolicy())

			reader := newModelEnforcer(t)
			reader.SetAdapter(fileadapter.NewAdapter(path))
			require.NoError(t, reader.LoadPolicy())

			wantP, err := m.casbin.GetPolicy()
			require.NoError(t, err)
			gotP, err := reader.GetPolicy()
			require.NoError(t, err)
			assert.ElementsMatch(t, wantP, gotP)

			wantG, err := m.casbin.GetGroupingPolicy()
			require.NoError(t, err)
			gotG, err := reader.GetGroupingPolicy()
			require.NoError(t, err)
			assert.ElementsMatch(t, wantG, gotG)
			assert.Contains(t, gotG, tt.wantGrouping)
		})
	}
}

// newModelEnforcer builds an enforcer on the RBAC model with no adapter.
func newModelEnforcer(t *testing.T) *casbin.Enforcer {
	t.Helper()
	m, err := model.NewModelFromString(MODEL)
	require.NoError(t, err)
	e, err := casbin.NewEnforcer(m)
	require.NoError(t, err)
	return e
}
