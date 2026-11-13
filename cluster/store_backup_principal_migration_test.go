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

package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	googleproto "google.golang.org/protobuf/proto"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
	"github.com/weaviate/weaviate/usecases/namespaces"
)

// migrationEntries returns the migration entries in st's log with an index
// above after. A migration entry is a role upsert that names the marker role.
func migrationEntries(t *testing.T, st *Store, after uint64) []cmd.CreateRolesRequest {
	t.Helper()
	first, err := st.logStore.FirstIndex()
	require.NoError(t, err)
	last, err := st.logStore.LastIndex()
	require.NoError(t, err)
	var out []cmd.CreateRolesRequest
	for i := max(first, after+1); i <= last; i++ {
		var l raft.Log
		require.NoError(t, st.logStore.GetLog(i, &l))
		var req cmd.ApplyRequest
		if l.Type != raft.LogCommand || googleproto.Unmarshal(l.Data, &req) != nil ||
			req.Type != cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS {
			continue
		}
		var sub cmd.CreateRolesRequest
		require.NoError(t, json.Unmarshal(req.SubCommand, &sub))
		if _, ok := sub.Roles[conv.BackupPrincipalsMigratedRole]; ok {
			out = append(out, sub)
		}
	}
	return out
}

// preMigrationHistory opens base without RBAC, runs writes against it and
// closes it, leaving a log of RBAC entries no RBAC store has applied.
func preMigrationHistory(t *testing.T, base MockStore, writes func(srv *Raft)) {
	t.Helper()
	srv := openMockStore(t, base)
	writes(srv)
	require.NoError(t, srv.Close(context.Background()))
}

func rbacSnapshotBlob(t *testing.T, policy, grouping [][]string) []byte {
	t.Helper()
	b, err := json.Marshal(map[string]any{"roles_policies": policy, "grouping_policies": grouping, "version": 1})
	require.NoError(t, err)
	return b
}

func TestBackupPrincipalLeaderMigration(t *testing.T) {
	movies := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Movies")})
	usersGrant := backupsGrant(t, authorization.AllBackupUsers)
	rolesGrant := backupsGrant(t, authorization.AllBackupRoles)
	readBackups := func(b *models.PermissionBackups) authorization.Policy {
		ps, err := conv.PermissionToPolicies(&models.Permission{Action: authorization.String(authorization.ReadBackups), Backups: b})
		require.NoError(t, err)
		return *ps[0]
	}
	readBooks := readBackups(&models.PermissionBackups{Collection: authorization.String("Books")})
	readUsers, readRoles := readBackups(authorization.AllBackupUsers), readBackups(authorization.AllBackupRoles)
	readData := readDataGrant(t)
	create := func(roles map[string][]authorization.Policy) func(srv *Raft) {
		return func(srv *Raft) {
			executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, &cmd.CreateRolesRequest{
				Roles: roles, Version: cmd.RBACLatestCommandPolicyVersion, RoleCreation: true,
			})
		}
	}

	t.Run("full log, no snapshot: each custom collections role gains both wildcards with its verb", func(t *testing.T) {
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		preMigrationHistory(t, base, create(map[string][]authorization.Policy{
			"operator": {movies}, "reader": {readBooks}, "plain": {readData},
		}))
		require.Zero(t, lastSnapshotIndex(base.store.snapshotStore))

		m, _ := reopenWithRBAC(t, base, t.TempDir())
		runMigration(t, m.store)

		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.ElementsMatch(t, []authorization.Policy{readBooks, readUsers, readRoles}, rolePolicies(t, m.cfg.RBAC, "reader"))
		assert.Equal(t, []authorization.Policy{readData}, rolePolicies(t, m.cfg.RBAC, "plain"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))

		entries := migrationEntries(t, m.store, 0)
		require.Len(t, entries, 1, "the migration is proposed once")
		assert.Equal(t, cmd.RBACLatestCommandPolicyVersion, entries[0].Version)
		assert.False(t, entries[0].RoleCreation)
	})

	t.Run("the loop Open starts migrates without a direct call", func(t *testing.T) {
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		preMigrationHistory(t, base, create(map[string][]authorization.Policy{"operator": {movies}}))

		m, _ := reopenWithRBAC(t, base, t.TempDir())

		require.Eventually(t, func() bool {
			ok, err := m.cfg.RBAC.BackupPrincipalsMigrated()
			return err == nil && ok
		}, 3*backupPrincipalMigrationInterval, 100*time.Millisecond)
		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
	})

	t.Run("Close returns only after the loop has exited, even mid-attempt", func(t *testing.T) {
		m, srv := reopenWithRBAC(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()), t.TempDir())
		// Holding the mutex for longer than one tick leaves the loop blocked
		// inside an attempt.
		m.store.backupPrincipalMigrationMu.Lock()
		time.Sleep(backupPrincipalMigrationInterval + time.Second)

		closed := make(chan error, 1)
		go func() { closed <- srv.Close(context.Background()) }()
		select {
		case err := <-closed:
			m.store.backupPrincipalMigrationMu.Unlock()
			t.Fatalf("Close returned while the loop was mid-attempt: %v", err)
		case <-time.After(2 * time.Second):
		}
		m.store.backupPrincipalMigrationMu.Unlock()
		require.NoError(t, <-closed)

		select {
		case <-m.store.backupPrincipalMigrationDone:
		default:
			t.Fatal("the loop is still running after Close returned")
		}
	})

	t.Run("once the marker is present, an attempt proposes nothing and the last index is unchanged", func(t *testing.T) {
		m, _ := reopenWithRBAC(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()), t.TempDir())
		runMigration(t, m.store)
		require.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
		// The leader commits a cluster ID once. Wait for it so the last index
		// stops moving.
		require.True(t, tryNTimesWithWait(20, 200*time.Millisecond, func() bool { return m.store.ClusterID() != "" }))

		before := m.store.raft.Load().LastIndex()
		proposed, err := m.store.migrateBackupPrincipalGrants()
		require.NoError(t, err)
		assert.False(t, proposed)
		assert.Equal(t, before, m.store.raft.Load().LastIndex())
	})

	t.Run("a snapshot with an RBAC section", func(t *testing.T) {
		// A user restore installs a raft snapshot. Its RBAC section holds the
		// rows and the snapshot version a node writes before the upgrade. No
		// RBAC store applies it.
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		preMigrationHistory(t, base, func(srv *Raft) {
			fs, err := srv.store.Snapshot()
			require.NoError(t, err)
			snap := fs.(*fsmSnapshot).snap
			snap.RBAC = rbacSnapshotBlob(t,
				[][]string{{conv.PrefixRoleName("operator"), movies.Resource, movies.Verb, movies.Domain}},
				[][]string{{anchorSubject, conv.PrefixRoleName("operator")}})
			b, err := json.Marshal(&snap)
			require.NoError(t, err)
			require.NoError(t, srv.store.raft.Load().Restore(&raft.SnapshotMeta{Version: raft.SnapshotVersionMax, Size: int64(len(b))}, bytes.NewReader(b), 5*time.Second))
		})
		snaps, err := base.store.snapshotStore.List()
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		policy, hasRBAC := snapshotRBACPolicies(t, base.store, snaps[0].ID)
		require.True(t, hasRBAC)
		require.Len(t, policy, 1)

		m, _ := reopenWithRBAC(t, base, t.TempDir())
		runMigration(t, m.store)

		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
	})

	t.Run("a role in a suspended namespace is skipped", func(t *testing.T) {
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		seedNamespaceInState(t, base.cfg.NamespacesController, "ns1", cmd.NamespaceStateActive)
		preMigrationHistory(t, base, create(map[string][]authorization.Policy{
			"operator": {movies}, "ns1:operator": {movies},
		}))
		// ns1 is suspended before the RBAC store opens, so neither the loop
		// nor the call below sees it active.
		require.NoError(t, base.cfg.NamespacesController.ChangeState("ns1", cmd.NamespaceStateSuspended, namespaces.StateChange{AppliedIndex: nsFlipIndex}))

		m, _ := reopenWithRBAC(t, base, t.TempDir())
		runMigration(t, m.store)

		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.Equal(t, []authorization.Policy{movies}, rolePolicies(t, m.cfg.RBAC, "ns1:operator"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
	})
}

func TestBackupPrincipalMigrationAfterRoleRestore(t *testing.T) {
	movies := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Movies")})
	usersGrant := backupsGrant(t, authorization.AllBackupUsers)
	rolesGrant := backupsGrant(t, authorization.AllBackupRoles)
	operator := conv.PrefixRoleName("operator")
	row := func(p authorization.Policy) []string { return []string{operator, p.Resource, p.Verb, p.Domain} }
	markerRow := []string{anchorSubject, conv.PrefixRoleName(conv.BackupPrincipalsMigratedRole)}

	restore := func(t *testing.T, srv *Raft, blob []byte) {
		t.Helper()
		executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_RESTORE_ROLES_AND_USERS, &cmd.RestoreRolesAndUsersRequest{Roles: blob})
	}
	// migratedStore is an RBAC store the migration has already run on.
	migratedStore := func(t *testing.T) (MockStore, *Raft) {
		t.Helper()
		m, srv := reopenWithRBAC(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()), t.TempDir())
		runMigration(t, m.store)
		require.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
		return m, srv
	}

	t.Run("a version-1 whole-store blob with tightened state and the marker keeps the removal", func(t *testing.T) {
		m, srv := migratedStore(t)
		restore(t, srv, rbacSnapshotBlob(t,
			[][]string{row(movies), row(usersGrant)},
			[][]string{{anchorSubject, operator}, markerRow}))
		runMigration(t, m.store)

		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
	})

	t.Run("an explicit-list blob from Snapshot(roles...) keeps the removal", func(t *testing.T) {
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		preMigrationHistory(t, base, func(srv *Raft) {
			executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, &cmd.CreateRolesRequest{
				Roles: map[string][]authorization.Policy{"operator": {movies}}, Version: cmd.RBACLatestCommandPolicyVersion, RoleCreation: true,
			})
		})
		m, srv := reopenWithRBAC(t, base, t.TempDir())
		runMigration(t, m.store)
		require.ElementsMatch(t, []authorization.Policy{movies, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		require.NoError(t, srv.RemovePermissions("operator", []*authorization.Policy{&rolesGrant}))
		blob, err := m.cfg.RBAC.Snapshot("operator")
		require.NoError(t, err)

		restore(t, srv, blob)
		runMigration(t, m.store)

		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
	})

	t.Run("a pre-upgrade blob regains the wildcards", func(t *testing.T) {
		m, srv := migratedStore(t)
		restore(t, srv, rbacSnapshotBlob(t, [][]string{row(movies)}, [][]string{{anchorSubject, operator}}))
		runMigration(t, m.store)

		assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
	})

	t.Run("a blob of only built-in assignments yields an entry holding only the marker", func(t *testing.T) {
		logger, _ := test.NewNullLogger()
		source, err := rbac.New(t.TempDir(), rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, logger)
		require.NoError(t, err)
		require.NoError(t, source.AddRolesForUser("db:alice", []string{authorization.Admin}))
		blob, err := source.BuiltInAssignments("alice")
		require.NoError(t, err)
		require.NotNil(t, blob)

		m, srv := migratedStore(t)
		// Read before the restore, because the loop can propose before
		// runMigration does.
		before := m.store.raft.Load().LastIndex()
		restore(t, srv, blob)
		runMigration(t, m.store)

		entries := migrationEntries(t, m.store, before)
		require.Len(t, entries, 1)
		assert.Len(t, entries[0].Roles, 1)
		assert.Empty(t, entries[0].Roles[conv.BackupPrincipalsMigratedRole])
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
		roles, err := m.cfg.RBAC.GetRolesForUserOrGroup("alice", authentication.AuthTypeDb, false)
		require.NoError(t, err)
		assert.Contains(t, roles, authorization.Admin)
	})
}
