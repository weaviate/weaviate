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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

// Most tests in this file, store_backup_principal_migration_test.go and
// store_rbac_replay_base_test.go build pre-migration history on a
// NewMockStore. Its cfg.RBAC is unset, so it logs RBAC entries, applies them
// as no-ops, and never runs the migration. These tests then reopen the same
// raft directories with rbacMockStore.
//
// Tests that need no pre-migration history start on rbacMockStore.
//
// An rbacMockStore runs its migration loop alongside any direct
// migrateBackupPrincipalGrants call. Every assertion holds no matter which of
// the two proposed.

// rbacMockStore is NewMockStore with a real RBAC store under rbacDir, built
// as production builds it: NewFSM, then its address.
func rbacMockStore(t *testing.T, m MockStore, rbacDir string) MockStore {
	t.Helper()
	authZ, err := rbac.New(rbacDir, rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, m.logger)
	require.NoError(t, err)
	m.cfg.RBAC = authZ
	s := NewFSM(m.cfg, prometheus.NewPedanticRegistry())
	s.schemaManager.SetReplicationFSM(m.replicationFSM)
	m.store = &s
	return m
}

// writeLegacyPolicy fabricates the policy.csv and version files a pre-1.41
// release left under rbacDir, for rbac.New's legacy seed to load.
func writeLegacyPolicy(t *testing.T, rbacDir, version string, rows ...string) {
	t.Helper()
	legacyDir := filepath.Join(rbacDir, "rbac")
	require.NoError(t, os.MkdirAll(legacyDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(legacyDir, "policy.csv"), []byte(strings.Join(rows, "\n")+"\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(legacyDir, "version"), []byte(version), 0o600))
}

func openMockStore(t *testing.T, m MockStore) *Raft {
	t.Helper()
	ctx := context.Background()
	srv := NewRaft(mocks.NewMockNodeSelector(), m.store, nil)
	m.indexer.On("Open", Anything).Return(nil)
	m.indexer.On("TriggerSchemaUpdateCallbacks").Return().Maybe()
	m.indexer.On("Close", Anything).Return(nil).Maybe()
	require.NoError(t, srv.Open(ctx, m.indexer))
	require.NoError(t, srv.store.Notify(m.cfg.NodeID, fmt.Sprintf("%s:%d", m.cfg.Host, m.cfg.RaftPort)))
	require.NoError(t, srv.WaitUntilDBRestored(ctx, time.Second, make(chan struct{})))
	require.True(t, tryNTimesWithWait(20, 200*time.Millisecond, srv.store.IsLeader))
	require.True(t, tryNTimesWithWait(10, 200*time.Millisecond, srv.Ready))
	return srv
}

// reopenWithRBAC opens base's raft directories again with a real RBAC store
// under rbacDir, and closes it when the test ends.
func reopenWithRBAC(t *testing.T, base MockStore, rbacDir string) (MockStore, *Raft) {
	t.Helper()
	m := rbacMockStore(t, base, rbacDir)
	srv := openMockStore(t, m)
	t.Cleanup(func() { require.NoError(t, srv.Close(context.Background())) })
	return m, srv
}

// executeRoleWrite commits a role write through raft, as a node on either
// side of the upgrade would.
func executeRoleWrite(t *testing.T, srv *Raft, typ cmd.ApplyRequest_Type, req any) {
	t.Helper()
	sub, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = srv.Execute(context.Background(), &cmd.ApplyRequest{Type: typ, SubCommand: sub})
	require.NoError(t, err)
}

// leaderMigration builds the entry migrateBackupPrincipalGrants proposes for
// roles, so a test can write it into a log by hand.
func leaderMigration(roles map[string][]authorization.Policy) *cmd.CreateRolesRequest {
	req := &cmd.CreateRolesRequest{Roles: map[string][]authorization.Policy{conv.BackupPrincipalsMigratedRole: {}}, Version: cmd.RBACLatestCommandPolicyVersion}
	for name, policies := range roles {
		req.Roles[name] = policies
	}
	return req
}

func runMigration(t *testing.T, st *Store) {
	t.Helper()
	_, err := st.migrateBackupPrincipalGrants()
	require.NoError(t, err)
}

func backupsGrant(t *testing.T, b *models.PermissionBackups) authorization.Policy {
	t.Helper()
	ps, err := conv.PermissionToPolicies(&models.Permission{Action: authorization.String(authorization.ManageBackups), Backups: b})
	require.NoError(t, err)
	return *ps[0]
}

func readDataGrant(t *testing.T) authorization.Policy {
	t.Helper()
	ps, err := conv.PermissionToPolicies(&models.Permission{Action: authorization.String(authorization.ReadData), Data: authorization.AllData})
	require.NoError(t, err)
	return *ps[0]
}

// anchorSubject is the placeholder subject every role is assigned to.
var anchorSubject = conv.UserNameWithTypeFromId(conv.InternalPlaceHolder, authentication.AuthTypeDb)

func csvPolicy(role string, p authorization.Policy) string {
	return fmt.Sprintf("p, %s, %s, %s, %s", conv.PrefixRoleName(role), p.Resource, p.Verb, p.Domain)
}

func csvAnchor(role string) string {
	return fmt.Sprintf("g, %s, %s", anchorSubject, conv.PrefixRoleName(role))
}

func rolePolicies(t *testing.T, authZ *rbac.Manager, role string) []authorization.Policy {
	t.Helper()
	roles, err := authZ.GetRoles(role)
	require.NoError(t, err)
	return roles[role]
}

func backupPrincipalsMigrated(t *testing.T, authZ *rbac.Manager) bool {
	t.Helper()
	ok, err := authZ.BackupPrincipalsMigrated()
	require.NoError(t, err)
	return ok
}

// A node is upgraded and the migration runs. An admin removes the roles
// wildcard. The node is rolled back to a release that saves its rows in
// policy.csv, then upgraded again. The removal must hold on that second boot.
func TestRBACRollbackRoundTrip(t *testing.T) {
	movies := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Movies")})
	usersGrant := backupsGrant(t, authorization.AllBackupUsers)
	rolesGrant := backupsGrant(t, authorization.AllBackupRoles)

	for _, tt := range []struct {
		name     string
		snapshot bool
	}{
		{name: "post-upgrade policy.csv rows, no snapshot"},
		{name: "post-upgrade policy.csv rows under a snapshot without an RBAC section", snapshot: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
			srv := openMockStore(t, base)
			executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, &cmd.CreateRolesRequest{
				Roles: map[string][]authorization.Policy{"operator": {movies}}, Version: cmd.RBACLatestCommandPolicyVersion, RoleCreation: true,
			})
			executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS,
				leaderMigration(map[string][]authorization.Policy{"operator": {usersGrant, rolesGrant}}))
			executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_REMOVE_PERMISSIONS, &cmd.RemovePermissionsRequest{
				Role: "operator", Permissions: []*authorization.Policy{&rolesGrant}, Version: cmd.RBACLatestCommandPolicyVersion,
			})
			if tt.snapshot {
				require.NoError(t, srv.store.raft.Load().Snapshot().Error())
				snaps, err := base.store.snapshotStore.List()
				require.NoError(t, err)
				require.Len(t, snaps, 1)
				_, hasRBAC := snapshotRBACPolicies(t, base.store, snaps[0].ID)
				require.False(t, hasRBAC)
			}
			require.NoError(t, srv.Close(ctx))
			require.Equal(t, tt.snapshot, lastSnapshotIndex(base.store.snapshotStore) != 0)

			rbacDir := t.TempDir()
			writeLegacyPolicy(t, rbacDir, "1.39.6",
				csvPolicy("operator", movies), csvPolicy("operator", usersGrant), csvAnchor("operator"),
				csvAnchor(conv.BackupPrincipalsMigratedRole))
			m, _ := reopenWithRBAC(t, base, rbacDir)
			runMigration(t, m.store)

			assert.ElementsMatch(t, []authorization.Policy{movies, usersGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
			assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
		})
	}
}
