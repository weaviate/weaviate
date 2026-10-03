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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/cluster/mocks"
	"github.com/weaviate/weaviate/usecases/config"
)

// The RBAC backup-principals migration arms the forced-snapshot worker: role
// data written before backups/users and backups/roles existed migrates on
// replay, and the stateful removal means replaying that history onto later
// state can re-grant what a later write removed. These tests prove that Close stops the worker and
// that the snapshot keeps a restart from re-granting.

// rbacMockStore is NewMockStore with a real RBAC store under rbacDir, built
// as production builds it: NewFSM, then its address.
func rbacMockStore(t *testing.T, m MockStore, rbacDir string) MockStore {
	t.Helper()
	authZ, err := rbac.New(rbacDir, rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, m.logger)
	require.NoError(t, err)
	m.cfg.RBAC = authZ
	s := NewFSM(m.cfg, nil, prometheus.NewPedanticRegistry())
	s.schemaManager.SetReplicationFSM(m.replicationFSM)
	m.store = &s
	return m
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

// executeRoleWrite commits a role write through raft, as a node on either
// side of the upgrade would.
func executeRoleWrite(t *testing.T, srv *Raft, typ cmd.ApplyRequest_Type, req any) {
	t.Helper()
	sub, err := json.Marshal(req)
	require.NoError(t, err)
	_, err = srv.Execute(context.Background(), &cmd.ApplyRequest{Type: typ, SubCommand: sub})
	require.NoError(t, err)
}

func backupsGrant(t *testing.T, b *models.PermissionBackups) authorization.Policy {
	t.Helper()
	ps, err := conv.PermissionToPolicies(&models.Permission{Action: authorization.String(authorization.ManageBackups), Backups: b})
	require.NoError(t, err)
	return *ps[0]
}

func TestForcedSnapshotOnRBACMigration(t *testing.T) {
	movies := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Movies")})

	t.Run("Close stops the worker", func(t *testing.T) {
		m := rbacMockStore(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()), t.TempDir())
		srv := openMockStore(t, m)

		executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, &cmd.CreateRolesRequest{
			Roles: map[string][]authorization.Policy{"operator": {movies}}, Version: cmd.RBACLatestCommandPolicyVersion,
		})

		done := m.store.forcedSnapshotsDone
		require.NoError(t, srv.Close(context.Background()))
		select {
		case <-done:
		default:
			t.Fatal("the worker outlived Close")
		}
	})

	// History from before the upgrade, on a node with no raft snapshot: a
	// grant and a removal of Movies, both without the marker, then a grant of
	// the Books collection carrying it. The role must hold Books alone. The control deletes the snapshot
	// files before the restart: the whole log then replays onto a policy.csv
	// that already holds Books, and the removal keeps the users and roles
	// grants because Books is there.
	t.Run("a restart", func(t *testing.T) {
		books := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Books")})
		usersGrant := backupsGrant(t, authorization.AllBackupUsers)
		rolesGrant := backupsGrant(t, authorization.AllBackupRoles)

		rolePolicies := func(t *testing.T, authZ *rbac.Manager) []authorization.Policy {
			t.Helper()
			roles, err := authZ.GetRoles("operator")
			require.NoError(t, err)
			return roles["operator"]
		}

		for _, tt := range []struct {
			name            string
			deleteSnapshots bool
			want            []authorization.Policy
		}{
			{name: "the forced snapshot keeps the restart from re-granting", want: []authorization.Policy{books}},
			{name: "control: with the snapshot files deleted the restart re-grants", deleteSnapshots: true, want: []authorization.Policy{books, usersGrant, rolesGrant}},
		} {
			t.Run(tt.name, func(t *testing.T) {
				ctx := context.Background()
				rbacDir := t.TempDir()
				base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
				m := rbacMockStore(t, base, rbacDir)
				srv := openMockStore(t, m)
				require.Zero(t, lastSnapshotIndex(m.store.snapshotStore), "the node starts without a raft snapshot")

				// These role writes carry no marker, as writes from before the upgrade do.
				executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, &cmd.CreateRolesRequest{
					Roles: map[string][]authorization.Policy{"operator": {movies}}, Version: cmd.RBACLatestCommandPolicyVersion,
				})
				executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_REMOVE_PERMISSIONS, &cmd.RemovePermissionsRequest{
					Role: "operator", Permissions: []*authorization.Policy{&movies}, Version: cmd.RBACLatestCommandPolicyVersion,
				})
				require.Eventually(t, func() bool {
					return lastSnapshotIndex(m.store.snapshotStore) > 0
				}, 10*time.Second, 10*time.Millisecond, "the forced snapshot was never taken")

				// A write from after the upgrade goes through the production writer.
				require.NoError(t, srv.UpdateRolesPermissions(map[string][]authorization.Policy{"operator": {books}}))
				require.Equal(t, []authorization.Policy{books}, rolePolicies(t, m.cfg.RBAC))
				require.NoError(t, srv.Close(ctx))
				if tt.deleteSnapshots {
					// Raft's trailing-logs default keeps the whole log at this
					// scale, so the node restarts as one that never snapshotted.
					require.NoError(t, os.RemoveAll(filepath.Join(m.cfg.WorkDir, "snapshots")))
				}

				// Restart: a fresh RBAC store loads policy.csv, then raft restores and
				// replays. Tests leave build.Version empty, so the version file the
				// first boot wrote is replaced with a real one.
				require.NoError(t, os.WriteFile(filepath.Join(rbacDir, "rbac", "version"), []byte("1.39.6"), 0o600))
				base.indexer = m.indexer
				m = rbacMockStore(t, base, rbacDir)
				srv = openMockStore(t, m)
				t.Cleanup(func() { require.NoError(t, srv.Close(ctx)) })
				require.NoError(t, srv.store.raft.Load().Barrier(5*time.Second).Error())

				assert.ElementsMatch(t, tt.want, rolePolicies(t, m.cfg.RBAC))
			})
		}
	})
}
