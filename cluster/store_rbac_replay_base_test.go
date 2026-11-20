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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/fsm"
	cmd "github.com/weaviate/weaviate/cluster/proto/api"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authentication"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// snapshotRBACPolicies returns the policy ("p") rows in the RBAC section of
// raft snapshot id, and whether the section exists.
func snapshotRBACPolicies(t *testing.T, st *Store, id string) ([][]string, bool) {
	t.Helper()
	_, rc, err := st.snapshotStore.Open(id)
	require.NoError(t, err)
	defer rc.Close()
	var snap fsm.Snapshot
	require.NoError(t, json.NewDecoder(rc).Decode(&snap))
	if snap.RBAC == nil {
		return nil, false
	}
	var section struct {
		Policy [][]string `json:"roles_policies"`
	}
	require.NoError(t, json.Unmarshal(snap.RBAC, &section))
	return section.Policy, true
}

// A node with no raft snapshot and a log starting at index 1 replays its whole
// log over the roles policy.csv seeded.
func TestCSVOnlyRoleSurvivesUpgradeBoot(t *testing.T) {
	ctx := context.Background()
	readData := readDataGrant(t)
	rbacDir := t.TempDir()
	base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
	m := rbacMockStore(t, base, rbacDir)
	srv := openMockStore(t, m)
	executeRoleWrite(t, srv, cmd.ApplyRequest_TYPE_UPSERT_ROLES_PERMISSIONS, &cmd.CreateRolesRequest{
		Roles: map[string][]authorization.Policy{"logged": {readData}}, Version: cmd.RBACLatestCommandPolicyVersion, RoleCreation: true,
	})
	require.NoError(t, srv.Close(ctx))
	require.Zero(t, lastSnapshotIndex(m.store.snapshotStore), "the node closes without a raft snapshot")

	// "logged" is left out, so it can come only from the log replay.
	writeLegacyPolicy(t, rbacDir, "1.39.6",
		csvPolicy("csv-only", readData), csvAnchor("csv-only"), "g, db:alice, role:csv-only")
	m, srv = reopenWithRBAC(t, base, rbacDir)
	first, err := m.store.logStore.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), first)
	require.Zero(t, lastSnapshotIndex(m.store.snapshotStore))
	require.NoError(t, srv.store.raft.Load().Barrier(5*time.Second).Error())

	roles, err := m.cfg.RBAC.GetRoles()
	require.NoError(t, err)
	assert.Equal(t, []authorization.Policy{readData}, roles["logged"])
	assert.Equal(t, []authorization.Policy{readData}, roles["csv-only"])
	users, err := m.cfg.RBAC.GetUsersOrGroupForRole("csv-only", authentication.AuthTypeDb, false)
	require.NoError(t, err)
	assert.Equal(t, []string{"alice"}, users)
}

func TestRBACReplayBase(t *testing.T) {
	books := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Books")})

	// The snapshot without an RBAC section stands in for one taken before
	// v1.28.13: its log is truncated, so policy.csv alone holds the roles.
	t.Run("a snapshot without an RBAC section keeps policy.csv roles, and the migration adds the wildcards", func(t *testing.T) {
		ctx := context.Background()
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		srv := openMockStore(t, base)
		require.NoError(t, srv.store.raft.Load().Snapshot().Error())
		snaps, err := base.store.snapshotStore.List()
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		_, hasRBAC := snapshotRBACPolicies(t, base.store, snaps[0].ID)
		require.False(t, hasRBAC, "the RBAC-disabled boot writes a snapshot without an RBAC section")
		require.NoError(t, srv.Close(ctx))

		// Roles written before that snapshot exist only in the legacy
		// policy.csv; rbacMockStore's manager seeds them at construction.
		rbacDir := t.TempDir()
		writeLegacyPolicy(t, rbacDir, "1.39.6", csvPolicy("operator", books), csvAnchor("operator"))
		m, _ := reopenWithRBAC(t, base, rbacDir)
		runMigration(t, m.store)

		usersGrant := backupsGrant(t, authorization.AllBackupUsers)
		rolesGrant := backupsGrant(t, authorization.AllBackupRoles)
		assert.ElementsMatch(t, []authorization.Policy{books, usersGrant, rolesGrant}, rolePolicies(t, m.cfg.RBAC, "operator"))
		assert.True(t, backupPrincipalsMigrated(t, m.cfg.RBAC))
	})

	t.Run("single-node recovery keeps a role no log entry wrote", func(t *testing.T) {
		readData := readDataGrant(t)
		rbacDir := t.TempDir()
		writeLegacyPolicy(t, rbacDir, "1.39.6", csvPolicy("csv-only", readData), csvAnchor("csv-only"))
		m, srv := reopenWithRBAC(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()), rbacDir)
		require.NoError(t, srv.UpdateRolesPermissions(map[string][]authorization.Policy{"operator": {readData}}))
		require.Zero(t, lastSnapshotIndex(m.store.snapshotStore), "recovery starts without a raft snapshot")

		require.NoError(t, m.store.recoverSingleNode(true))
		require.True(t, tryNTimesWithWait(20, 200*time.Millisecond, m.store.IsLeader))
		require.NoError(t, m.store.raft.Load().Barrier(5*time.Second).Error())

		roles, err := m.cfg.RBAC.GetRoles()
		require.NoError(t, err)
		assert.Equal(t, []authorization.Policy{readData}, roles["operator"])
		assert.Equal(t, []authorization.Policy{readData}, roles["csv-only"])
	})
}
