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
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	raftbolt "github.com/hashicorp/raft-boltdb/v2"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/cluster/fsm"
	"github.com/weaviate/weaviate/cluster/utils"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
	"github.com/weaviate/weaviate/usecases/auth/authorization/conv"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac"
	"github.com/weaviate/weaviate/usecases/auth/authorization/rbac/rbacconf"
	"github.com/weaviate/weaviate/usecases/config"
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

func TestRBACReplayBase(t *testing.T) {
	books := backupsGrant(t, &models.PermissionBackups{Collection: authorization.String("Books")})

	// The snapshot without an RBAC section stands in for one taken before
	// v1.28.13: its log is truncated, so policy.csv alone holds the roles.
	t.Run("a snapshot without an RBAC section keeps policy.csv roles and is sealed", func(t *testing.T) {
		ctx := context.Background()
		base := NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort())
		m := base
		srv := openMockStore(t, m)
		require.NoError(t, srv.store.raft.Load().Snapshot().Error())
		snaps, err := m.store.snapshotStore.List()
		require.NoError(t, err)
		require.Len(t, snaps, 1)
		oldID := snaps[0].ID
		_, hasRBAC := snapshotRBACPolicies(t, m.store, oldID)
		require.False(t, hasRBAC, "the RBAC-disabled boot writes a snapshot without an RBAC section")
		require.NoError(t, srv.Close(ctx))

		// Roles written before that snapshot exist only in policy.csv. Tests
		// leave build.Version empty, so the version file gets a real one.
		rbacDir := t.TempDir()
		authZ, err := rbac.New(rbacDir, rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, m.logger)
		require.NoError(t, err)
		require.NoError(t, authZ.CreateRolesPermissions(map[string][]authorization.Policy{"operator": {books}}))
		require.NoError(t, os.WriteFile(filepath.Join(rbacDir, "rbac", "version"), []byte("1.39.6"), 0o600))

		// The restarted node stays quiet: nothing is written through raft, so
		// raft's applied index stays at the restored snapshot's.
		base.indexer = m.indexer
		m = rbacMockStore(t, base, rbacDir)
		srv = openMockStore(t, m)
		t.Cleanup(func() { require.NoError(t, srv.Close(ctx)) })

		roles, err := m.cfg.RBAC.GetRoles("operator")
		require.NoError(t, err)
		assert.Equal(t, []authorization.Policy{books}, roles["operator"], "policy.csv roles survive the boot")

		var newID string
		require.Eventually(t, func() bool {
			snaps, err := m.store.snapshotStore.List()
			if err != nil || len(snaps) == 0 || snaps[0].ID == oldID {
				return false
			}
			newID = snaps[0].ID
			return true
		}, 10*time.Second, 10*time.Millisecond, "the forced snapshot was never taken")
		policies, hasRBAC := snapshotRBACPolicies(t, m.store, newID)
		require.True(t, hasRBAC, "the new snapshot has an RBAC section")
		assert.Contains(t, policies, []string{conv.PrefixRoleName("operator"), books.Resource, books.Verb, books.Domain},
			"the new snapshot carries the policy.csv roles")
	})

	t.Run("single-node recovery replays onto a cleared store", func(t *testing.T) {
		ctx := context.Background()
		m := rbacMockStore(t, NewMockStore(t, "Node-1", utils.MustGetFreeTCPPort()), t.TempDir())
		srv := openMockStore(t, m)
		t.Cleanup(func() { require.NoError(t, srv.Close(ctx)) })
		require.NoError(t, srv.UpdateRolesPermissions(map[string][]authorization.Policy{"operator": {books}}))
		require.Zero(t, lastSnapshotIndex(m.store.snapshotStore), "recovery starts without a raft snapshot")

		// No log entry wrote the ghost role. RecoverCluster replays the whole
		// log, and a replay onto this store would carry it into the snapshot.
		require.NoError(t, m.cfg.RBAC.CreateRolesPermissions(map[string][]authorization.Policy{"ghost": {books}}))

		require.NoError(t, m.store.recoverSingleNode(true))
		require.NotZero(t, lastSnapshotIndex(m.store.snapshotStore), "RecoverCluster snapshots the replayed state")
		require.True(t, tryNTimesWithWait(20, 200*time.Millisecond, m.store.IsLeader))
		require.NoError(t, m.store.raft.Load().Barrier(5*time.Second).Error())

		roles, err := m.cfg.RBAC.GetRoles()
		require.NoError(t, err)
		assert.Equal(t, []authorization.Policy{books}, roles["operator"])
		_, hasGhost := roles["ghost"]
		assert.False(t, hasGhost, "the role no log entry wrote is gone")
	})

	// The reset trusts "no snapshot" to mean "the log is complete". Snapshot
	// files deleted by hand after raft truncated the log break that premise:
	// policy.csv is the only complete state left, so the reset must not run.
	t.Run("the reset guard on the first log index", func(t *testing.T) {
		for _, tt := range []struct {
			name       string
			firstIndex uint64
			wantKept   bool
		}{
			{name: "log starts past index 1: policy.csv roles kept", firstIndex: 3, wantKept: true},
			{name: "complete log: roles cleared", firstIndex: 1, wantKept: false},
		} {
			t.Run(tt.name, func(t *testing.T) {
				logger := logrus.New()
				logger.SetOutput(io.Discard)
				authZ, err := rbac.New(t.TempDir(), rbacconf.Config{Enabled: true}, config.Authentication{}, false, nil, logger)
				require.NoError(t, err)
				require.NoError(t, authZ.CreateRolesPermissions(map[string][]authorization.Policy{"operator": {books}}))

				workDir := t.TempDir()
				snaps, err := raft.NewFileSnapshotStore(workDir, 1, io.Discard)
				require.NoError(t, err)
				logs, err := raftbolt.NewBoltStore(filepath.Join(workDir, "raft.db"))
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, logs.Close()) })
				require.NoError(t, logs.StoreLog(&raft.Log{Index: tt.firstIndex, Term: 1, Type: raft.LogCommand}))

				st := &Store{cfg: Config{RBAC: authZ}, log: logger, snapshotStore: snaps, logStore: logs}
				require.NoError(t, st.resetRBACIfNoSnapshot())

				roles, err := authZ.GetRoles("operator")
				require.NoError(t, err)
				_, kept := roles["operator"]
				assert.Equal(t, tt.wantKept, kept)
			})
		}
	})
}
