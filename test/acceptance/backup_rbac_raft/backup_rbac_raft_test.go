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

// Package backup_rbac_raft covers backup restore of RBAC roles and dynamic
// users reaching every node and surviving a restart. The restore issues one
// RAFT entry that every node's state machine applies, so a node that was not
// a backup participant gets the state too, and a restart replays it.
package backup_rbac_raft

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/weaviate/weaviate/client/authz"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	adminUser = "admin-user"
	adminKey  = "admin-key"

	// restoredUser and restoredRole are deleted before the restore and must
	// come back on every node.
	restoredUser = "restored-user"
	restoredRole = "restoredRole"

	backend  = "s3"
	s3Bucket = "bucket"
	s3Region = "eu-west-1"

	className = "BackupRolesAndUsers"

	nodeCount = 3
)

func strPtr(s string) *string { return &s }

// newCluster starts a three-node cluster with RBAC, dynamic users and an S3
// backend. A filesystem backend is rejected for multi-node clusters.
// Namespaces stay disabled, which is the default.
func newCluster(t *testing.T, extraEnv map[string]string) (*docker.DockerCompose, func()) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	builder := docker.New().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateCluster(nodeCount).
		WithApiKey().
		WithRBAC().
		WithDbUsers().
		WithBackendS3(s3Bucket, s3Region).
		WithUserApiKey(adminUser, adminKey).
		WithRbacRoots(adminUser)
	for k, v := range extraEnv {
		builder = builder.WithWeaviateEnv(k, v)
	}

	compose, err := builder.Start(ctx)
	require.NoError(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())

	return compose, func() {
		termCtx, termCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer termCancel()
		if err := compose.Terminate(termCtx); err != nil {
			t.Errorf("failed to terminate test containers: %v", err)
		}
	}
}

// singleShardClass keeps the backed-up class off at least one node, so that
// node is never a restore participant. groupByShard records the leader plus
// the shard's owner only.
func singleShardClass() *models.Class {
	return &models.Class{
		Class:      className,
		Vectorizer: "none",
		Properties: []*models.Property{
			{Name: "title", DataType: []string{"text"}},
		},
		ShardingConfig:    map[string]interface{}{"desiredCount": 1},
		ReplicationConfig: &models.ReplicationConfig{Factor: 1},
	}
}

func createRestoredRole(t *testing.T) {
	t.Helper()
	helper.CreateRole(t, adminKey, &models.Role{
		Name: strPtr(restoredRole),
		Permissions: []*models.Permission{
			{
				Action: strPtr(authorization.ReadRoles),
				Roles: &models.PermissionRoles{
					Role:  strPtr("*"),
					Scope: strPtr(models.PermissionRolesScopeAll),
				},
			},
		},
	})
}

// requireRolesAndUsersOnEveryNode authenticates against each node with the restored
// user's key and exercises the restored role's permission. Both checks are
// node-local; a leader-routed query would report the leader's state from any
// node.
func requireRolesAndUsersOnEveryNode(t *testing.T, compose *docker.DockerCompose, userKey string) {
	t.Helper()
	for n := 1; n <= nodeCount; n++ {
		uri := compose.GetWeaviateNode(n).URI()
		// Followers apply the entry asynchronously.
		require.EventuallyWithTf(t, func(c *assert.CollectT) {
			helper.SetupClient(uri)
			// The listing's contents come from the leader, so they prove nothing
			// about this node. The assertion is only that the call is authorized.
			_, err := helper.Client(t).Authz.GetRoles(authz.NewGetRolesParams(), helper.CreateAuth(userKey))
			assert.NoError(c, err)
		}, 30*time.Second, 500*time.Millisecond,
			"restored user could not exercise its restored role on node %d (%s)", n, uri)
	}
}

// backupAndDeleteRolesAndUsers creates the user, role and assignment, backs the class
// up, then deletes all three so the restore has something to bring back. It
// returns the restored user's API key.
func backupAndDeleteRolesAndUsers(t *testing.T, backupID string) string {
	t.Helper()
	userKey := helper.CreateUser(t, restoredUser, adminKey)
	createRestoredRole(t)
	helper.AssignRoleToUser(t, adminKey, restoredRole, restoredUser)

	resp, err := helper.CreateBackupWithAuthz(t, helper.DefaultBackupConfig(), className, backend, backupID, helper.CreateAuth(adminKey))
	require.NoError(t, err)
	require.Equal(t, "", resp.Payload.Error)
	helper.ExpectBackupEventuallyCreated(t, backupID, backend, helper.CreateAuth(adminKey))

	helper.DeleteRole(t, adminKey, restoredRole)
	helper.DeleteUser(t, restoredUser, adminKey)
	helper.DeleteClassWithAuthz(t, className, helper.CreateAuth(adminKey))

	return userKey
}

// restoreRolesAndUsers asks for the roles and users to be restored; both default to
// noRestore, so they must be requested explicitly.
func restoreRolesAndUsers(t *testing.T, backupID string) {
	t.Helper()
	cfg := helper.DefaultRestoreConfig()
	cfg.RolesOptions = strPtr(models.RestoreConfigRolesOptionsAll)
	cfg.UsersOptions = strPtr(models.RestoreConfigUsersOptionsAll)
	resp, err := helper.RestoreBackupWithAuthz(t, cfg, className, backend, backupID, map[string]string{}, helper.CreateAuth(adminKey))
	require.NoError(t, err)
	require.Equal(t, "", resp.Payload.Error)
}

// restartCluster stops every node and brings them all back. The nodes are
// started concurrently because StartNode waits for readiness, a node is not
// ready until RAFT has quorum, and starting them one at a time therefore
// deadlocks on the first.
func restartCluster(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for n := 0; n < nodeCount; n++ {
		require.NoError(t, compose.StopNode(ctx, n, nil))
	}
	g, gctx := errgroup.WithContext(ctx)
	for n := 0; n < nodeCount; n++ {
		g.Go(func() error { return compose.StartNode(gctx, n) })
	}
	require.NoError(t, g.Wait())
}

// Default RAFT settings: the restore entry is replayed from the log on
// restart. Subtests run in order and build on each other's state.
func TestBackupRestoreRolesAndUsersPropagationAndRestart(t *testing.T) {
	// helper.SetupClient mutates package globals; nothing here may run in parallel.
	compose, down := newCluster(t, nil)
	defer down()
	defer helper.ResetClient()

	backupID := "roles-users-propagation"

	helper.CreateClassAuth(t, singleShardClass(), adminKey)
	userKey := backupAndDeleteRolesAndUsers(t, backupID)
	restoreRolesAndUsers(t, backupID)
	helper.ExpectBackupEventuallyRestored(t, backupID, backend, helper.CreateAuth(adminKey))

	t.Run("reaches every node", func(t *testing.T) {
		requireRolesAndUsersOnEveryNode(t, compose, userKey)
	})

	t.Run("survives a full-cluster restart", func(t *testing.T) {
		restartCluster(t, compose)
		requireRolesAndUsersOnEveryNode(t, compose, userKey)
	})

	t.Run("applies auth when the class restore fails", func(t *testing.T) {
		// The class is still present from the earlier subtests, so every
		// TYPE_RESTORE_CLASS is rejected by PreApplyFilter and the restore ends
		// FAILED. The roles and users must still be applied cluster-wide: a class
		// conflict is a failure of class restore, not of auth restore.
		helper.SetupClient(compose.GetWeaviate().URI())
		helper.DeleteRole(t, adminKey, restoredRole)
		helper.DeleteUser(t, restoredUser, adminKey)

		restoreRolesAndUsers(t, backupID)

		// Assert FAILED, not just any terminal status. The class conflict is what
		// separates this subtest from "reaches every node". If PreApplyFilter ever
		// starts accepting an existing class, this test must fail rather than
		// quietly turn into a copy of that one.
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			resp, err := helper.RestoreBackupStatusWithAuthz(t, backend, backupID, "", "", helper.CreateAuth(adminKey))
			if !assert.NoError(c, err) {
				return
			}
			assert.Equal(c, "FAILED", *resp.Payload.Status)
		}, 60*time.Second, 500*time.Millisecond, "restore did not report FAILED on the class conflict")

		requireRolesAndUsersOnEveryNode(t, compose, userKey)
	})
}

// A snapshot per change makes a restarting node install the snapshot instead
// of replaying the log. Only this test proves restored roles survive that
// path: without the RAFT entry, a node booting from a snapshot rebuilds the
// old role state and SavePolicy writes it back to disk.
func TestBackupRestoreRolesAndUsersSurvivesSnapshotRestart(t *testing.T) {
	compose, down := newCluster(t, map[string]string{
		"RAFT_SNAPSHOT_THRESHOLD": "1",
		"RAFT_SNAPSHOT_INTERVAL":  "1",
		"RAFT_TRAILING_LOGS":      "1",
	})
	defer down()
	defer helper.ResetClient()

	backupID := "roles-users-snapshot-restart"

	helper.CreateClassAuth(t, singleShardClass(), adminKey)
	userKey := backupAndDeleteRolesAndUsers(t, backupID)
	restoreRolesAndUsers(t, backupID)
	helper.ExpectBackupEventuallyRestored(t, backupID, backend, helper.CreateAuth(adminKey))

	requireRolesAndUsersOnEveryNode(t, compose, userKey)

	restartCluster(t, compose)
	requireRolesAndUsersOnEveryNode(t, compose, userKey)
}
