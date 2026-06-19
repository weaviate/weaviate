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

package authz

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/schema"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

// Static principals baked into the shared container. Tests in the
// default-config bucket authenticate with these; they are never created or
// deleted at runtime, so resetAuthzState can return the cluster to boot state
// without re-provisioning them.
const (
	sharedRootUser   = "admin-user"
	sharedRootKey    = "admin-key"
	sharedRoot2User  = "existing-user"
	sharedRoot2Key   = "existing-key"
	sharedViewerUser = "viewer-user"
	sharedViewerKey  = "viewer-key"
	// sharedImportUser is a static env user reserved for the destructive
	// import-static-user test, which consumes (imports then deletes) it.
	sharedImportUser = "import-user"
	sharedImportKey  = "import-key"
)

// sharedPlainPrincipals start with no role; resetAuthzState strips any
// assignment they accrue during a test.
var sharedPlainPrincipals = map[string]string{
	"custom-user":  "custom-key",
	"custom-user2": "custom-key2",
	"test-user":    "test-key",
	"limited-user": "limited-key",
}

var (
	sharedMu    sync.Mutex
	sharedAuthz *docker.DockerCompose

	sharedClusterMu sync.Mutex
	sharedCluster   *docker.DockerCompose
)

func TestMain(m *testing.M) {
	code := m.Run()
	terminateShared(&sharedMu, &sharedAuthz)
	terminateShared(&sharedClusterMu, &sharedCluster)
	os.Exit(code)
}

func terminateShared(mu *sync.Mutex, compose **docker.DockerCompose) {
	mu.Lock()
	c := *compose
	mu.Unlock()
	if c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		_ = c.Terminate(ctx)
		cancel()
	}
}

// getSharedCompose lazily starts the single-node RBAC container with the
// superset of static principals and reuses it for every default-config test.
func getSharedCompose(t *testing.T) *docker.DockerCompose {
	t.Helper()
	sharedMu.Lock()
	defer sharedMu.Unlock()
	if sharedAuthz != nil {
		return sharedAuthz
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	builder := docker.New().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateWithGRPC().WithRBAC().WithApiKey().WithDbUsers().
		WithBackendFilesystem().
		WithUserApiKey(sharedRootUser, sharedRootKey).
		WithUserApiKey(sharedRoot2User, sharedRoot2Key).
		WithUserApiKey(sharedViewerUser, sharedViewerKey).
		WithUserApiKey(sharedImportUser, sharedImportKey).
		WithRbacRoots(sharedRootUser, sharedRoot2User).
		WithRbacViewers(sharedViewerUser)
	for u, k := range sharedPlainPrincipals {
		builder = builder.WithUserApiKey(u, k)
	}

	compose, err := builder.Start(ctx)
	require.NoError(t, err)
	sharedAuthz = compose
	return sharedAuthz
}

// composeUpShared points the test client at the shared default-config
// container. Its principals are baked in at boot, so callers don't provision
// any. The teardown resets state instead of terminating, so the container
// survives for the next test.
func composeUpShared(t *testing.T) (*docker.DockerCompose, func()) {
	compose := getSharedCompose(t)
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())
	resetAuthzState(t) // clean any residue a prior test's teardown missed
	return compose, func() { resetAuthzState(t) }
}

// getSharedCluster lazily starts the single 3-node RBAC cluster reused by every
// multi-node test. It bakes the same principals as the single-node shared
// container plus the infra those tests need: an S3/MinIO backend for backup and
// REPLICA_MOVEMENT_ENABLED for replication. State is reset between tests.
func getSharedCluster(t *testing.T) *docker.DockerCompose {
	t.Helper()
	sharedClusterMu.Lock()
	defer sharedClusterMu.Unlock()
	if sharedCluster != nil {
		return sharedCluster
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	builder := docker.New().
		WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateCluster(3).
		WithApiKey().WithRBAC().WithDbUsers().
		WithBackendS3("bucket", s3BackupJourneyRegion).
		WithUserApiKey(sharedRootUser, sharedRootKey).
		WithUserApiKey(sharedRoot2User, sharedRoot2Key).
		WithRbacRoots(sharedRootUser, sharedRoot2User)
	for u, k := range sharedPlainPrincipals {
		builder = builder.WithUserApiKey(u, k)
	}

	compose, err := builder.Start(ctx)
	require.NoError(t, err)
	sharedCluster = compose
	return sharedCluster
}

// composeUpSharedCluster points the test client at node 1 of the shared cluster
// and returns a reset-based teardown so the cluster survives for the next test.
func composeUpSharedCluster(t *testing.T) (*docker.DockerCompose, func()) {
	compose := getSharedCluster(t)
	resetClusterState(t, compose)
	return compose, func() { resetClusterState(t, compose) }
}

// resetClusterState restarts any node a prior test left stopped, re-points the
// client at node 1, and clears RBAC/schema residue.
func resetClusterState(t *testing.T, compose *docker.DockerCompose) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	for n := 1; n <= 3; n++ {
		require.NoError(t, compose.EnsureRunning(ctx, n))
	}
	cancel()
	helper.SetupClient(compose.GetWeaviate().URI())
	resetAuthzState(t)
}

// countDynamicUsers returns how many of the listed users were created at
// runtime (db_user); static env users baked at boot are excluded.
func countDynamicUsers(users []*models.DBUserInfo) int {
	n := 0
	for _, u := range users {
		if u.DbUserType != nil && *u.DbUserType == models.DBUserInfoDbUserTypeDbUser {
			n++
		}
	}
	return n
}

// resetAuthzState returns the shared container to boot state: drops every
// user-created role and class and strips role assignments accrued by the
// permissionless principals. Built-in roles and the static principals' boot
// assignments are preserved.
func resetAuthzState(t *testing.T) {
	t.Helper()
	builtin := make(map[string]struct{}, len(authorization.BuiltInRoles))
	for _, r := range authorization.BuiltInRoles {
		builtin[r] = struct{}{}
	}

	// Dropping a custom role also removes its assignments.
	for _, role := range helper.GetRoles(t, sharedRootKey) {
		if role.Name == nil {
			continue
		}
		if _, ok := builtin[*role.Name]; ok {
			continue
		}
		helper.DeleteRole(t, sharedRootKey, *role.Name)
	}

	// Strip any built-in role a test pinned onto a permissionless principal.
	for user := range sharedPlainPrincipals {
		for _, role := range helper.GetRolesForUser(t, user, sharedRootKey, false) {
			if role.Name != nil {
				helper.RevokeRoleFromUser(t, sharedRootKey, *role.Name, user)
			}
		}
	}

	// Drop dynamically-created DB users; static env users survive to boot state.
	for _, u := range helper.ListAllUsers(t, sharedRootKey) {
		if u.UserID == nil || u.DbUserType == nil {
			continue
		}
		if *u.DbUserType == models.DBUserInfoDbUserTypeDbUser {
			helper.DeleteUser(t, *u.UserID, sharedRootKey)
		}
	}

	// Aliases reference classes, so drop them before the classes they point at.
	if aliases := helper.GetAliasesWithAuthz(t, nil, helper.CreateAuth(sharedRootKey)); aliases != nil {
		for _, a := range aliases.Aliases {
			helper.DeleteAliasWithAuthz(t, a.Alias, helper.CreateAuth(sharedRootKey))
		}
	}

	resp, err := helper.Client(t).Schema.SchemaDump(schema.NewSchemaDumpParams(), helper.CreateAuth(sharedRootKey))
	require.NoError(t, err)
	for _, c := range resp.Payload.Classes {
		helper.DeleteClassAuth(t, c.Class, sharedRootKey)
	}
}
