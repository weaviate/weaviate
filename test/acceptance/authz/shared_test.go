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
)

func TestMain(m *testing.M) {
	code := m.Run()
	sharedMu.Lock()
	c := sharedAuthz
	sharedMu.Unlock()
	if c != nil {
		_ = c.Terminate(context.Background())
	}
	os.Exit(code)
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
		WithWeaviateWithGRPC().WithRBAC().WithApiKey().
		WithBackendFilesystem().
		WithUserApiKey(sharedRootUser, sharedRootKey).
		WithUserApiKey(sharedRoot2User, sharedRoot2Key).
		WithUserApiKey(sharedViewerUser, sharedViewerKey).
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

// composeUpShared is a drop-in for composeUp for the default-config bucket.
// The admins/users/viewers maps are ignored: their principals are baked into
// the shared container at boot. The teardown resets state instead of
// terminating, so the container survives for the next test.
func composeUpShared(t *testing.T, _, _, _ map[string]string) (*docker.DockerCompose, func()) {
	compose := getSharedCompose(t)
	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())
	resetAuthzState(t) // clean any residue a prior test's teardown missed
	return compose, func() { resetAuthzState(t) }
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
