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

// Package namespace_limits is the acceptance suite for per-namespace
// MAXIMUM_ALLOWED_OBJECTS_COUNT enforcement. The shared compose sets the
// cap to 10 with aggressive flush so the async object-count path catches
// up within seconds.
package namespace_limits

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
	"github.com/weaviate/weaviate/usecases/auth/authorization"
)

const (
	adminUser, adminKey = "admin-user", "admin-key"
	objectCap           = 10
)

// nsCounter backs uniqueNS. Tests must not hardcode namespace names: a shared
// compose runs every test against one cluster, so reused names collide.
var nsCounter atomic.Int64

// uniqueNS returns a process-unique, validator-legal namespace name.
func uniqueNS() string {
	return fmt.Sprintf("ns%d", nsCounter.Add(1))
}

var sharedCompose *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithApiKey().
		WithRBAC().
		WithUserApiKey(adminUser, adminKey).
		WithRbacRoots(adminUser).
		WithDbUsers().
		WithNamespaces().
		WithWeaviateEnv("MAXIMUM_ALLOWED_OBJECTS_COUNT", strconv.Itoa(objectCap)).
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_IDLE_AFTER_SECONDS", "1").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_MIN_ACTIVE_DURATION_SECONDS", "1").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_MAX_ACTIVE_DURATION_SECONDS", "2").
		WithWeaviateClusterWithGRPC().
		Start(ctx)
	if err != nil {
		panic(errors.Wrap(err, "failed to start shared compose"))
	}
	sharedCompose = compose

	helper.SetupClient(compose.GetWeaviate().URI())

	code := m.Run()

	if err := sharedCompose.Terminate(ctx); err != nil {
		panic(errors.Wrap(err, "failed to terminate shared compose"))
	}
	os.Exit(code)
}

// createNamespacedUser creates a namespaced DB user, waits for the apikey to be
// recognized by the follower the test client talks to, and grants the built-in
// admin role so the user can act within its namespace under mandatory RBAC.
func createNamespacedUser(t *testing.T, userID, ns string) string {
	t.Helper()

	var apikey string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(ns+":"+userID).WithBody(users.CreateUserBody{}),
			helper.CreateAuth(adminKey),
		)
		if !assert.NoError(c, err) {
			return
		}
		if !assert.NotNil(c, resp.Payload.Apikey) {
			return
		}
		apikey = *resp.Payload.Apikey
	}, 10*time.Second, 50*time.Millisecond, "user %q could not be created", userID)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		_, err := helper.Client(t).Users.GetOwnInfo(
			users.NewGetOwnInfoParams(), helper.CreateAuth(apikey))
		assert.NoError(c, err)
	}, 10*time.Second, 50*time.Millisecond, "user %q apikey not recognized after create", userID)

	// Mandatory RBAC: grant the built-in admin so the namespaced user can create
	// classes and write objects. The quota chokepoint is orthogonal to RBAC and
	// still fires. DeleteUser revokes all bindings on cleanup.
	helper.AssignRoleToUser(t, adminKey, authorization.Admin, ns+":"+userID)

	// AssignRoleToUser returns once the leader applies the binding; the follower
	// the test client talks to may still be replicating it. Tests that create a
	// class immediately after this helper would otherwise race that replication
	// and get a spurious create_collections 403, so wait until the role is
	// locally visible before returning.
	helper.WaitForOwnRole(t, apikey, authorization.Admin)

	return apikey
}
