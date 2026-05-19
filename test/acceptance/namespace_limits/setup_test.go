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
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/client/users"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

const (
	adminUser, adminKey = "admin-user", "admin-key"
	objectCap           = 10
)

var sharedCompose *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
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

// createNamespacedUser creates a namespaced DB user and waits for the
// apikey to be recognized by the follower the test client talks to.
func createNamespacedUser(t *testing.T, userID, ns string) string {
	t.Helper()

	var apikey string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp, err := helper.Client(t).Users.CreateUser(
			users.NewCreateUserParams().WithUserID(userID).WithBody(users.CreateUserBody{Namespace: ns}),
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

	return apikey
}
