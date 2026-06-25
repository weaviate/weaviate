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

package namespace

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// nsCounter backs uniqueNS. Tests must not hardcode namespace names: a shared
// literal would collide once tests run in parallel against the shared cluster.
var nsCounter atomic.Int64

// uniqueNS returns a process-unique namespace name ("ns1", "ns2", ...) that
// satisfies the name contract (lowercase alphanumeric, 3-36 chars). Each test
// allocates its own so namespaced state (classes, users, objects) stays
// isolated across concurrent tests.
func uniqueNS() string {
	return fmt.Sprintf("ns%d", nsCounter.Add(1))
}

// roleCounter backs uniqueRole.
var roleCounter atomic.Int64

// uniqueRole returns a process-unique global role short name. Operator-created
// global roles share a cluster-wide short-name reservation, so a hardcoded name
// would collide once tests run in parallel against the shared cluster.
func uniqueRole() string {
	return fmt.Sprintf("role%d", roleCounter.Add(1))
}

// retryOnAliasLag retries op until it returns no error. Used after
// CreateAliasAuth on the multi-node cluster: the create returns when the
// leader has applied, but the follower the test client talks to may still
// be replicating the alias entry, so the immediate next operation (which
// resolves the alias via local schema state) can transiently fail with
// "class not found" / 404 / 422. Pattern mirrors helper.CreateNamespace's
// EventuallyWithT poll, just on the operation rather than the entity.
func retryOnAliasLag(t *testing.T, op func() error) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.NoError(c, op())
	}, 10*time.Second, 50*time.Millisecond, "operation kept failing while waiting for alias to be visible locally")
}

// adminUser is the env-var root. Namespaced DB users are created at runtime via
// createNamespacedUser and granted the built-in admin role by this root.
const adminUser, adminKey = "admin-user", "admin-key"

// Two extra static API-key users for TestGlobalCallerColonUserIDAuthz. Static
// keys are global operators (ns==""), the journey the matcher fix targets. The
// test grants gCaller a narrow role at runtime; gTarget just needs to exist.
const (
	gCaller, gCallerKey = "gcaller", "gcaller-key"
	gTarget, gTargetKey = "gtarget", "gtarget-key"
)

var sharedCompose *docker.DockerCompose

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	// offload-s3 needs AWS creds in the process env before Start so each
	// node can authenticate against the MinIO sidecar.
	os.Setenv("AWS_ACCESS_KEY_ID", "aws_access_key")
	os.Setenv("AWS_SECRET_KEY", "aws_secret_key")

	compose, err := docker.New().
		WithApiKey().
		WithRBAC().
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(gCaller, gCallerKey).
		WithUserApiKey(gTarget, gTargetKey).
		WithRbacRoots(adminUser).
		WithDbUsers().
		WithNamespaces().
		WithMCP().
		WithOffloadS3("offloading", "us-west-1").
		// backup-s3 + export share the MinIO sidecar; the "backups" bucket backs
		// both. REPLICA_MOVEMENT_ENABLED ensures the Replicate handler reaches the
		// auth check (it 501s before authz when movement is off), so the RBAC deny
		// in rbac_surfaces_test.go is observable.
		WithBackendS3("backups", "us-west-1").
		WithWeaviateEnv("EXPORT_ENABLED", "true").
		WithWeaviateEnv("EXPORT_DEFAULT_BUCKET", "backups").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
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
