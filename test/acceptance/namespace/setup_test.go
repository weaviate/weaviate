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
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

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

// Test personas. All API keys are statically declared at compose-up time.
// Per-test work is limited to creating a role with the desired permissions
// and assigning it to one of these users (reversed in t.Cleanup).
const (
	adminUser, adminKey               = "admin-user", "admin-key"
	manageUser, manageKey             = "manage-user", "manage-key"
	scopedManageUser, scopedManageKey = "scoped-manage-user", "scoped-manage-key"
	viewerUser, viewerKey             = "viewer-user", "viewer-key"
	noPermsUser, noPermsKey           = "no-perms-user", "no-perms-key"
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
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(manageUser, manageKey).
		WithUserApiKey(scopedManageUser, scopedManageKey).
		WithUserApiKey(viewerUser, viewerKey).
		WithUserApiKey(noPermsUser, noPermsKey).
		WithDbUsers().
		WithNamespaces().
		WithMCP().
		WithOffloadS3("offloading", "us-west-1").
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
