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

	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

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
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	compose, err := docker.New().
		WithApiKey().
		WithUserApiKey(adminUser, adminKey).
		WithUserApiKey(manageUser, manageKey).
		WithUserApiKey(scopedManageUser, scopedManageKey).
		WithUserApiKey(viewerUser, viewerKey).
		WithUserApiKey(noPermsUser, noPermsKey).
		WithDbUsers().
		WithNamespaces().
		WithWeaviateWithGRPC().
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
