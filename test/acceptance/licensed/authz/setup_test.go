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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// composeUp starts a namespace-enabled RBAC node with DB users, the admins as
// RBAC roots and the users as plain API-key users.
func composeUp(t *testing.T, admins, users map[string]string) func() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	builder := docker.New().WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").WithWeaviate().
		WithRBAC().WithApiKey().WithDbUsers().WithNamespaces()
	adminUserNames := make([]string, 0, len(admins))
	for userName, key := range admins {
		builder = builder.WithUserApiKey(userName, key)
		adminUserNames = append(adminUserNames, userName)
	}
	builder = builder.WithRbacRoots(adminUserNames...)
	for userName, key := range users {
		builder = builder.WithUserApiKey(userName, key)
	}
	compose, err := builder.Start(ctx)
	require.NoError(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	return func() {
		helper.ResetClient()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
		cancel()
	}
}
