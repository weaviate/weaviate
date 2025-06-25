//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
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

func composeUp(t *testing.T, admins map[string]string, users map[string]string, viewers map[string]string) (*docker.DockerCompose, func()) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)

	builder := docker.New().WithWeaviateEnv("AUTOSCHEMA_ENABLED", "false").WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").WithWeaviateWithGRPC().WithRBAC().WithApiKey()
	adminUserNames := make([]string, 0, len(admins))
	viewerUserNames := make([]string, 0, len(viewers))
	for userName, key := range admins {
		builder = builder.WithUserApiKey(userName, key)
		adminUserNames = append(adminUserNames, userName)
	}
	for userName, key := range viewers {
		builder = builder.WithUserApiKey(userName, key)
		viewerUserNames = append(viewerUserNames, userName)
	}
	if len(admins) > 0 {
		builder = builder.WithRbacRoots(adminUserNames...)
	}
	if len(viewers) > 0 {
		builder = builder.WithRbacViewers(viewerUserNames...)
	}
	for userName, key := range users {
		builder = builder.WithUserApiKey(userName, key)
	}
	compose, err := builder.Start(ctx)
	require.Nil(t, err)

	helper.SetupClient(compose.GetWeaviate().URI())
	helper.SetupGRPCClient(t, compose.GetWeaviate().GrpcURI())

	return compose, func() {
		helper.ResetClient()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %v", err)
		}
		cancel()
	}
}
