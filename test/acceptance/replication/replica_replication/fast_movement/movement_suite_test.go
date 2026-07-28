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

package replication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/weaviate/weaviate/test/docker"
)

// ReplicationMovementTestSuite shares a single 3-node cluster across the
// conflict (COPY/MOVE) and MOVE-source-deletion tests, mirroring the fast/
// package's ReplicationTestSuite so both packages pay cluster startup once.
type ReplicationMovementTestSuite struct {
	suite.Suite
	compose *docker.DockerCompose
	down    func()
}

func (suite *ReplicationMovementTestSuite) SetupSuite() {
	t := suite.T()
	t.Setenv("TEST_WEAVIATE_IMAGE", "weaviate/test-server")

	mainCtx := context.Background()
	ctx, cancel := context.WithTimeout(mainCtx, 10*time.Minute)

	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("REPLICATION_ENGINE_MAX_WORKERS", "100").
		WithWeaviateEnv("REPLICA_MOVEMENT_MINIMUM_ASYNC_WAIT", "5s").
		WithWeaviateEnv("REPLICA_MOVEMENT_ENABLED", "true").
		Start(ctx)
	require.Nil(t, err)
	if cancel != nil {
		cancel()
	}
	suite.compose = compose
	suite.down = func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()
		if err := compose.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate test containers: %s", err.Error())
		}
	}
}

func (suite *ReplicationMovementTestSuite) TearDownSuite() {
	if suite.down != nil {
		suite.down()
	}
}

func TestReplicationMovementTestSuite(t *testing.T) {
	suite.Run(t, new(ReplicationMovementTestSuite))
}
