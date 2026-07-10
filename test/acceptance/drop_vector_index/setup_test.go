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

package drop_vector_index

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/test/docker"
	"github.com/weaviate/weaviate/test/helper"
)

// The suite runs identically against a single node and a 3-node cluster; on
// the cluster, finalize additionally proves cross-node unit completion and
// RAFT propagation of the schema removal.
//
// PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS=1 makes memtables become
// segments promptly, so drops exercise real segment rewrites.

func TestDropVectorIndex_SingleNode(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviate().
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	runSuite(t, compose)
}

func TestDropVectorIndex_Cluster(t *testing.T) {
	ctx := context.Background()
	compose, err := docker.New().
		WithWeaviateCluster(3).
		WithWeaviateEnv("ENABLE_EXPERIMENTAL_ALTER_SCHEMA_DROP_VECTOR_INDEX_ENDPOINT", "true").
		WithWeaviateEnv("PERSISTENCE_MEMTABLES_FLUSH_DIRTY_AFTER_SECONDS", "1").
		Start(ctx)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, compose.Terminate(ctx))
	}()

	runSuite(t, compose)
}

func runSuite(t *testing.T, compose *docker.DockerCompose) {
	helper.SetupClient(compose.GetWeaviate().URI())
	defer helper.ResetClient()

	t.Run("lifecycle", testLifecycle())
	t.Run("write matrix", testWriteMatrix())
	t.Run("multi tenant", testMultiTenant())
}
