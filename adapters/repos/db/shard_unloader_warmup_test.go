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

package db

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/entities/schema"
)

func TestStartupWarmupAcrossIndexesStaysWithinTheLimitAndNeverMakesRoom(t *testing.T) {
	const indexes, limit = 6, 2
	u := newFakeShardUnloader()
	u.warmupLimit.Store(limit)
	repo, migrator, _, _ := newLazyLoadRepoWithConfig(t, singleShardState(), true, 0, withShardUnloader(u))
	t.Cleanup(func() { require.NoError(t, repo.Shutdown(context.Background())) })
	for n := range indexes {
		require.NoError(t, migrator.AddClass(testCtx(), newClassWithWarmProp(fmt.Sprintf("Warmup%d", n))))
	}
	for n := range indexes {
		index := repo.GetIndex(schema.ClassName(fmt.Sprintf("Warmup%d", n)))
		require.NotNil(t, index)
		require.Eventually(t, index.allShardsReady.Load, 20*time.Second, 10*time.Millisecond)
	}
	require.EqualValues(t, limit, u.registry.Loaded(), "warmup fills the limit and stops there")
	require.Zero(t, u.beforeLoads.Load(), "a warmup load never makes room")
}
