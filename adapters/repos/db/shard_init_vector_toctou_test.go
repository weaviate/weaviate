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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// Re-initialising an existing target vector must not replace and orphan its
// queue. This is the outcome of two concurrent UpdateVectorIndexConfigs calls
// that both observe the target absent before either creates it; reproduced here
// sequentially.
func TestInitTargetVector_Idempotent_DoesNotOrphanQueue(t *testing.T) {
	ctx := context.Background()
	shardLike, _ := testShard(t, ctx, "ToctouDoubleCreate")
	shard := underlyingShard(t, shardLike)

	cfg := enthnsw.UserConfig{Skip: true}

	require.NoError(t, shard.initTargetVector(ctx, "v1", cfg, false))
	shard.vectorIndexMu.RLock()
	q1 := shard.queues["v1"]
	shard.vectorIndexMu.RUnlock()
	require.NotNil(t, q1)

	require.NoError(t, shard.initTargetVector(ctx, "v1", cfg, false))

	shard.vectorIndexMu.RLock()
	q2 := shard.queues["v1"]
	shard.vectorIndexMu.RUnlock()

	assert.Same(t, q1, q2,
		"re-initialising an existing target vector must not silently replace and "+
			"orphan its queue; the first queue is leaked (never Dropped)")
}

// underlyingShard returns the concrete *Shard behind a ShardLike, loading it if
// the test helper handed back a lazy-load wrapper (the default differs across
// release branches, so handle both).
func underlyingShard(t *testing.T, sl ShardLike) *Shard {
	t.Helper()
	switch s := sl.(type) {
	case *Shard:
		return s
	case *LazyLoadShard:
		require.NoError(t, s.Load(context.Background()))
		return s.shard
	default:
		t.Fatalf("unexpected ShardLike type %T", sl)
		return nil
	}
}
