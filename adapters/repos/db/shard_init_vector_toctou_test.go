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
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	schemaConfig "github.com/weaviate/weaviate/entities/schema/config"
	"github.com/weaviate/weaviate/entities/storagestate"
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

// TestInitTargetVector_ShutsDownIndexWhenQueueCreationFails pins that a vector
// index whose queue fails to be created is shut down rather than orphaned,
// leaking its cache goroutine. The cancelled-ctx case guards that the shutdown
// uses s.shutCtx and not the caller's (possibly cancelled) ctx.
func TestInitTargetVector_ShutsDownIndexWhenQueueCreationFails(t *testing.T) {
	tests := []struct {
		name      string
		callerCtx func() context.Context
	}{
		{name: "valid caller ctx", callerCtx: context.Background},
		{
			name: "cancelled caller ctx",
			callerCtx: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// async indexing makes NewVectorIndexQueue call q.Init()
			shardLike, _ := testShard(t, context.Background(), "VecQueueOrphan",
				func(idx *Index) { idx.AsyncIndexingEnabled = true })
			s := underlyingShard(t, shardLike)

			const target = "orphanTarget"
			// a file where the queue dir must go makes q.Init() fail
			queueDir := filepath.Join(s.path(), fmt.Sprintf("%s.queue.d", s.vectorIndexID(target)))
			require.NoError(t, os.WriteFile(queueDir, []byte("x"), 0o600))

			// the HNSW cache goroutine only exits on Shutdown/Drop
			watchers := func() int {
				buf := make([]byte, 1<<20)
				for {
					n := runtime.Stack(buf, true)
					if n < len(buf) {
						return strings.Count(string(buf[:n]), "watchForDeletion")
					}
					buf = make([]byte, 2*len(buf))
				}
			}
			baseline := watchers()

			err := s.initTargetVector(tt.callerCtx(), target, enthnsw.NewDefaultUserConfig(), false)
			require.Error(t, err)
			require.ErrorContains(t, err, "cannot create index queue")

			s.vectorIndexMu.RLock()
			_, stored := s.vectorIndexes[target]
			s.vectorIndexMu.RUnlock()
			require.False(t, stored, "orphaned index must not be stored")

			require.Eventually(t, func() bool { return watchers() <= baseline }, 5*time.Second, 20*time.Millisecond,
				"orphaned HNSW index was not shut down (cache goroutine leaked)")
		})
	}
}

// TestUpdateVectorIndexConfigs_FailedInitRestoresStatus pins that a failed
// vector-index creation does not leave the shard read-only: the goroutine that
// restores StatusReady must run even when initTargetVector returns an error.
func TestUpdateVectorIndexConfigs_FailedInitRestoresStatus(t *testing.T) {
	// async indexing makes NewVectorIndexQueue call q.Init()
	shardLike, _ := testShard(t, context.Background(), "VecQueueReadonly",
		func(idx *Index) { idx.AsyncIndexingEnabled = true })
	s := underlyingShard(t, shardLike)

	const target = "newTarget"
	// a file where the queue dir must go makes q.Init() fail
	queueDir := filepath.Join(s.path(), fmt.Sprintf("%s.queue.d", s.vectorIndexID(target)))
	require.NoError(t, os.WriteFile(queueDir, []byte("x"), 0o600))

	err := s.UpdateVectorIndexConfigs(context.Background(),
		map[string]schemaConfig.VectorIndexConfig{target: enthnsw.NewDefaultUserConfig()})
	require.Error(t, err)

	require.Eventually(t, func() bool { return s.GetStatus() == storagestate.StatusReady }, 5*time.Second, 20*time.Millisecond,
		"shard left read-only after a failed vector index creation")
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
