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

	"github.com/stretchr/testify/require"

	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

// TestInitTargetVector_ShutsDownIndexWhenQueueCreationFails pins that a vector
// index whose queue fails to be created is shut down rather than orphaned,
// leaking its cache goroutine. The cancelled-ctx case guards that the shutdown
// uses s.shutCtx and not the caller's (possibly cancelled) ctx.
func TestInitTargetVector_ShutsDownIndexWhenQueueCreationFails(t *testing.T) {
	tests := []struct {
		name      string
		callerCtx func() context.Context
	}{
		{
			name:      "valid caller ctx",
			callerCtx: context.Background,
		},
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
			shardLike, index := testShard(t, context.Background(), "VecQueueOrphan")
			s := shardLike.(*Shard)

			// async indexing makes NewVectorIndexQueue call q.Init(), which mkdirs
			// the queue dir
			index.AsyncIndexingEnabled = true

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
