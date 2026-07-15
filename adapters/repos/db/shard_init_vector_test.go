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
// index whose queue fails to be created is shut down rather than orphaned.
// The index is created before the queue but not yet stored in the shard, so
// cleanupPartialInit cannot reach it; without an explicit shutdown its cache
// goroutine (and commitlog fd) leak on every lazy-load retry.
func TestInitTargetVector_ShutsDownIndexWhenQueueCreationFails(t *testing.T) {
	ctx := context.Background()
	shardLike, index := testShard(t, ctx, "VecQueueOrphan")
	s := shardLike.(*Shard)

	// async indexing makes NewVectorIndexQueue call q.Init(), which mkdirs the
	// queue dir — the only realistic failure point after the index is created.
	index.AsyncIndexingEnabled = true

	const target = "orphanTarget"
	queueDir := filepath.Join(s.path(), fmt.Sprintf("%s.queue.d", s.vectorIndexID(target)))
	require.NoError(t, os.WriteFile(queueDir, []byte("x"), 0o600))

	// the HNSW cache runs a watchForDeletion goroutine that only exits on the
	// index's Shutdown/Drop, so its disappearance proves the index was shut down.
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

	err := s.initTargetVector(ctx, target, enthnsw.NewDefaultUserConfig(), false)
	require.Error(t, err)
	require.ErrorContains(t, err, "cannot create index queue")

	s.vectorIndexMu.RLock()
	_, stored := s.vectorIndexes[target]
	s.vectorIndexMu.RUnlock()
	require.False(t, stored, "orphaned index must not be stored")

	require.Eventually(t, func() bool { return watchers() <= baseline }, 5*time.Second, 20*time.Millisecond,
		"orphaned HNSW index was not shut down (cache goroutine leaked)")
}
