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

package hfresh

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/compressionhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func TestHFreshBackupListFiles(t *testing.T) {
	logger, _ := test.NewNullLogger()
	store := testinghelpers.NewDummyStore(t)
	cfg, ucfg := makeHFreshConfig(t)

	vectors_size := 2000
	queries_size := 100
	dimensions := 64
	k := 10

	before := time.Now()
	vectors, queries := testinghelpers.RandomVecsFixedSeed(vectors_size, queries_size, dimensions)
	var mu sync.Mutex
	truths := make([][]uint64, queries_size)
	compressionhelpers.Concurrently(logger, uint64(len(queries)), func(i uint64) {
		res, _ := testinghelpers.BruteForce(logger, vectors, queries[i], k, distanceWrapper(distancer.NewL2SquaredProvider()))
		mu.Lock()
		truths[i] = res
		mu.Unlock()
	})

	fmt.Printf("generating data took %s\n", time.Since(before))

	cfg.VectorForIDThunk = hnsw.NewVectorForIDThunk(cfg.TargetVector, func(ctx context.Context, indexID uint64, targetVector string) ([]float32, error) {
		return vectors[indexID], nil
	})
	index := makeHFreshWithConfig(t, store, cfg, ucfg)

	before = time.Now()
	var count atomic.Uint32
	compressionhelpers.Concurrently(logger, uint64(vectors_size), func(id uint64) {
		cur := count.Add(1)
		if cur%1000 == 0 {
			fmt.Printf("indexing vectors %d/%d\n", cur, vectors_size)
		}
		err := index.Add(t.Context(), id, vectors[id])
		require.NoError(t, err)
	})

	t.Run("test disk layout", func(t *testing.T) {
		dirs, err := os.ReadDir(cfg.RootPath)
		require.NoError(t, err)
		dirsFound := make(map[string]struct{})
		for _, dir := range dirs {
			dirsFound[dir.Name()] = struct{}{}
		}
		expectedDirs := []string{
			"analyze.queue.d",
			"centroids.hnsw.commitlog.d",
			"centroids.hnsw.snapshot.d",
			"merge.queue.d",
			"reassign.queue.d",
			"split.queue.d",
		}
		for _, expectedDir := range expectedDirs {
			if _, ok := dirsFound[expectedDir]; !ok {
				t.Fatalf("expected dir %s not found in %v", expectedDir, dirsFound)
			}
		}
	})

	fmt.Printf("indexing done, took: %s, waiting for background tasks...\n", time.Since(before))

	t.Run("test list files during execution", func(t *testing.T) {
		var err error
		for index.taskQueue.Size() > 0 {
			fmt.Println("background tasks: ", index.taskQueue.Size())

			err = index.stopTaskQueues()
			require.NoError(t, err)
			if index.taskQueue.Size() > 0 {
				hasAtLeastOneQueueFile := false
				files, err := index.ListFiles(t.Context(), cfg.RootPath)
				require.NoError(t, err)
				for _, file := range files {
					if strings.Contains(file, "queue.d") {
						fmt.Println("found queue file: ", file)
						hasAtLeastOneQueueFile = true
						break
					}
				}
				// queue files should be present, but the number is not deterministic
				require.True(t, hasAtLeastOneQueueFile)
			}
			index.resumeTaskQueues()
			time.Sleep(500 * time.Millisecond)
		}
		fmt.Println("all background tasks done, took: ", time.Since(before))
	})

	t.Run("test list files after backup preparation", func(t *testing.T) {
		err := index.PrepareForBackup(t.Context())
		require.NoError(t, err)
		files, err := index.ListFiles(t.Context(), cfg.RootPath)
		require.NoError(t, err)

		// at least one centroid commit log
		var hasCentroidCommitLog bool
		for _, file := range files {
			if strings.HasPrefix(file, "centroids.hnsw.commitlog.d") {
				hasCentroidCommitLog = true
				break
			}
		}
		require.True(t, hasCentroidCommitLog)
	})
}
