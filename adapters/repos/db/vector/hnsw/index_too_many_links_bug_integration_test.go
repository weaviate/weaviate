//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2022 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

//go:build integrationTest && !race
// +build integrationTest,!race

package hnsw

import (
	"context"
	"math/rand"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The !race build tag makes sure that this test is EXCLUDED from running with
// the race detector on, but now we also need to make sure that it runs in the
// separate no-race test run. To INCLUDE it there we use the Test_NoRace_
// prefix.
// This test imports 10,000 objects concurrently which is extremely expensive
// with the race detector on.
// It prevents a regression on
// https://github.com/semi-technologies/weaviate/issues/1868
func Test_NoRace_ManySmallCommitlogs(t *testing.T) {
	n := 10000
	dim := 16
	m := 8

	rand.Seed(time.Now().UnixNano())
	rootPath := t.TempDir()

	logger, _ := test.NewNullLogger()
	original, err := NewCommitLogger(rootPath, "too_many_links_test", 1, logger,
		WithCommitlogThreshold(1e5), WithCommitlogThresholdForCombining(5e5))
	require.Nil(t, err)

	data := make([][]float32, n)
	for i := range data {
		data[i] = make([]float32, dim)
		for j := range data[i] {
			data[i][j] = rand.Float32()
		}

	}

	var index *hnsw

	t.Run("set up an index with the specified commit logger", func(t *testing.T) {
		idx, err := New(Config{
			MakeCommitLoggerThunk: func() (CommitLogger, error) {
				return original, nil
			},
			ID:               "too_many_links_test",
			RootPath:         rootPath,
			DistanceProvider: distancer.NewCosineDistanceProvider(),
			Logger:           logger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		}, UserConfig{
			MaxConnections:         m,
			EFConstruction:         128,
			CleanupIntervalSeconds: 0,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 2 * n,
		})
		require.Nil(t, err)
		idx.PostStartup()
		index = idx
	})

	t.Run("add data", func(t *testing.T) {
		type tuple struct {
			vec []float32
			id  uint64
		}

		jobs := make(chan tuple, n)

		wg := sync.WaitGroup{}
		worker := func(jobs chan tuple) {
			for job := range jobs {
				index.Add(job.id, job.vec)
			}

			wg.Done()
		}

		for i := 0; i < runtime.GOMAXPROCS(0); i++ {
			wg.Add(1)
			go worker(jobs)
		}

		for i, vec := range data {
			jobs <- tuple{id: uint64(i), vec: vec}
		}

		close(jobs)

		wg.Wait()
	})

	index.Flush()

	t.Run("verify there are no nodes with too many links - control", func(t *testing.T) {
		for i, node := range index.nodes {
			if node == nil {
				continue
			}

			for level, conns := range node.connections {
				m := index.maximumConnections
				if level == 0 {
					m = index.maximumConnectionsLayerZero
				}

				assert.LessOrEqualf(t, len(conns), m, "node %d at level %d with %d conns",
					i, level, len(conns))
			}
		}
	})

	t.Run("destroy the old index", func(t *testing.T) {
		// kill the commit loger and index
		require.Nil(t, original.Shutdown(context.Background()))
		index = nil
		original = nil
	})

	t.Run("create a new one from the disk files", func(t *testing.T) {
		idx, err := New(Config{
			MakeCommitLoggerThunk: MakeNoopCommitLogger, // no longer need a real one
			ID:                    "too_many_links_test",
			RootPath:              rootPath,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			Logger:                logger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		}, UserConfig{
			MaxConnections:         m,
			EFConstruction:         128,
			CleanupIntervalSeconds: 1,

			// The actual size does not matter for this test, but if it defaults to
			// zero it will constantly think it's full and needs to be deleted - even
			// after just being deleted, so make sure to use a positive number here.
			VectorCacheMaxObjects: 2 * n,
		})
		require.Nil(t, err)
		idx.PostStartup()
		index = idx
	})

	t.Run("verify there are no nodes with too many links - after restart", func(t *testing.T) {
		for i, node := range index.nodes {
			if node == nil {
				continue
			}

			for level, conns := range node.connections {
				m := index.maximumConnections
				if level == 0 {
					m = index.maximumConnectionsLayerZero
				}

				require.LessOrEqualf(t, len(conns), m, "node %d at level %d with %d conns",
					i, level, len(conns))
			}
		}
	})

	t.Run("destroy the index", func(t *testing.T) {
		require.Nil(t, index.Drop(context.Background()))
	})
}
