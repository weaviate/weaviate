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

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_ManySmallCommitlogs(t *testing.T) {
	n := 10000
	dim := 16
	m := 8

	rand.Seed(time.Now().UnixNano())
	rootPath := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(rootPath, 0o777)
	defer func() {
		err := os.RemoveAll(rootPath)
		fmt.Println(err)
	}()

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
			DistanceProvider: distancer.NewCosineProvider(),
			Logger:           logger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		}, UserConfig{
			MaxConnections:         m,
			EFConstruction:         128,
			CleanupIntervalSeconds: 0,
		})
		require.Nil(t, err)
		index = idx
	})

	t.Run("add data", func(t *testing.T) {
		type tuple struct {
			vec []float32
			id  uint64
		}

		jobs := make(chan tuple)

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
		// kill the index
		index = nil
		original = nil
	})

	t.Run("create a new one from the disk files", func(t *testing.T) {
		idx, err := New(Config{
			MakeCommitLoggerThunk: MakeNoopCommitLogger, // no longer need a real one
			ID:                    "too_many_links_test",
			RootPath:              rootPath,
			DistanceProvider:      distancer.NewCosineProvider(),
			Logger:                logger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		}, UserConfig{
			MaxConnections:         m,
			EFConstruction:         128,
			CleanupIntervalSeconds: 1,
		})
		require.Nil(t, err)
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
}
