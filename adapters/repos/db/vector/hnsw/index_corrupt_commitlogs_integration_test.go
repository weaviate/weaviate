//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// +build integrationTest

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartupWithCorruptCondenseFiles(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	rootPath := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	os.MkdirAll(rootPath, 0o777)
	defer func() {
		err := os.RemoveAll(rootPath)
		fmt.Println(err)
	}()

	logger, _ := test.NewNullLogger()
	original, err := NewCommitLogger(rootPath, "corrupt_test", 0, logger)
	require.Nil(t, err)

	data := [][]float32{
		{0.1, 0.2},
		{0.12, 0.2},
		{0.13, 0.2},
		{0.14, 0.2},
		{0.15, 0.2},
		{0.16, 0.2},
		{0.16, 0.2},
		{0.17, 0.2},
	}

	var index *hnsw

	t.Run("set up an index with the specified commit logger", func(t *testing.T) {
		idx, err := New(Config{
			MakeCommitLoggerThunk: func() (CommitLogger, error) {
				return original, nil
			},
			TombstoneCleanupInterval: 0,
			ID:                       "corrupt_test",
			RootPath:                 rootPath,
			DistanceProvider:         distancer.NewCosineProvider(),
			EFConstruction:           100,
			Logger:                   logger,
			MaximumConnections:       100,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		})
		require.Nil(t, err)
		index = idx
	})

	t.Run("add data", func(t *testing.T) {
		for i, vec := range data {
			err := index.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	t.Run("create a corrupt commit log file without deleting the original",
		func(t *testing.T) {
			input, ok, err := getCurrentCommitLogFileName(commitLogDirectory(rootPath,
				"corrupt_test"))
			require.Nil(t, err)
			require.True(t, ok)

			f, err := os.Create(path.Join(commitLogDirectory(rootPath, "corrupt_test"),
				fmt.Sprintf("%s.condensed", input)))
			require.Nil(t, err)

			// write random non-sense to make sure the file is corrupt
			_, err = f.Write([]uint8{0xa8, 0x07, 0x34, 0x77, 0xf8, 0xff})
			require.Nil(t, err)
			f.Close()
		})

	t.Run("destroy the old index", func(t *testing.T) {
		// kill the index
		index = nil
		original = nil
	})

	t.Run("create a new one from the disk files", func(t *testing.T) {
		idx, err := New(Config{
			MakeCommitLoggerThunk:    MakeNoopCommitLogger, // no longer need a real one
			TombstoneCleanupInterval: 0,
			ID:                       "corrupt_test",
			RootPath:                 rootPath,
			DistanceProvider:         distancer.NewCosineProvider(),
			EFConstruction:           100,
			Logger:                   logger,
			MaximumConnections:       100,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		})
		require.Nil(t, err)
		index = idx
	})

	t.Run("verify querying works", func(t *testing.T) {
		res, err := index.SearchByVector([]float32{0.08, 0.08}, 100, nil)
		require.Nil(t, err)
		assert.Len(t, res, 8)
	})
}
