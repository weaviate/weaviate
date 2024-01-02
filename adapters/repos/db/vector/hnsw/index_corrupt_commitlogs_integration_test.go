//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	hnswent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestStartupWithCorruptCondenseFiles(t *testing.T) {
	rootPath := t.TempDir()

	logger, _ := test.NewNullLogger()
	original, err := NewCommitLogger(rootPath, "corrupt_test", logger,
		cyclemanager.NewCallbackGroupNoop())
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
			ID:               "corrupt_test",
			RootPath:         rootPath,
			DistanceProvider: distancer.NewCosineDistanceProvider(),
			Logger:           logger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		}, hnswent.UserConfig{
			MaxConnections:         100,
			EFConstruction:         100,
			CleanupIntervalSeconds: 0,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		index = idx
	})

	t.Run("add data", func(t *testing.T) {
		for i, vec := range data {
			err := index.Add(uint64(i), vec)
			require.Nil(t, err)
		}
	})

	index.Flush()

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
			MakeCommitLoggerThunk: MakeNoopCommitLogger, // no longer need a real one
			ID:                    "corrupt_test",
			RootPath:              rootPath,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			Logger:                logger,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return data[int(id)], nil
			},
		}, hnswent.UserConfig{
			MaxConnections:         100,
			EFConstruction:         100,
			CleanupIntervalSeconds: 0,
		}, cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(),
			cyclemanager.NewCallbackGroupNoop(), testinghelpers.NewDummyStore(t))
		require.Nil(t, err)
		index = idx
	})

	t.Run("verify querying works", func(t *testing.T) {
		res, _, err := index.SearchByVector([]float32{0.08, 0.08}, 100, nil)
		require.Nil(t, err)
		assert.Len(t, res, 8)
	})
}
