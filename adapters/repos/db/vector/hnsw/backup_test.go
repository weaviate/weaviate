//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestBackup_PauseMaintenance(t *testing.T) {
	t.Run("assert that context timeout works for long maintenance cycle", func(t *testing.T) {
		indexID := "backup-pause-maintenance-test"

		dirName := makeTestDir(t)

		userConfig := enthnsw.NewDefaultUserConfig()
		userConfig.CleanupIntervalSeconds = 1

		idx, err := New(Config{
			RootPath: "doesnt-matter-as-committlogger-is-mocked-out",
			ID:       indexID,
			MakeCommitLoggerThunk: func() (CommitLogger, error) {
				return NewCommitLogger(dirName, indexID, logrus.New())
			},
			DistanceProvider: distancer.NewCosineDistanceProvider(),
			VectorForIDThunk: testVectorForID,
		}, userConfig)
		require.Nil(t, err)

		ctx, cancel := context.WithDeadline(context.Background(), time.Now())
		defer cancel()

		err = idx.PauseMaintenance(ctx)
		require.NotNil(t, err)
		assert.Equal(t,
			"long-running commitlog shutdown in progress: context deadline exceeded",
			err.Error())

		err = idx.Shutdown(context.Background())
		require.Nil(t, err)
	})

	t.Run("assert tombstone maintenance is successfully paused", func(t *testing.T) {
		ctx := context.Background()

		idx, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "backup-pause-maintenance-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk:      testVectorForID,
		}, enthnsw.NewDefaultUserConfig())
		require.Nil(t, err)

		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		err = idx.PauseMaintenance(ctx)
		assert.Nil(t, err)

		err = idx.Shutdown(ctx)
		require.Nil(t, err)
	})
}

func TestBackup_SwitchCommitLogs(t *testing.T) {
	ctx := context.Background()

	indexID := "backup-switch-commitlogs-test"

	dirName := makeTestDir(t)

	idx, err := New(Config{
		RootPath: dirName,
		ID:       indexID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, enthnsw.NewDefaultUserConfig())
	require.Nil(t, err)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	err = idx.SwitchCommitLogs(ctx)
	assert.Nil(t, err)

	err = idx.Shutdown(ctx)
	require.Nil(t, err)
}

func TestBackup_ListFiles(t *testing.T) {
	ctx := context.Background()

	dirName := makeTestDir(t)

	indexID := "backup-list-files-test"

	idx, err := New(Config{
		RootPath: dirName,
		ID:       indexID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, enthnsw.NewDefaultUserConfig())
	require.Nil(t, err)

	t.Run("assert expected index contents", func(t *testing.T) {
		files, err := idx.ListFiles(ctx)
		assert.Nil(t, err)

		// should return empty, because the only file which
		// exists in the commitlog root is the current active
		// log file.
		assert.Len(t, files, 0)

		// checking to ensure that the commitlog root does
		// contain a file. this is the one that was ignored
		// in the check above.
		ls, err := os.ReadDir(path.Join(dirName, fmt.Sprintf("%s.hnsw.commitlog.d", indexID)))
		require.Nil(t, err)
		require.Len(t, ls, 1)
		// filename should just be a 10 digit int
		matched, err := regexp.MatchString("[0-9]{10}", ls[0].Name())
		assert.Nil(t, err)
		assert.True(t, matched, "regex does not match")
	})

	err = idx.Shutdown(ctx)
	require.Nil(t, err)
}

func TestBackup_ResumeMaintenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	indexID := "backup-resume-maintenance-test"

	dirName := makeTestDir(t)

	idx, err := New(Config{
		RootPath: dirName,
		ID:       "backup-pause-maintenance-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, enthnsw.NewDefaultUserConfig())
	require.Nil(t, err)

	t.Run("insert vector into index", func(t *testing.T) {
		first := &vertex{level: 0, id: 0, connections: make([][]uint64, 1)}
		err := idx.insert(first, []float32{1, 2, 3})
		require.Nil(t, err)
	})

	t.Run("assert cleanup restarts after pausing", func(t *testing.T) {
		err = idx.PauseMaintenance(ctx)
		require.Nil(t, err)

		err = idx.ResumeMaintenance(ctx)
		assert.Nil(t, err)
		assert.True(t, idx.tombstoneCleanupCycle.Running())
		assert.True(t, idx.commitLog.MaintenanceInProgress())
	})

	err = idx.Shutdown(ctx)
	require.Nil(t, err)
}

func makeTestDir(t *testing.T) string {
	rand.Seed(time.Now().UnixNano())
	return t.TempDir()
}
