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

package hnsw

import (
	"context"
	"fmt"
	"os"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestBackup_SwitchCommitLogs(t *testing.T) {
	ctx := context.Background()

	dirName := t.TempDir()
	indexID := "backup-switch-commitlogs-test"

	idx, err := New(Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New(), cyclemanager.NewCallbackGroupNoop())
		},
	}, enthnsw.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), nil)
	require.Nil(t, err)
	idx.PostStartup()

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	err = idx.SwitchCommitLogs(ctx)
	assert.Nil(t, err)

	err = idx.Shutdown(ctx)
	require.Nil(t, err)
}

func TestBackup_ListFiles(t *testing.T) {
	ctx := context.Background()

	dirName := t.TempDir()
	indexID := "backup-list-files-test"

	idx, err := New(Config{
		RootPath:         dirName,
		ID:               indexID,
		Logger:           logrus.New(),
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, logrus.New(), cyclemanager.NewCallbackGroupNoop())
		},
	}, enthnsw.NewDefaultUserConfig(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), cyclemanager.NewCallbackGroupNoop(), nil)
	require.Nil(t, err)
	idx.PostStartup()

	t.Run("assert expected index contents", func(t *testing.T) {
		files, err := idx.ListFiles(ctx, dirName)
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
