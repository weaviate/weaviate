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

//go:build integrationTest
// +build integrationTest

package hnsw

import (
	"context"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	enthnsw "github.com/weaviate/weaviate/entities/vectorindex/hnsw"
)

func TestBackup_Integration(t *testing.T) {
	ctx := context.Background()

	indexID := "backup-integration-test"

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

	t.Run("insert vector into index", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			inc := float32(i)
			err := idx.Add(uint64(i), []float32{inc, inc + 1, inc + 2})
			require.Nil(t, err)
		}
	})

	// let the index age for a second so that
	// the commitlogger filenames, which are
	// based on current timestamp, can differ
	time.Sleep(time.Second)

	t.Run("pause maintenance", func(t *testing.T) {
		err = idx.PauseMaintenance(ctx)
		require.Nil(t, err)
	})

	t.Run("switch commit logs", func(t *testing.T) {
		err = idx.SwitchCommitLogs(ctx)
		require.Nil(t, err)
	})

	t.Run("list files", func(t *testing.T) {
		files, err := idx.ListFiles(ctx)
		require.Nil(t, err)

		// by this point there should be two files in the commitlog directory.
		// one is the active log file, and the other is the previous active
		// log which was in use prior to `SwitchCommitLogs`. additionally,
		// maintenance has been paused, so we shouldn't see any .condensed
		// files either.
		//
		// because `ListFiles` is used within the context of backups,
		// it excludes any currently active log files, which are not part
		// of the backup. in this case, the only other file is the prev
		// commitlog, so we should only have 1 result here.
		assert.Len(t, files, 1)

		t.Run("verify commitlog dir contents", func(t *testing.T) {
			// checking to ensure that indeed there are only 2 files in the
			// commit log directory, and that one of them is the one result
			// from `ListFiles`, and that the other is not a .condensed file
			ls, err := os.ReadDir(path.Join(dirName, fmt.Sprintf("%s.hnsw.commitlog.d", indexID)))
			require.Nil(t, err)
			assert.Len(t, ls, 2)

			var prevLogFound bool
			for _, info := range ls {
				if path.Base(files[0]) == info.Name() {
					prevLogFound = true
				}

				assert.Empty(t, path.Ext(info.Name()))
			}
			assert.True(t, prevLogFound, "previous commitlog not found in commitlog root dir")
		})
	})

	t.Run("resume maintenance", func(t *testing.T) {
		err = idx.ResumeMaintenance(ctx)
		require.Nil(t, err)
	})

	err = idx.Shutdown(ctx)
	require.Nil(t, err)
}
