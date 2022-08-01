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

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_Integration(t *testing.T) {
	ctx := context.Background()

	indexID := "snapshot-integration-test"

	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	idx, err := New(Config{
		RootPath: dirName,
		ID:       indexID,
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, indexID, 500*time.Millisecond,
				logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, NewDefaultUserConfig())
	require.Nil(t, err)

	t.Run("pause maintenance", func(t *testing.T) {
		err = idx.PauseMaintenance(ctx)
		require.Nil(t, err)
	})

	// give the index a sec to pause maintenance cycle
	time.Sleep(time.Second)

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
		// because `ListFiles` is used within the context of snapshotting,
		// it excludes any currently active log files, which are not part
		// of the snapshot. in this case, the only other file is the prev
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
