package hnsw

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"math/rand"
	"os"
	"path"
	"regexp"
	"testing"
	"time"

	"github.com/semi-technologies/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSnapshot_PauseMaintenance(t *testing.T) {
	t.Run("assert that context timeout works for long maintenance cycle", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		idx, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "snapshot-pause-maintenance-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk:      testVectorForID,
		}, NewDefaultUserConfig())
		require.Nil(t, err)

		ctx, cancel := context.WithTimeout(ctx, time.Nanosecond)
		defer cancel()

		err = idx.PauseMaintenance(ctx)
		require.NotNil(t, err)
		assert.Equal(t, "long-running tombstone cleanup in progress: context deadline exceeded", err.Error())

		err = idx.Shutdown()
		require.Nil(t, err)
	})

	t.Run("assert tombstone maintenance is successfully paused", func(t *testing.T) {
		ctx := context.Background()

		dirName := makeTestDir(t)
		defer removeTestDir(t, dirName)

		idx, err := New(Config{
			RootPath:              "doesnt-matter-as-committlogger-is-mocked-out",
			ID:                    "snapshot-pause-maintenance-test",
			MakeCommitLoggerThunk: MakeNoopCommitLogger,
			DistanceProvider:      distancer.NewCosineDistanceProvider(),
			VectorForIDThunk:      testVectorForID,
		}, NewDefaultUserConfig())
		require.Nil(t, err)

		ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		err = idx.PauseMaintenance(ctx)
		assert.Nil(t, err)

		err = idx.Shutdown()
		require.Nil(t, err)
	})
}

func TestSnapshot_SwitchCommitlogs(t *testing.T) {
	ctx := context.Background()

	indexID := "snapshot-pause-maintenance-test"

	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	idx, err := New(Config{
		RootPath: dirName,
		ID:       "snapshot-pause-maintenance-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(path.Join(dirName, indexID), indexID, 500*time.Millisecond,
				logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, NewDefaultUserConfig())
	require.Nil(t, err)

	//ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	//defer cancel()

	ctx = context.Background()

	err = idx.SwitchCommitLogs(ctx)
	assert.Nil(t, err)

	//time.Sleep(5 * time.Second)

	files, err := idx.ListFiles(context.Background())
	require.Nil(t, err)
	require.Len(t, files, 2)
}

func TestSnapshot_ListFiles(t *testing.T) {
	ctx := context.Background()

	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	commitLoggerID := "snapshot-test"

	idx, err := New(Config{
		RootPath: dirName,
		ID:       "snapshot-pause-maintenance-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, commitLoggerID, 500*time.Millisecond,
				logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, NewDefaultUserConfig())
	require.Nil(t, err)

	t.Run("assert expected index contents", func(t *testing.T) {
		files, err := idx.ListFiles(ctx)
		assert.Nil(t, err)
		assert.Len(t, files, 1)

		parent, child := path.Split(idx.rootPath)
		pattern := fmt.Sprintf("%s\\/%s", path.Clean(parent), child)
		pattern = fmt.Sprintf("%s\\/%s\\.hnsw\\.commitlog\\.d\\/[0-9]{10}", pattern, commitLoggerID)
		matched, err := regexp.MatchString(pattern, files[0])

		assert.Nil(t, err)
		assert.True(t, matched, "regex does not match")
	})

	err = idx.Shutdown()
	require.Nil(t, err)
}

func TestSnapshot_ResumeMaintenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	commitLoggerID := "snapshot-test"

	dirName := makeTestDir(t)
	defer removeTestDir(t, dirName)

	idx, err := New(Config{
		RootPath: dirName,
		ID:       "snapshot-pause-maintenance-test",
		MakeCommitLoggerThunk: func() (CommitLogger, error) {
			return NewCommitLogger(dirName, commitLoggerID, 500*time.Millisecond,
				logrus.New())
		},
		DistanceProvider: distancer.NewCosineDistanceProvider(),
		VectorForIDThunk: testVectorForID,
	}, NewDefaultUserConfig())
	require.Nil(t, err)

	t.Run("insert vector into index", func(t *testing.T) {
		first := &vertex{level: 0, id: 0, connections: make(map[int][]uint64)}
		err := idx.insert(first, []float32{1, 2, 3})
		require.Nil(t, err)
	})

	t.Run("assert cleanup restarts after pausing", func(t *testing.T) {
		err = idx.PauseMaintenance(ctx)
		require.Nil(t, err)

		err = idx.ResumeMaintenance(ctx)
		assert.Nil(t, err)
	})

	err = idx.Shutdown()
	require.Nil(t, err)
}

func makeTestDir(t *testing.T) string {
	rand.Seed(time.Now().UnixNano())
	dirName := fmt.Sprintf("./testdata/%d", rand.Intn(10000000))
	if err := os.MkdirAll(dirName, 0o777); err != nil {
		t.Fatalf("failed to make test dir '%s': %s", dirName, err)
	}
	return dirName
}

func removeTestDir(t *testing.T, dirName string) {
	if err := os.RemoveAll(dirName); err != nil {
		t.Errorf("failed to remove test dir '%s': %s", dirName, err)
	}
}
