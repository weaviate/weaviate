package hnsw

import (
	"context"
	"fmt"
	"math/rand"
	"os"
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

func TestSnapshot_SwitchCommitlogs(t *testing.T) {}

func TestSnapshot_ListFiles(t *testing.T) {}

func TestSnapshot_ResumeMaintenance(t *testing.T) {}

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
